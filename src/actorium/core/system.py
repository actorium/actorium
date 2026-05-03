from __future__ import annotations

import contextvars
import traceback
from asyncio import CancelledError, get_running_loop
from collections import defaultdict
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncGenerator, Literal, Protocol
from uuid import UUID, uuid4

import anyio
from anyio import create_task_group, move_on_after, sleep
from anyio.abc import TaskGroup
from anyio.from_thread import start_blocking_portal
from pydantic import BaseModel

from .actor import Actor, ActorFactory, AnyRef, Mailbox, RawMailbox, Ref
from .ttl_map import TtlMap
from .types import ActorAddress, ActorId, SystemId, Timeout


class MessageForActor(BaseModel):
    type_: Literal["message-for-actor"] = "message-for-actor"
    actor_address: ActorAddress
    message: str  # json-serialized


type GatewayMessage = MessageForActor | PublishRoute | Register | Unregister


class RegisterRoute(BaseModel):
    type_: Literal["announce-route"] = "announce-route"
    system_id: SystemId
    gateway: Ref[GatewayMessage]
    # lease_time_seconds: float


class PublishRoute(BaseModel):
    "Tell the other side that we handle this system_id."

    type_: Literal["publish-route"] = "publish-route"
    system_id: SystemId


type PublishMessage = PublishRoute | Register | Unregister


class SubscribeState(BaseModel):
    type_: Literal["subscribe-state"] = "subscribe-state"
    reply_to: Ref[PublishMessage]
    lease_time_seconds: float
    unsubscribe_key: UUID


class UnsubscribeState(BaseModel):
    type_: Literal["unsubscribe-state"] = "unsubscribe-state"
    unsubscribe_key: UUID


class Register(BaseModel):
    type: Literal["register"] = "register"
    name: str
    address: ActorAddress
    lease_time_seconds: float
    unregister_key: UUID


class Unregister(BaseModel):
    type_: Literal["unregister"] = "unregister"
    name: str

    # The `Unregister` is only process if the key corresponds to what was
    # provided as part of `Register`.
    unregister_key: UUID


type LookupResultMessage = ActorAddress | None


class Lookup(BaseModel):
    type_: Literal["lookup"] = "lookup"
    name: str
    reply_to: Ref[LookupResultMessage]


type ActorSystemMessage = (
    MessageForActor
    | RegisterRoute
    | Register
    | Unregister
    | Lookup
    | SubscribeState
    | UnsubscribeState
)

_CURRENT_SYSTEM: ActorSystem | None = None  # TODO: make contextvar.

_ALL_ACTOR_SYSTEMS: dict[SystemId, ActorSystem] = {}

_ACTOR_SYSTEM: ContextVar[ActorSystem | None] = ContextVar("_ACTOR_SYSTEM")


@dataclass
class _NameRegistration:
    address: ActorAddress
    unregister_key: UUID


class ActorSystem(Actor[ActorSystemMessage]):
    """
    Special actor that manages all actors within an actor system.
    """

    def __init__(self, task_group: TaskGroup) -> None:
        self._task_group = task_group
        self._actor_mailboxes: dict[ActorId, RawMailbox] = {}
        self._system_id = uuid4()

        self._routes: dict[SystemId, list[Ref[GatewayMessage]]] = defaultdict(list)
        # Add self-route.
        self._routes[self._system_id].append(
            Ref[GatewayMessage](
                actor_address=ActorAddress(
                    system_id=self._system_id,
                    actor_id="SYSTEM",
                )
            )
        )

        # Self mailbox.
        self._mailbox = RawMailbox()
        self._actor_mailboxes["SYSTEM"] = self._mailbox

        # Name registration.
        self._name_to_actor_address: TtlMap[str, _NameRegistration] = TtlMap()

        # unsubscribe_key to subscription registration
        self._subscriptions: TtlMap[UUID, Ref[PublishMessage]] = TtlMap()

        self._loop = get_running_loop()

    @classmethod
    def current(cls) -> ActorSystem | None:
        return _ACTOR_SYSTEM.get()

    @classmethod
    async def create_and_run[A, R: AnyRef, **P](
        cls, factory: ActorFactory[A, R, P], /, *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """
        Create an actor system where all actors are scheduled within the
        current asyncio thread.
        """
        global _CURRENT_SYSTEM

        async with create_task_group() as tg:
            instance = cls(tg)

            # Register as a global.
            _CURRENT_SYSTEM = instance
            _ACTOR_SYSTEM.set(instance)
            _ALL_ACTOR_SYSTEMS[instance._system_id] = instance
            try:
                options = SpawnOptions(terminate_system_on_complete=True)

                async with instance.spawn_with_options(
                    options, factory, *args, **kwargs
                ):
                    mailbox = Mailbox[ActorSystemMessage](
                        message_type=ActorSystemMessage,
                        raw_mailbox=instance._mailbox,
                        ref=ActorSystemRef(
                            actor_address=ActorAddress(
                                system_id=instance._system_id,
                                actor_id="SYSTEM",
                            ),
                        ),
                    )

                    await instance.run(mailbox)
            finally:
                _CURRENT_SYSTEM = None
                _ACTOR_SYSTEM.set(None)
                del _ALL_ACTOR_SYSTEMS[instance._system_id]

    @classmethod
    def create_and_run_in_thread[A, R: AnyRef, **P](
        cls, factory: ActorFactory[A, R, P], /, *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """
        Create a background thread (for the duration of this context manager).
        All actors scheduled in this actor system will be scheduled in this
        event loop.
        """
        with start_blocking_portal() as portal:
            portal.call(lambda: cls.create_and_run(factory, *args, **kwargs))

    async def run(self, mailbox: Mailbox[ActorSystemMessage], /) -> None:
        async for msg in mailbox:
            match msg:
                case MessageForActor(actor_address=actor_address, message=message):
                    await self._call_actor(actor_address, message)
                case RegisterRoute(system_id=system_id, gateway=gateway):
                    if gateway.actor_address.system_id != self._system_id:
                        print(
                            "Actor gateway should be an actor from the current "
                            "actor system."
                        )
                    else:
                        self._routes[system_id].append(gateway)

                        # Broadcast to all subscriptions.
                        for _, subscription in self._subscriptions.items():
                            if subscription != gateway:
                                subscription.tell(PublishRoute(system_id=system_id))

                case Register(
                    name=name,
                    address=address,
                    lease_time_seconds=lease_time_seconds,
                    unregister_key=unregister_key,
                ):
                    self._name_to_actor_address.set(
                        name,
                        _NameRegistration(
                            address=address, unregister_key=unregister_key
                        ),
                        ttl_seconds=lease_time_seconds,
                    )

                    # Broadcast to all subscriptions.
                    # Except towards the route where this name originates from.
                    for _, subscription in self._subscriptions.items():
                        if subscription not in self._routes[address.system_id]:
                            subscription.tell(msg)

                case Unregister(name=name, unregister_key=unregister_key):
                    registration = self._name_to_actor_address.get(name)
                    if (
                        registration is not None
                        and registration.unregister_key == unregister_key
                    ):
                        self._name_to_actor_address.pop(name)
                    # TODO broadcast `Unregister`!
                case Lookup(name=name, reply_to=reply_to):
                    registration = self._name_to_actor_address.get(name)
                    reply_to.tell(
                        None if registration is None else registration.address
                    )
                case SubscribeState(
                    reply_to=reply_to,
                    lease_time_seconds=lease_time_seconds,
                    unsubscribe_key=unsubscribe_key,
                ):
                    self._subscriptions.set(
                        unsubscribe_key,
                        reply_to,
                        ttl_seconds=lease_time_seconds,
                    )

                    # Broadcast initial state.
                    for system_id in self._routes:
                        reply_to.tell(PublishRoute(system_id=system_id))

                    for (
                        name,
                        registration,
                        ttl_seconds,
                    ) in self._name_to_actor_address.items_with_remaining_ttl():
                        reply_to.tell(
                            Register(
                                name=name,
                                address=registration.address,
                                lease_time_seconds=ttl_seconds,
                                unregister_key=registration.unregister_key,
                            )
                        )
                case UnsubscribeState(unsubscribe_key=unsubscribe_key):
                    self._subscriptions.pop(unsubscribe_key)

    @asynccontextmanager
    async def spawn[A, R: AnyRef, **P](
        self, factory: ActorFactory[A, R, P], /, *args: P.args, **kwargs: P.kwargs
    ) -> AsyncGenerator[R]:
        async with self.spawn_with_options(
            SpawnOptions(), factory, *args, **kwargs
        ) as ref:
            yield ref

    @asynccontextmanager
    async def spawn_with_options[A, R: AnyRef, **P](
        self,
        options: SpawnOptions,
        factory: ActorFactory[A, R, P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> AsyncGenerator[R]:
        actor = factory(*args, **kwargs)
        actor_id = uuid4()

        mailbox = RawMailbox()
        actor_address = ActorAddress(system_id=self._system_id, actor_id=actor_id)

        self._actor_mailboxes[actor_id] = mailbox

        async def run_wrapper(mailbox: RawMailbox) -> None:
            try:
                await factory.actor_run(actor, mailbox, actor_address)
            except CancelledError:
                raise
            except BaseException as e:
                print(f"Unhandled exception in actor! {e}")
                traceback.print_exc()

            if options.terminate_system_on_complete:
                self._task_group.cancel_scope.cancel()

        self._task_group.start_soon(run_wrapper, mailbox)

        ref = factory.actor_ref(actor, actor_address=actor_address)
        try:
            yield ref
        finally:
            del self._actor_mailboxes[actor_id]

    async def _call_actor(self, actor_address: ActorAddress, message: str) -> None:
        system_id = actor_address.system_id
        actor_id = actor_address.actor_id

        # If this message if for *this* actor system, then directly route into
        # the right actor.
        if system_id == self._system_id:
            # Route into the right callback.
            try:
                mailbox = self._actor_mailboxes[actor_id]
            except KeyError:
                # print("Actor not found.")
                pass
            else:
                mailbox.feed(message)
            return

        # Check other actor systems in this process.
        try:
            system = _ALL_ACTOR_SYSTEMS[system_id]
        except KeyError:
            pass
        else:
            # (Likely other thread: call thread safe.)
            system.call_actor_threadsafe(actor_address, message)
            return

        # otherwise, find the right gateway.
        try:
            gateway = self._routes[system_id]
            if len(gateway) == 0:
                raise KeyError  # Same as no route.
        except KeyError:
            print(f"No route to actor system_id={system_id}, self={self._system_id}")
        else:
            gateway[0].tell(
                MessageForActor(actor_address=actor_address, message=message)
            )

    def call_actor_threadsafe(self, actor_address: ActorAddress, message: str) -> None:
        """
        Threadsafe call to send a message to any actor started by this actor
        system.
        """
        self._loop.call_soon_threadsafe(
            self._task_group.start_soon, self._call_actor, actor_address, message
        )

    def ref(self) -> ActorSystemRef:
        """
        Register this system as a global and produce a ref. For bootstrapping
        the actor system.
        """
        return ActorSystemRef(
            actor_address=ActorAddress(system_id=self._system_id, actor_id="SYSTEM")
        )


@dataclass
class SpawnOptions:
    terminate_system_on_complete: bool = False
    respawn_on_failure: bool = False


def run[A, R: AnyRef, **P](
    factory: ActorFactory[A, R, P], /, *args: P.args, **kwargs: P.kwargs
) -> None:
    async def main() -> None:
        await ActorSystem.create_and_run(factory, *args, **kwargs)

    ctx = contextvars.copy_context()
    ctx.run(anyio.run, main)


class ActorSystemRef(Ref[ActorSystemMessage]):
    async def lookup[T: AnyRef](
        self, name: str, type_: _ActorRefType[T], timeout: float | None = None
    ) -> T | Timeout:
        from ..actors.future import future

        with move_on_after(timeout):
            while True:
                async with future[LookupResultMessage]() as (f, reply_to):
                    self.tell(Lookup(name=name, reply_to=reply_to))

                    with move_on_after(1.0):
                        actor_address = await f
                        if actor_address is not None:
                            return type_(actor_address=actor_address)
                    await sleep(0.1)

        return Timeout()

    @asynccontextmanager
    async def register(self, actor_ref: AnyRef, name: str) -> AsyncGenerator[None]:
        publish_interval = 5
        lease_time = 10
        unregister_key = uuid4()

        async def register_loop() -> None:
            # NOTE: while delivery to the local system might be reliable, this
            #       `Register` message gets broadcasted to all connected
            #       systems, even over unreliable transports. So, it makes
            #       sense to publish it in an interval from here so that
            #       everyone gets updated at the same time.
            while True:
                self.tell(
                    Register(
                        address=actor_ref.actor_address,
                        name=name,
                        lease_time_seconds=lease_time,
                        unregister_key=unregister_key,
                    )
                )
                await sleep(publish_interval)

        async with create_task_group() as tg:
            tg.start_soon(register_loop)

            try:
                yield
                tg.cancel_scope.cancel()
            finally:
                self.tell(Unregister(name=name, unregister_key=unregister_key))

    @asynccontextmanager
    async def subscribe_state(
        self, reply_to: Ref[PublishMessage]
    ) -> AsyncGenerator[None]:
        subscribe_interval = 5
        lease_time = 10
        unsubscribe_key = uuid4()

        async def subscribe_loop() -> None:
            while True:
                self.tell(
                    SubscribeState(
                        reply_to=reply_to,
                        unsubscribe_key=unsubscribe_key,
                        lease_time_seconds=lease_time,
                    )
                )
                await sleep(subscribe_interval)

        try:
            async with create_task_group() as tg:
                tg.start_soon(subscribe_loop)
                yield
                tg.cancel_scope.cancel()
        finally:
            self.tell(UnsubscribeState(unsubscribe_key=unsubscribe_key))


class _ActorRefType[T: AnyRef](Protocol):
    def __call__(self, *, actor_address: ActorAddress) -> T: ...


def _get_system() -> ActorSystem:
    system = _ACTOR_SYSTEM.get()
    if system is None:
        raise RuntimeError("Actor system not running.")
    return system


def get_system() -> ActorSystemRef:
    return _get_system().ref()


@asynccontextmanager
async def spawn[A, R: AnyRef, **P](
    factory: ActorFactory[A, R, P], /, *args: P.args, **kwargs: P.kwargs
) -> AsyncGenerator[R]:
    """
    Context manager for spawning a new actor.

    The first argument `factory` is the actor class to be instantiated, the
    optional arguments and keyword arguments that follow are passed to the
    factory to instantiate the actor.

    Example usage::

        class Collector(Actor[int]):
            " Actor class. "
            def __init__(self, param: str)-> None:
                ...

            async def run(self, mailbox: Mailbox[int]) -> None:
                async for msg in mailbox:
                    ...

        async with spawn(Collector, param="some-param") as ref: ...
    """
    # If we are within an actor in this actor system,
    system = _get_system()

    async with system.spawn(factory, *args, **kwargs) as ref:
        yield ref
