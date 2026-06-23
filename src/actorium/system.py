from asyncio import CancelledError, Future, get_running_loop
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import partial
from typing import Any, AsyncGenerator, Protocol, assert_never
from uuid import UUID, uuid4

from anyio import (
    Event,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    sleep,
)
from anyio.abc import TaskGroup
from anyio.from_thread import start_blocking_portal
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from msgspec import Struct
from typing_extensions import TypeForm

from actorium.actor import ActorFactory, BaseActor, RawMailbox
from actorium.logger import logger
from actorium.runtime_generic import substitute_types
from actorium.serialization import SerializedData, deserialize, serialize
from actorium.types import ActorAddress, ActorId, SystemId
from actorium.utils import TtlMap

__all__ = [
    "ActorSystem",
    "lookup",
    "spawn",
    "GatewayMessage",
]

type GatewayMessage = MessageForActor | PublishRoute | Register | Unregister


class _GatewayMessage(Struct, tag=str.lower):
    pass


class MessageForActor(_GatewayMessage):
    actor_address: ActorAddress
    message: SerializedData


class PublishRoute(_GatewayMessage):
    """Tells the other side that we handle this system_id."""

    system_id: SystemId
    lease_time_seconds: float


class Register(_GatewayMessage):
    name: str
    ref: SerializedData  # (serialized) ActorAddress
    lease_time_seconds: float
    unregister_key: UUID


class Unregister(_GatewayMessage):
    name: str

    # The `Unregister` is only process if the key corresponds to what was
    # provided as part of `Register`.
    unregister_key: UUID


_ACTOR_SYSTEM: ContextVar[ActorSystem | None] = ContextVar("_ACTOR_SYSTEM")

_CURRENT_ACTOR: ContextVar[BaseActor | None] = ContextVar("_CURRENT_ACTOR")


@dataclass
class _NameRegistration:
    ref: SerializedData  # (serialized) object
    unregister_key: UUID


@dataclass
class _SystemIdRegistration:
    # unregister_key: UUID
    pass


@dataclass(frozen=True)
class _Gateway:
    to_gateway_writer: MemoryObjectSendStream[GatewayMessage]
    system_ids: TtlMap[SystemId, _SystemIdRegistration] = field(default_factory=TtlMap)
    registered_names: TtlMap[str, _NameRegistration] = field(default_factory=TtlMap)


class ActorSystem:
    """
    Special actor that manages all actors within an actor system.
    """

    def __init__(self, task_group: TaskGroup) -> None:
        self._task_group = task_group
        self._actor_mailboxes: dict[ActorId, RawMailbox] = {}
        self._system_id = uuid4()

        # Running actors.
        self._terminate_event = Event()

        # Name registration.
        self._name_to_actor_address: dict[str, _NameRegistration] = {}
        self._name_to_actor_address_waiters: dict[str, set[Future[SerializedData]]] = (
            defaultdict(set)
        )

        self._actor_to_task_group: dict[BaseActor, TaskGroup] = {}

        self._gateways: list[_Gateway] = []

        self._loop = get_running_loop()

    @classmethod
    def current(cls) -> ActorSystem | None:
        return _ACTOR_SYSTEM.get()

    @classmethod
    async def create_and_run[A: BaseActor, R, *P](
        cls, factory: ActorFactory[A, R, *P], /, *args: *P
    ) -> None:
        """
        Create an actor system where all actors are scheduled within the
        current asyncio thread.
        """
        async with create_task_group() as tg:
            instance = cls(tg)

            with _ACTOR_SYSTEM.set(instance):
                with _CURRENT_ACTOR.set(None):
                    # Spawn main actor.
                    options = SpawnOptions()
                    instance.spawn_with_options(options, factory, *args)

                    # Wait until we don't have a reference to any actor anymore and
                    # until the main actor terminated.
                    await instance._terminate_event.wait()

    @classmethod
    def create_and_run_in_thread[A: BaseActor, R, *P](
        cls, factory: ActorFactory[A, R, *P], /, *args: *P
    ) -> None:
        """
        Create a background thread (for the duration of this context manager).
        All actors scheduled in this actor system will be scheduled in this
        event loop.
        """
        with start_blocking_portal() as portal:
            portal.call(lambda: cls.create_and_run(factory, *args))

    def spawn[A: BaseActor, R, *P](
        self, factory: ActorFactory[A, R, *P], /, *args: *P, name: str | None = None
    ) -> R:
        return self.spawn_with_options(SpawnOptions(), factory, *args, name=name)

    def spawn_with_options[A: BaseActor, R, *P](
        self,
        options: SpawnOptions,
        factory: ActorFactory[A, R, *P],
        /,
        *args: *P,
        name: str | None = None,
    ) -> R:
        # Initialize actor class.
        try:
            actor = factory(*args)
        except BaseException as e:
            breakpoint()

        # Run `actor_post_init` -> here the actor can create its mailboxes.
        def create_mailbox() -> RawMailbox:
            mailbox_id = uuid4()
            address = ActorAddress(system_id=self._system_id, actor_id=mailbox_id)
            mailbox = RawMailbox(address=address)
            self._actor_mailboxes[mailbox_id] = mailbox
            return mailbox

        actor.actor_post_init(create_mailbox)

        # Get actor reference.
        ref = factory.actor_ref(actor)

        # Start async `actor_run`, where the mailboxes will be consumed.
        parent_actor = _CURRENT_ACTOR.get(None)
        if parent_actor is None:
            tg = self._task_group
        else:
            tg = self._actor_to_task_group[parent_actor]

        tg.start_soon(
            partial(
                self._run_wrapper,
                factory,
                name,
                actor,
                ref,
                is_main=parent_actor is None,
            )
        )

        return ref

    async def _run_wrapper[A: BaseActor, R, *P](
        self,
        factory: ActorFactory[A, R, *P],
        name: str | None,
        actor: A,
        ref: object,
        is_main: bool,
    ) -> None:
        assert isinstance(factory, type)

        async with AsyncExitStack() as stack:
            if name is not None:
                return_annotation = factory.actor_ref.__annotations__["return"]
                await stack.enter_async_context(
                    self._register(
                        ref, substitute_types(return_annotation, factory), name
                    )
                )

            with _CURRENT_ACTOR.set(actor):
                async with create_task_group() as tg:
                    self._actor_to_task_group[actor] = tg

                    try:
                        await factory.actor_run(actor)
                        tg.cancel_scope.cancel()
                    except CancelledError:
                        raise
                    # except BaseException as e:
                    #     print(f"Unhandled exception in actor! {e}")
                    #     traceback.print_exc()

                    finally:
                        del self._actor_to_task_group[actor]

                        if is_main:
                            self._terminate_event.set()

    @asynccontextmanager
    async def _register(
        self, ref: object, typeform: TypeForm[Any], name: str
    ) -> AsyncGenerator[None]:
        unregister_key = uuid4()

        # Serialize
        ref = serialize(ref)

        self._name_to_actor_address[name] = _NameRegistration(
            ref=ref, unregister_key=unregister_key
        )

        # Unblock `lookup()` calls waiting for a registration.
        for fut in self._name_to_actor_address_waiters.get(name, set()):
            if not fut.done():
                fut.set_result(ref)

        # Broadcast to all subscriptions.
        register_msg = Register(
            ref=ref,
            name=name,
            lease_time_seconds=10,
            unregister_key=unregister_key,
        )

        for gateway in self._gateways:
            await gateway.to_gateway_writer.send(register_msg)

        try:
            yield
        finally:
            self._unregister(Unregister(name=name, unregister_key=unregister_key))

    def _unregister(self, msg: Unregister) -> None:
        registration = self._name_to_actor_address.get(msg.name)
        if (
            registration is not None
            and registration.unregister_key == msg.unregister_key
        ):
            self._name_to_actor_address.pop(msg.name)
        # TODO broadcast `Unregister`!

    async def lookup(self, name: str, timeout: float | None = None) -> SerializedData:
        # Look for local registration.
        registration = self._name_to_actor_address.get(name)
        if registration is not None:
            return registration.ref

        # Look for registration through any gateway.
        for gateway in self._gateways:
            registration = gateway.registered_names.get(name)
            if registration is not None:
                return registration.ref

        f = Future[SerializedData]()
        self._name_to_actor_address_waiters[name].add(f)
        try:
            with fail_after(timeout):
                return await f
        finally:
            self._name_to_actor_address_waiters[name].discard(f)

    @asynccontextmanager
    async def connect_gateway(
        self,
        messages_from_gateway: MemoryObjectReceiveStream[GatewayMessage],
    ) -> AsyncGenerator[MemoryObjectReceiveStream[GatewayMessage]]:
        """
        Gateways should subscribe to the system state, and forward all
        `GatewayMessage` to the other system.
        """
        # To gateway
        to_gateway_writer, to_gateway_reader = create_memory_object_stream[
            GatewayMessage
        ]()

        this_gateway = _Gateway(to_gateway_writer=to_gateway_writer)

        async def send_state_once() -> None:
            await to_gateway_writer.send(
                PublishRoute(system_id=self._system_id, lease_time_seconds=10)
            )

            for name, registration in self._name_to_actor_address.items():
                await to_gateway_writer.send(
                    Register(
                        name=name,
                        ref=registration.ref,
                        lease_time_seconds=10,  # TODO: proper lease time.
                        unregister_key=registration.unregister_key,
                    )
                )

            for gateway in self._gateways:
                if gateway != this_gateway:
                    for system_id in gateway.system_ids.keys():
                        await to_gateway_writer.send(
                            PublishRoute(
                                system_id=system_id,
                                lease_time_seconds=10,
                            )
                        )

                    for name, registration in gateway.registered_names.items():
                        await to_gateway_writer.send(
                            Register(
                                name=name,
                                ref=registration.ref,
                                lease_time_seconds=10,  # TODO: proper lease time.
                                unregister_key=registration.unregister_key,
                            )
                        )

        async def send_state_loop() -> None:
            while True:
                await send_state_once()
                await sleep(5)

        async def consume_from_gateway() -> None:
            with messages_from_gateway:
                async for msg in messages_from_gateway:
                    match msg:
                        case MessageForActor(
                            actor_address=actor_address, message=message
                        ):
                            await self._call_actor(actor_address, message)
                        case PublishRoute(
                            system_id=system_id,
                            lease_time_seconds=lease_time_seconds,
                        ):
                            this_gateway.system_ids.set(
                                system_id,
                                _SystemIdRegistration(),
                                ttl_seconds=lease_time_seconds,
                            )

                            # Forward route to other gateways.
                            for g in self._gateways:
                                if g != this_gateway:
                                    await g.to_gateway_writer.send(msg)

                        case Register(
                            name=name,
                            ref=ref,
                            lease_time_seconds=lease_time_seconds,
                            unregister_key=unregister_key,
                        ):
                            this_gateway.registered_names.set(
                                name,
                                _NameRegistration(
                                    ref=ref, unregister_key=unregister_key
                                ),
                                ttl_seconds=lease_time_seconds,
                            )

                            # Unblock waiters.
                            for fut in self._name_to_actor_address_waiters.get(
                                name, set()
                            ):
                                if not fut.done():
                                    fut.set_result(ref)

                        case Unregister(name=name, unregister_key=unregister_key):
                            registration = this_gateway.registered_names.get(name)
                            if (
                                registration is not None
                                and registration.unregister_key == unregister_key
                            ):
                                this_gateway.registered_names.pop(name)
                        case _:
                            assert_never(msg)

        with to_gateway_writer:
            self._gateways.append(this_gateway)
            try:
                async with create_task_group() as tg:
                    tg.start_soon(send_state_loop)
                    tg.start_soon(consume_from_gateway)
                    yield to_gateway_reader
                    tg.cancel_scope.cancel()
            finally:
                self._gateways.remove(this_gateway)

    async def _call_actor[T](
        self,
        actor_address: ActorAddress,
        serialized_message: SerializedData,
    ) -> None:
        system_id = actor_address.system_id
        actor_id = actor_address.actor_id

        # If this message is for *this* actor system, then directly route into
        # the right actor.
        if system_id == self._system_id:
            # Route into the right callback.
            try:
                mailbox = self._actor_mailboxes[actor_id]
            except KeyError:
                logger.warning("Actor not found: actor_id=%s", actor_id)
            else:
                mailbox.feed(serialized_message)
            return

        for gateway in self._gateways:
            if system_id in gateway.system_ids.keys():
                await gateway.to_gateway_writer.send(
                    MessageForActor(
                        actor_address=actor_address, message=serialized_message
                    )
                )
                return

        logger.warning(
            "No route to actor actor_id=%s, system_id=%s, current_system_id=%s",
            actor_id,
            system_id,
            self._system_id,
        )

    def call_actor_soon(
        self, actor_address: ActorAddress, serialized_message: SerializedData
    ) -> None:
        """
        Threadsafe call to send a message to any actor started by this actor
        system.
        """
        self._task_group.start_soon(self._call_actor, actor_address, serialized_message)


@dataclass
class SpawnOptions:
    respawn_on_failure: bool = False


class _ActorRefType[T](Protocol):
    def __call__(self, *, actor_address: ActorAddress) -> T: ...


def _get_system() -> ActorSystem:
    system = _ACTOR_SYSTEM.get()
    if system is None:
        raise RuntimeError("Actor system not running.")
    return system


def spawn[A: BaseActor, R, *P](
    factory: ActorFactory[A, R, *P], /, *args: *P, name: str | None = None
) -> R:
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

    return system.spawn(factory, *args, name=name)


async def lookup[T](name: str, type_: type[T], timeout: float | None = None) -> T:
    system = _get_system()

    value = await system.lookup(name, timeout=timeout)

    return deserialize(value, type=type_)


@asynccontextmanager
async def connect_gateway(
    messages_from_gateway: MemoryObjectReceiveStream[GatewayMessage],
) -> AsyncGenerator[MemoryObjectReceiveStream[GatewayMessage]]:
    system = _get_system()

    async with system.connect_gateway(messages_from_gateway) as read_stream:
        yield read_stream
