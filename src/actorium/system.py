"""
Implementation of cross-process reactivity.
"""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, AsyncGenerator, Callable, Self, Sequence

from anyio import BrokenResourceError, create_task_group
from anyio.abc import TaskGroup
from pydantic import TypeAdapter

from .addresses import ActorId, Address
from .transports import (
    Listener,
    MultiProtocolListener,
    MultiProtocolOutbox,
    Outbox,
    SendResult,
)

if TYPE_CHECKING:
    from .actors import ActorRef
    from .reactivity.registry import NameResolver, RegistryMessage

__all__ = [
    "ActorSystem",
    "actor_system",
    "current_actor_system",
    "ActorSystemNotInitializedError",
    "register",
    "name_resolver",
]


class ActorSystemNotInitializedError(Exception):
    pass


class _CurrentActorSystem(threading.local):
    def __init__(self) -> None:
        super().__init__()
        self.actor_system: ActorSystem | None = None
        self.outbox: Outbox | None = None


_CURRENT_ACTOR_SYSTEM = _CurrentActorSystem()


def current_actor_system() -> ActorSystem:
    actor_system = _CURRENT_ACTOR_SYSTEM.actor_system
    if actor_system is None:
        raise ActorSystemNotInitializedError
    return actor_system


class ActorSystem:
    """
    Actor system: keep track of currently running actors.
    """

    def __init__(
        self,
        outbox: MultiProtocolOutbox,
        listener: MultiProtocolListener,
        task_group: TaskGroup,
    ) -> None:
        from .reactivity.registry import Registry, RegistryMessage

        self.outbox = outbox
        self.listener = listener
        self._task_group = task_group
        self._actor_callbacks: dict[ActorId, Callable[[str], None]] = {}
        self._registry = Registry()

        # Route messages for `REGISTRY` straight to the Registry actor.
        registry_type_adapter: TypeAdapter[RegistryMessage] = TypeAdapter(
            RegistryMessage
        )
        self._actor_callbacks["REGISTRY"] = lambda msg: task_group.start_soon(
            self._registry.receive, registry_type_adapter.validate_json(msg)
        )

    @classmethod
    @asynccontextmanager
    async def create(cls, listeners: Sequence[Listener] = ()) -> AsyncGenerator[Self]:
        assert _CURRENT_ACTOR_SYSTEM.actor_system is None

        def listen_callback(actor_id: ActorId, serialized_message: str) -> None:
            try:
                callback = instance._actor_callbacks[actor_id]
            except KeyError:
                print(f"Actor not found: {actor_id}")
            else:
                callback(serialized_message)

        async with (
            create_task_group() as tg,
            MultiProtocolOutbox.create() as outbox,
            MultiProtocolListener.create(listeners) as listener,
            listener.listen(listen_callback),
        ):
            instance = cls(outbox=outbox, listener=listener, task_group=tg)
            _CURRENT_ACTOR_SYSTEM.actor_system = instance
            try:
                yield instance
                tg.cancel_scope.cancel()
            finally:
                _CURRENT_ACTOR_SYSTEM.actor_system = None

    @asynccontextmanager
    async def register[T](
        self, actor_ref: ActorRef[T], name: str
    ) -> AsyncGenerator[None]:
        """
        Register the given actor under a discoverable name.
        """
        with self._registry.register(actor_ref, name):
            yield

    def name_resolver(self, peer_addresses: Sequence[Address] = ()) -> NameResolver:
        from .reactivity.registry import NameResolver

        return NameResolver([*self.addresses(), *peer_addresses])

    @asynccontextmanager
    async def listen(
        self, actor_id: ActorId, callback: Callable[[str], None]
    ) -> AsyncGenerator[None]:
        """
        Register the given actor under a discoverable name.
        """
        self._actor_callbacks[actor_id] = callback
        try:
            yield
        finally:
            del self._actor_callbacks[actor_id]

    def addresses(self) -> list[Address]:
        return self.listener.addresses()

    def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> None:
        """
        Schedule sending of message to actor.
        """

        async def send() -> None:
            with suppress(BrokenResourceError):
                send_result = await self.outbox.send_to_actor(
                    addresses, actor_id, serialized_message
                )
                if send_result == SendResult.ACTOR_NOT_FOUND:
                    print(f"actor not found: {actor_id}", repr(serialized_message))
                    return

        self._task_group.start_soon(send)


actor_system = ActorSystem.create


@asynccontextmanager
async def register[T](actor_ref: ActorRef[T], name: str) -> AsyncGenerator[None]:
    """
    Register the given actor under a discoverable name.
    """
    async with current_actor_system().register(actor_ref, name):
        yield


def name_resolver(peer_addresses: Sequence[Address] = ()) -> NameResolver:
    return current_actor_system().name_resolver(peer_addresses=peer_addresses)
