from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncGenerator, Callable, Self, Sequence

from anyio import create_task_group
from anyio.abc import TaskGroup

from ..addresses import ActorId, Address
from .base import Listener, Outbox, SendResult
from .in_memory import InMemoryListener, InMemoryOutbox
from .redis import RedisOutbox
from .tcp import TcpOutbox
from .unix_socket import UnixSocketOutbox

__all__ = [
    "MultiProtocolOutbox",
    "MultiProtocolListener",
]


class MultiProtocolOutbox(Outbox):
    def __init__(self, outboxes: list[Outbox], task_group: TaskGroup) -> None:
        self._outboxes = outboxes
        self._task_group = task_group

    @classmethod
    @asynccontextmanager
    async def create(cls) -> AsyncGenerator[Self]:
        async with (
            InMemoryOutbox.create() as in_memory_outbox,
            UnixSocketOutbox.create() as unix_socket_outbox,
            TcpOutbox.create() as tcp_outbox,
            RedisOutbox.create() as redis_outbox,
            create_task_group() as tg,
        ):
            yield cls(
                outboxes=[
                    in_memory_outbox,
                    unix_socket_outbox,
                    tcp_outbox,
                    redis_outbox,
                ],
                task_group=tg,
            )

    async def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> SendResult:
        for outbox in self._outboxes:
            result = await outbox.send_to_actor(addresses, actor_id, serialized_message)
            if result in (SendResult.MESSAGE_SENT, SendResult.ACTOR_NOT_FOUND):
                return result
            assert result == SendResult.NO_ADDRESS_HANDLED_HERE

        return SendResult.NO_ADDRESS_HANDLED_HERE


class MultiProtocolListener(Listener):
    def __init__(self, listeners: list[Listener]) -> None:
        self._listeners = listeners

    @classmethod
    @asynccontextmanager
    async def create(cls, listeners: Sequence[Listener]) -> AsyncGenerator[Self]:
        async with InMemoryListener.create() as in_memory_listener:
            yield cls(listeners=[in_memory_listener, *listeners])

    @asynccontextmanager
    async def listen(
        self, callback: Callable[[ActorId, str], None]
    ) -> AsyncGenerator[None]:
        async with AsyncExitStack() as stack:
            for listener in self._listeners:
                await stack.enter_async_context(listener.listen(callback))

            yield

    def addresses(self) -> list[Address]:
        result = []
        for listener in self._listeners:
            result.extend(listener.addresses())
        return result
