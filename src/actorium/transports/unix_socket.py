from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Callable, Self, Sequence

from anyio import (
    Lock,
    connect_unix,
    create_task_group,
    create_unix_listener,
    move_on_after,
)
from anyio.abc import SocketStream, TaskGroup

from ..addresses import ActorId, Address, UnixSocketAddress
from ._line_protocol import LineReceiver, MessageForActor
from .base import Listener, Outbox, SendResult

__all__ = [
    "UnixSocketOutbox",
    "UnixSocketListener",
]


class UnixSocketOutbox(Outbox):
    def __init__(self, tg: TaskGroup) -> None:
        self.tg = tg
        self._address_to_outbox: dict[
            tuple[Address, ActorId], UnixSocketActorOutbox
        ] = {}

    @classmethod
    @asynccontextmanager
    async def create(cls) -> AsyncGenerator[Self]:
        async with create_task_group() as tg:
            instance = cls(tg=tg)
            try:
                yield instance
            finally:
                for actor_outbox in instance._address_to_outbox.values():
                    await actor_outbox.aclose()

    async def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> SendResult:
        for address in addresses:
            if not isinstance(address, UnixSocketAddress):
                continue

            try:
                actor_outbox = self._address_to_outbox[address, actor_id]
            except KeyError:
                socketstream = await connect_unix(address.path)
                actor_outbox = UnixSocketActorOutbox(socketstream, actor_id)
                self._address_to_outbox[address, actor_id] = actor_outbox
                await actor_outbox.send(serialized_message)
                return SendResult.MESSAGE_SENT

            # TODO: figure out if the actor with the given ID does exist on
            #       this node and return `ActorNotFound` if so.

        return SendResult.NO_ADDRESS_HANDLED_HERE


class UnixSocketActorOutbox:
    def __init__(self, socketstream: SocketStream, actor_id: ActorId) -> None:
        self._socketstream = socketstream
        self._actor_id = actor_id
        self._lock = Lock()

    async def send(self, message: str) -> None:
        message_for_actor = MessageForActor(actor_id=self._actor_id, message=message)

        async with self._lock:
            await self._socketstream.send(
                message_for_actor.model_dump_json().encode() + b"\n"
            )

    async def aclose(self) -> None:
        async with self._lock:
            await self._socketstream.aclose()


class UnixSocketListener(Listener):
    def __init__(self, path: Path) -> None:
        self.path = path

        self._callback: Callable[[ActorId, str], None] | None = None

    def addresses(self) -> list[Address]:
        return [UnixSocketAddress(path=self.path)]

    @classmethod
    @asynccontextmanager
    async def create(cls, path: Path) -> AsyncGenerator[Self]:
        unix_listener = await create_unix_listener(path)

        try:
            async with create_task_group() as tg:
                instance = cls(path=path)
                tg.start_soon(unix_listener.serve, instance._handle_unix_connection)

                yield instance

                tg.cancel_scope.cancel()
        finally:
            with move_on_after(1.0, shield=True):
                await unix_listener.aclose()

    async def _handle_unix_connection(self, client: SocketStream) -> None:
        line_receiver = LineReceiver(client)
        async for line in line_receiver:
            message = MessageForActor.model_validate_json(line)
            callback = self._callback
            if callback is None:
                print("No callback set in unix socket listener.")
                continue

            callback(message.actor_id, message.message)

    @asynccontextmanager
    async def listen(
        self, callback: Callable[[ActorId, str], None]
    ) -> AsyncGenerator[None]:
        self._callback = callback
        try:
            yield
        finally:
            self._callback = None
