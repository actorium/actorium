from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Self, Sequence
from uuid import UUID, uuid4

from anyio.from_thread import BlockingPortal

from ..addresses import ActorId, Address, InMemoryAddress
from ..process_hash import interpreter_hash, process_hash
from .base import Listener, Outbox, SendResult

__all__ = [
    "InMemoryOutbox",
    "InMemoryListener",
]

_IN_MEMORY_LISTENERS: dict[UUID, "InMemoryListener"] = {}


def _send_to_listener(
    address: InMemoryAddress, actor_id: ActorId, serialized_message: str
) -> SendResult:
    if address.interpreter_hash != interpreter_hash():
        # Not handled in this interpreter!
        return SendResult.NO_ADDRESS_HANDLED_HERE

    try:
        listener = _IN_MEMORY_LISTENERS[address.listener_id]
    except KeyError:
        return SendResult.NO_ADDRESS_HANDLED_HERE
    else:
        callback = listener._callback
        if callback is None:
            # Listener is here, so the address *is* handled here, but the actor
            # does not exist.
            return SendResult.ACTOR_NOT_FOUND

        listener._loop.call_soon_threadsafe(
            lambda: callback(actor_id, serialized_message)
        )
        return SendResult.MESSAGE_SENT

    # Correct interpreter, but listener does not exist.
    return SendResult.ACTOR_NOT_FOUND


class InMemoryOutbox(Outbox):
    @classmethod
    @asynccontextmanager
    async def create(cls) -> AsyncGenerator[Self]:
        yield cls()

    async def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> SendResult:
        for address in addresses:
            if not isinstance(address, InMemoryAddress):
                continue

            if address.process_hash != process_hash():
                # Other process: in-memory delivery not possible. Try
                # TCP/Redis/...
                continue

            if address.interpreter_hash == interpreter_hash():
                # Look for a listener in this interpreter.
                return _send_to_listener(address, actor_id, serialized_message)

            # Look for listener in other interpreter
            self._send_to_other_interpreter(address, actor_id, serialized_message)

        return SendResult.NO_ADDRESS_HANDLED_HERE

    def _send_to_other_interpreter(
        self, address: InMemoryAddress, actor_id: ActorId, serialized_message: str
    ) -> SendResult:
        import concurrent.interpreters

        for interpreter in concurrent.interpreters.list_all():
            send_result = interpreter.call(
                _send_to_listener, address, actor_id, serialized_message
            )
            if send_result in (SendResult.MESSAGE_SENT, SendResult.ACTOR_NOT_FOUND):
                return send_result

        # We are in the right process, but the interpreter was not found.
        return SendResult.ACTOR_NOT_FOUND


class InMemoryListener(Listener):
    def __init__(self, listener_id: UUID, portal: BlockingPortal) -> None:
        self.listener_id = listener_id
        self.portal = portal

        self._callback: Callable[[ActorId, str], None] | None = None

        self._loop = asyncio.get_running_loop()

    def addresses(self) -> list[Address]:
        return [
            InMemoryAddress(
                process_hash=process_hash(),
                interpreter_hash=interpreter_hash(),
                listener_id=self.listener_id,
            )
        ]

    @classmethod
    @asynccontextmanager
    async def create(cls) -> AsyncGenerator[Self]:
        listener_id = uuid4()

        async with BlockingPortal() as portal:
            instance = cls(listener_id=listener_id, portal=portal)

            _IN_MEMORY_LISTENERS[listener_id] = instance
            try:
                yield instance
            finally:
                del _IN_MEMORY_LISTENERS[listener_id]

    @asynccontextmanager
    async def listen(
        self, callback: Callable[[ActorId, str], None]
    ) -> AsyncGenerator[None]:
        self._callback = callback
        try:
            yield
        finally:
            self._callback = None
