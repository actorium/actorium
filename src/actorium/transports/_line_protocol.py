from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from anyio import EndOfStream
from anyio.abc import SocketStream
from pydantic import BaseModel

from ..addresses import ActorId


@dataclass
class LineReceiver:
    client: SocketStream

    def __post_init__(self) -> None:
        self._buffer = bytearray()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> str:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration

    async def receive(self) -> str:
        while True:
            pos = self._buffer.find(b"\n")
            if pos != -1:
                break

            chunk = await self.client.receive()

            self._buffer.extend(chunk)

        line = self._buffer[:pos]
        del self._buffer[: pos + 1]

        return line.decode("utf-8")


class MessageForActor(BaseModel):
    actor_id: ActorId
    message: str  # json-serialized
