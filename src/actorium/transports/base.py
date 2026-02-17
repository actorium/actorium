from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from enum import Enum
from typing import Callable, Sequence

from ..addresses import ActorId, Address

__all__ = [
    "Listener",
    "Outbox",
    "SendResult",
]


class Listener(ABC):
    @abstractmethod
    def listen(
        self, callback: Callable[[ActorId, str], None]
    ) -> AbstractAsyncContextManager[None]:
        """
        Listen for incoming messages for the given actor through this listener.
        """

    @abstractmethod
    def addresses(self) -> list[Address]:
        """
        List of addresses through which this listener implementation is
        (potentially) reachable.
        """


class SendResult(Enum):
    MESSAGE_SENT = "MESSAGE_SENT"
    ACTOR_NOT_FOUND = "ACTOR_NOT_FOUND"
    NO_ADDRESS_HANDLED_HERE = "NO_ADDRESS_HANDLED_HERE"


class Outbox(ABC):
    @abstractmethod
    async def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> SendResult: ...
