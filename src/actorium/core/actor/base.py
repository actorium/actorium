from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Protocol, Self

from anyio import create_memory_object_stream

from ..types import ActorAddress

__all__ = [
    "AnyRef",
    "RawMailbox",
    "BaseActor",
    "ActorFactory",
]


class AnyRef(Protocol):
    """
    We will have multiple `ref` types.

    Some will have helper methods attached, some will use Pydantic
    serialization, while other use Pickle. The only thing they have in common
    should be the following two field.
    """

    @property
    def actor_address(self) -> ActorAddress: ...


class RawMailbox:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(self) -> None:
        self._sender, self._receiver = create_memory_object_stream[str](math.inf)

    def feed(self, serialized_msg: str) -> None:
        self._sender.send_nowait(serialized_msg)

    async def next(self) -> str:
        return await self._receiver.receive()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> str:
        return await self.next()


class BaseActor(ABC):
    @abstractmethod
    def actor_ref(self, actor_address: ActorAddress) -> AnyRef:
        """
        Factory for producing the `ref` proxy that gets produced in the
        `spawn` context manager.
        """

    @abstractmethod
    async def actor_run(self, mailbox: RawMailbox, actor_address: ActorAddress) -> None:
        """
        Main `run` function of the actor.
        """


class ActorFactory[A, R: AnyRef, **P](Protocol):
    """
    Actor protocol: a class *definition*, not instance, which:

    - can be initialized through the given paramspec (specified by `__call__`
      here).
    - has a `receive` method which accepts actor messages. Note that on the
      class definition, `receive` is unbound, so it takes the actor `A` as
      first argument.
    """

    def __call__(self, *args: P.args, **kwars: P.kwargs) -> A:
        "Actor `__init__` signature."

    def actor_ref(self, state: A, actor_address: ActorAddress) -> R: ...

    async def actor_run(
        self, state: A, mailbox: RawMailbox, actor_address: ActorAddress
    ) -> None: ...
