import math
from abc import ABC, abstractmethod
from typing import Protocol, Self

from anyio import create_memory_object_stream
from pydantic import BaseModel

from actorium.types import ActorAddress

__all__ = [
    "AnyRef",
    "RawMailbox",
    "BaseActor",
    "ActorFactory",
    "SerializedMessage",
]


class AnyRef(Protocol):
    """
    Actor reference (proxy) type.

    Some implementations will add helper methods for interaction.
    """

    @property
    def actor_address(self) -> ActorAddress: ...


class SerializedMessage(BaseModel):
    data: str


class RawMailbox:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(self) -> None:
        self._sender, self._receiver = create_memory_object_stream[
            object | SerializedMessage
        ](math.inf)

    def feed(self, message: object | SerializedMessage) -> None:
        self._sender.send_nowait(message)

    async def next(self) -> object | SerializedMessage:
        return await self._receiver.receive()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> object | SerializedMessage:
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


class ActorFactory[A: BaseActor, R: AnyRef, *P](Protocol):
    """
    Representation of the actor class *definition*, not the instance. This is
    what is passed as a first argument to the `spawn()` function:

    - can be initialized through the given paramspec (specified by `__call__`
      here).
    - has a `receive` method which accepts actor messages. Note that on the
      class definition, `receive` is unbound, so it takes the actor `A` as
      first argument.
    """

    def __call__(self, *args: *P) -> A:
        """
        Actor `__init__` signature.

        Note that we only have positional arguments. This is because `spawn()`
        only forwards positional arguments to the actor instance and handles
        the keyword arguments itself.
        """

    def actor_ref(self, state: A, actor_address: ActorAddress) -> R:
        """
        Actor reference that gets returned by the `spawn()` function after
        starting the actor.

        The actor reference is a stateless proxy towards the actor, it is
        serializable and only holds the address.

        :param state: This is the `self` from the actor instance. (This
            protocol represents the class, not the instance.)
        """

    async def actor_run(
        self, state: A, mailbox: RawMailbox, actor_address: ActorAddress
    ) -> None:
        """
        Entry point for the actor. This is where the actor can consume its
        mailbox.

        :param state: This is the `self` from the actor instance. (This
            protocol represents the class, not the instance.)
        """
