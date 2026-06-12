import math
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Protocol, Self

from anyio import create_memory_object_stream

from .serialization import SerializedData
from .types import ActorAddress

__all__ = [
    "RawMailbox",
    "BaseActor",
    "ActorFactory",
]


class RawMailbox:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(self, address: ActorAddress) -> None:
        self.address = address
        self._sender, self._receiver = create_memory_object_stream[SerializedData](
            math.inf
        )

    def feed(self, serialized_message: SerializedData) -> None:
        self._sender.send_nowait(serialized_message)

    async def next(self) -> SerializedData:
        return await self._receiver.receive()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> SerializedData:
        return await self.next()


class BaseActor(ABC):
    @abstractmethod
    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        pass

    @abstractmethod
    def actor_ref(self) -> object:
        """
        Factory for producing the `ref` proxy that gets produced in the
        `spawn` context manager.
        """

    @abstractmethod
    async def actor_run(self) -> None:
        """
        Main `run` function of the actor.
        """


class ActorFactory[A: BaseActor, R, *P](Protocol):
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

    def actor_post_init(
        self, __state: A, create_mailbox: Callable[[], RawMailbox]
    ) -> None:
        """
        Initialization for specific actor type. This is the place where
        mailboxes are created.
        """

    def actor_ref(self, __state: A) -> R:
        """
        Actor reference that gets returned by the `spawn()` function after
        starting the actor.

        The actor reference is a stateless proxy towards the actor, it is
        serializable and only holds the address.

        :param state: This is the `self` from the actor instance. (This
            protocol represents the class, not the instance.)
        """

    async def actor_run(self, __state: A, /) -> None:
        """
        Entry point for the actor. This is where the actor can consume its
        mailbox.

        :param state: This is the `self` from the actor instance. (This
            protocol represents the class, not the instance.)
        """
