from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Self

import msgspec

from actorium.actor import BaseActor, RawMailbox
from actorium.logger import logger
from actorium.runtime_generic import runtime_generic
from actorium.serialization import deserialize, serialize
from actorium.types import ActorAddress

__all__ = [
    "SimpleActor",
    "Mailbox",
    "SimpleRef",
]


@runtime_generic
class SimpleActor[*T](BaseActor):
    """
    Simple Pydantic based actor implementation that receives messages of a
    given type `T`.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        self._raw_mailbox: RawMailbox = create_mailbox()
        self.mailbox: Mailbox[*T]

        if TYPE_CHECKING:
            self.mailbox = Mailbox[*T](self._raw_mailbox)
        else:
            t = self._typevar_to_args[T]
            self.mailbox = Mailbox[*t](self._raw_mailbox)

    def actor_ref(self) -> SimpleRef[*T]:
        address = self._raw_mailbox.address

        if TYPE_CHECKING:
            return SimpleRef[*T](actor_address=address)
        else:
            t = self._typevar_to_args[T]
            return SimpleRef[*t](actor_address=address)

    @abstractmethod
    async def actor_run(self) -> None:
        pass


@runtime_generic
class Mailbox[*T]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(self, raw_mailbox: RawMailbox) -> None:
        self._raw_mailbox = raw_mailbox

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[*T]:
        if TYPE_CHECKING:
            type_ = tuple[*T]
        else:
            t = self._typevar_to_args[T]
            type_ = tuple[*t]

        while True:
            message = await self._raw_mailbox.next()
            try:
                msg: tuple[*T] = deserialize(message, type=type_)
            except msgspec.ValidationError:
                logger.warning(
                    "Cannot deserialize message in `SimpleActor` mailbox type=%s, message=%s",
                    repr(type_),
                    repr(message[:30] + "..." if len(message) > 30 else message),
                )
                continue
            else:
                return msg


@runtime_generic
class SimpleRef[*T]:
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    def __init__(self, actor_address: ActorAddress) -> None:
        # assert hasattr(self, "_args")

        self.actor_address = actor_address

    def tell(self, *message: *T) -> None:
        """
        Send message to the underlying actor.
        """
        from actorium.system import _get_system

        serialized_message = serialize(message)
        _get_system().call_actor_soon(self.actor_address, serialized_message)

    def __call__(self, *message: *T) -> None:
        self.tell(*message)
