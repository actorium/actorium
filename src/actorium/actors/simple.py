from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Self

import msgspec
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox
from actorium.logger import logger
from actorium.serialization import deserialize, serialize
from actorium.types import ActorAddress
from actorium.utils import generic_class_getitem

__all__ = [
    "SimpleActor",
    "Mailbox",
    "SimpleRef",
]


class SimpleActor[*T](BaseActor):
    """
    Simple Pydantic based actor implementation that receives messages of a
    given type `T`.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    __class_getitem__ = generic_class_getitem

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        if not TYPE_CHECKING:
            T = self._args

        self._raw_mailbox: RawMailbox = create_mailbox()
        self.mailbox = Mailbox[*T](
            # Make sure that if `actor_ref` is overridden, that we take the
            # message type from there.
            tuple[*self._args],  # type:ignore[name-defined]
            self._raw_mailbox,
        )

    def actor_ref(self) -> SimpleRef[*T]:
        address = self._raw_mailbox.address

        if not TYPE_CHECKING:
            T = self._args

        return SimpleRef[*T](actor_address=address)

    @abstractmethod
    async def actor_run(self) -> None:
        pass


class Mailbox[*T]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    __class_getitem__ = generic_class_getitem

    def __init__(
        self,
        message_type: TypeForm[tuple[*T]],
        raw_mailbox: RawMailbox,
    ) -> None:
        self._message_type = message_type
        self._raw_mailbox = raw_mailbox

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[*T]:
        if not TYPE_CHECKING:
            T = self._args

        while True:
            message = await self._raw_mailbox.next()

            try:
                msg: tuple[*T] = deserialize(message, type=tuple[*T])
            except msgspec.ValidationError:
                logger.warning(
                    "Cannot deserialize message in actor mailbox type=%s, message=%s",
                    repr(tuple[*T]),
                    repr(message[:30] + "..." if len(message) > 30 else message),
                )
                continue
            else:
                return msg


class SimpleRef[*T]:
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    __class_getitem__ = generic_class_getitem

    def __init__(self, actor_address: ActorAddress) -> None:
        assert hasattr(self, "_args")

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
