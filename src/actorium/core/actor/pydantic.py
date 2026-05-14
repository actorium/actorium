from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Literal, Self, get_args

from pydantic import BaseModel, TypeAdapter
from typing_extensions import TypeForm

from ..types import ActorAddress
from .base import BaseActor, RawMailbox, SerializedMessage

__all__ = [
    "Actor",
    "Mailbox",
    "Ref",
]


class Actor[T](BaseActor):
    """
    Pydantic based actor implementation.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    def actor_ref(self, actor_address: ActorAddress) -> Ref[T]:
        if not TYPE_CHECKING:
            if hasattr(self, "__orig_class__"):
                T = get_args(self.__orig_class__)[0]
            else:
                T = get_args(self.__orig_bases__[0])[0]  # type:ignore

        return Ref[T](actor_address=actor_address)

    async def actor_run(
        self, raw_mailbox: RawMailbox, actor_address: ActorAddress
    ) -> None:
        mailbox = Mailbox(
            # Make sure that if `actor_ref` is overridden, that we take the
            # message type from there.
            self.actor_ref(actor_address).message_type(),
            raw_mailbox,
            ref=self.actor_ref(actor_address),
        )
        await self.run(mailbox)

    @abstractmethod
    async def run(self, mailbox: Mailbox[T]) -> None:
        pass


class Mailbox[T]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(
        self, message_type: TypeForm[T], raw_mailbox: RawMailbox, ref: Ref[T]
    ) -> None:
        self._message_type = message_type
        self._raw_mailbox = raw_mailbox
        self._ref = ref

        self._type_adapter: TypeAdapter[T] = TypeAdapter(message_type)

    def ref(self) -> Ref[T]:
        return self._ref

    async def next(self) -> T:
        message = await self._raw_mailbox.next()

        if isinstance(message, SerializedMessage):
            msg: T = self._type_adapter.validate_json(message.data)
            return msg

        return self._type_adapter.validate_python(message)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        return await self.next()


class Ref[T](BaseModel):
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    # Discriminator, for when it's used in a union with other types.
    type_: Literal["actor-ref"] = "actor-ref"

    actor_address: ActorAddress

    model_config = {"frozen": True}

    def model_post_init(self, __context: object) -> None:
        TypeAdapter(self.message_type())

    @classmethod
    def message_type(cls) -> type[Any]:
        # NOTE: The return type is actually `type[T]`, but it doesn't matter in
        #       this context. We want `Ref[T]` to be covariant.
        try:
            return cls.__pydantic_generic_metadata__["args"][0]  # type:ignore
        except IndexError:
            return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type:ignore

    def tell(self, message: T) -> None:
        """
        Send message to the underlying actor.
        """
        from ..system import _get_system

        _get_system().call_actor_soon(self.actor_address, message, self._serialize)

    @classmethod
    def _serialize(cls, message: T) -> SerializedMessage:
        type_adapter: TypeAdapter[T] = TypeAdapter(cls.message_type())
        return SerializedMessage(data=type_adapter.dump_json(message).decode())
