from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Self, get_args

from typing import Never
from pydantic import BaseModel, ConfigDict, TypeAdapter
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox, SerializedMessage
from actorium.types import ActorAddress

__all__ = [
    "SimpleActor",
    "Mailbox",
    "SimpleRef",
]


class SimpleActor[T](BaseActor):
    """
    Simple Pydantic based actor implementation that receives messages of a
    given type `T`.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    def message_type(self) -> TypeForm[T]:
        if hasattr(self, "__orig_class__"):
            return get_args(self.__orig_class__)[0]  # type:ignore
        else:
            return get_args(self.__orig_bases__[0])[0]  # type:ignore

    def actor_ref(self, actor_address: ActorAddress) -> SimpleRef[T]:
        if not TYPE_CHECKING:
            T = self.message_type()

        return SimpleRef[T](actor_address=actor_address)

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
        self, message_type: TypeForm[T], raw_mailbox: RawMailbox, ref: SimpleRef[T]
    ) -> None:
        self._message_type = message_type
        self._raw_mailbox = raw_mailbox
        self._ref = ref

        self._type_adapter: None | TypeAdapter[T] = None

        if message_type is not Never:  # type: ignore
            try:
                self._type_adapter = TypeAdapter(message_type)
            except BaseException:
                breakpoint()

    def ref(self) -> SimpleRef[T]:
        return self._ref

    async def next(self) -> T:
        if self._type_adapter is None:
            raise TypeError("Can't receive messages in `SimpleActor[Never]`.")

        message = await self._raw_mailbox.next()

        if isinstance(message, SerializedMessage):
            msg: T = self._type_adapter.validate_json(message.data)
            return msg

        return self._type_adapter.validate_python(message)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        return await self.next()


class SimpleRef[T](BaseModel):
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    model_config = ConfigDict(frozen=True)

    actor_address: ActorAddress

    def model_post_init(self, __context: object) -> None:
        if self.message_type() is Never:  # type: ignore
            # `Never` means we can't receive messages.
            return

        try:
            TypeAdapter(self.message_type())
        except BaseException:
            breakpoint()

    @classmethod
    def message_type(cls) -> type[Any]:
        # NOTE: The return type is actually `type[T]`, but it doesn't matter in
        #       this context. We want `SimpleRef[T]` to be covariant.
        try:
            return cls.__pydantic_generic_metadata__["args"][0]  # type:ignore
        except IndexError:
            return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type:ignore

    def tell(self, message: T) -> None:
        """
        Send message to the underlying actor.
        """
        from actorium.system import _get_system

        _get_system().call_actor_soon(self.actor_address, message, self._serialize)

    @classmethod
    def _serialize(cls, message: T) -> SerializedMessage:
        type_adapter: TypeAdapter[T] = TypeAdapter(cls.message_type())
        return SerializedMessage(data=type_adapter.dump_json(message).decode())
