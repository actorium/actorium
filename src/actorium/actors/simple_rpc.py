from abc import abstractmethod
from collections.abc import Callable
from functools import cache
from typing import TYPE_CHECKING, Any, ClassVar, Never, Self

from anyio import fail_after
from pydantic import BaseModel, ConfigDict, TypeAdapter
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox, SerializedMessage
from actorium.types import ActorAddress

from .future import Future
from .simple import SimpleRef

__all__ = [
    "RpcActor",
    "RpcRef",
    "RpcMessage",
]


class RpcMessage[I: tuple[Any, ...], O](BaseModel):
    """
    Message type sent from `BehaviorRef` to `BehaviorActor`.
    """

    # Behavior input, tuple[] of arguments.
    inputs: I

    # RPC output, actor address where the response is sent to.
    reply_to: SimpleRef[O]


class RpcActor[*I, O](BaseActor):
    """
    Simple Pydantic based actor implementation that receives messages of a
    given type `T`.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    _i: ClassVar[TypeForm[tuple[*I] | None]] = None
    _o: ClassVar[TypeForm[O | None]] = None

    @cache
    @staticmethod
    def __class_getitem__(*items: TypeForm[Any]) -> type:
        class _RpcActor(RpcActor):  # type: ignore
            _i = items[:-1]  # type: ignore
            _o = items[-1]

        return _RpcActor

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        if not TYPE_CHECKING:
            I = self._i
            O = self._o

        self._raw_mailbox: RawMailbox = create_mailbox()
        self.mailbox = RpcMailbox[tuple[*I], O](
            # Make sure that if `actor_ref` is overridden, that we take the
            # message type from there.
            RpcMessage[tuple[*I], O],
            self._raw_mailbox,
        )

    def actor_ref(self) -> RpcRef[*I, O]:
        if not TYPE_CHECKING:
            I = self._i
            O = self._o

        return RpcRef[*I, O](actor_address=self._raw_mailbox.address)

    @abstractmethod
    async def actor_run(self) -> None:
        pass


class RpcMailbox[I: tuple[Any, ...], O]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(
        self,
        message_type: TypeForm[RpcMessage[I, O]],
        raw_mailbox: RawMailbox,
    ) -> None:
        self._message_type = message_type
        self._raw_mailbox = raw_mailbox
        self._type_adapter: TypeAdapter[RpcMessage[I, O]] = TypeAdapter(message_type)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> RpcMessage[I, O]:
        message = await self._raw_mailbox.next()

        if isinstance(message, SerializedMessage):
            msg: RpcMessage[I, O] = self._type_adapter.validate_json(message.data)
            return msg

        return self._type_adapter.validate_python(message)


class RpcRef[*I, O](BaseModel):
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

    async def __call__(self, *inputs: *I, timeout: float | None = None) -> O:
        from actorium.system import _get_system

        if not TYPE_CHECKING:
            I = self.__pydantic_generic_metadata__["args"][0]
            O = self.__pydantic_generic_metadata__["args"][1]

        reply_to = Future[O]()
        # msg = RpcMessage[tuple[*I], O](
        msg = RpcMessage[Any, O](
            inputs=inputs,
            reply_to=reply_to.actor,
        )
        _get_system().call_actor_soon(self.actor_address, msg, self._serialize)

        with fail_after(timeout):
            return await reply_to.result()

    @classmethod
    def _serialize(cls, message: RpcMessage[tuple[*I], O]) -> SerializedMessage:
        breakpoint()
        type_adapter: TypeAdapter[RpcMessage[tuple[*I], O]] = TypeAdapter(
            cls.message_type()
        )
        return SerializedMessage(data=type_adapter.dump_json(message).decode())
