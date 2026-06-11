from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from anyio import fail_after
from msgspec import Struct
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox
from actorium.serialization import deserialize, serialize
from actorium.types import ActorAddress
from actorium.utils import generic_class_getitem

from .future import Future
from .simple import SimpleRef

__all__ = [
    "RpcActor",
    "RpcRef",
    "RpcMessage",
]


class RpcMessage[I: tuple[Any, ...], O](Struct):
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

    __class_getitem__ = generic_class_getitem

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        if not TYPE_CHECKING:
            I = self._args[:-1]
            O = self._args[-1]

        self._raw_mailbox: RawMailbox = create_mailbox()
        self.mailbox = RpcMailbox[tuple[*I], O](
            # Make sure that if `actor_ref` is overridden, that we take the
            # message type from there.
            RpcMessage[tuple[*I], O],
            self._raw_mailbox,
        )

    def actor_ref(self) -> RpcRef[*I, O]:
        if not TYPE_CHECKING:
            I = self._args[:-1]
            O = self._args[-1]

        return RpcRef[*I, O](actor_address=self._raw_mailbox.address)

    @abstractmethod
    async def actor_run(self) -> None:
        pass


class RpcMailbox[I: tuple[Any, ...], O]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    __class_getitem__ = generic_class_getitem

    def __init__(
        self,
        message_type: TypeForm[RpcMessage[I, O]],
        raw_mailbox: RawMailbox,
    ) -> None:
        assert hasattr(self, "_args")
        self._message_type = message_type
        self._raw_mailbox = raw_mailbox

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> RpcMessage[I, O]:
        if not TYPE_CHECKING:
            I = self._args[0]
            O = self._args[1]

        message = await self._raw_mailbox.next()

        msg: RpcMessage[I, O] = deserialize(message, type=RpcMessage[I, O])
        return msg


class RpcRef[*I, O]:
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

    async def __call__(self, *inputs: *I, timeout: float | None = None) -> O:
        from actorium.system import _get_system

        type I2 = Any  # `tuple[*I]`, which is not yet supported.
        if not TYPE_CHECKING:
            I2 = tuple[self._args[:-1]]
            O = self._args[-1]

        reply_to = Future[O]()
        msg = RpcMessage[I2, O](
            inputs=inputs,
            reply_to=reply_to.actor,
        )
        _get_system().call_actor_soon(self.actor_address, serialize(msg))

        with fail_after(timeout):
            return await reply_to.result()
