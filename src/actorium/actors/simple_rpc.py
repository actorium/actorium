from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

import msgspec
from anyio import fail_after
from msgspec import Struct

from actorium.actor import BaseActor, RawMailbox
from actorium.logger import logger
from actorium.runtime_generic import runtime_generic
from actorium.serialization import deserialize, serialize
from actorium.types import ActorAddress

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


@runtime_generic
class RpcActor[*I, O](BaseActor):
    """
    Simple Pydantic based actor implementation that receives messages of a
    given type `T`.

    This deserializes all incoming messages using the type: `T`, which
    should be a Pydantic `BaseModel` or anything supported by Pydantic's
    `TypeAdapter`.
    """

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        self._raw_mailbox: RawMailbox = create_mailbox()

        if TYPE_CHECKING:
            self.mailbox = RpcMailbox[tuple[*I], O](self._raw_mailbox)
        else:
            i = self._typevar_to_args[I]
            o = self._typevar_to_args[O]
            self.mailbox = RpcMailbox[tuple[*i], o](self._raw_mailbox)

    def actor_ref(self) -> RpcRef[*I, O]:
        if TYPE_CHECKING:
            return RpcRef[*I, O](actor_address=self._raw_mailbox.address)
        else:
            i = self._typevar_to_args[I]
            o = self._typevar_to_args[O]
            return RpcRef[*i, o](actor_address=self._raw_mailbox.address)

    @abstractmethod
    async def actor_run(self) -> None:
        pass


@runtime_generic
class RpcMailbox[I: tuple[Any, ...], O]:
    """
    Mailbox for a single actor from where the actor can receive messages.
    """

    def __init__(self, raw_mailbox: RawMailbox) -> None:
        assert hasattr(self, "_typevar_to_args")
        self._raw_mailbox = raw_mailbox

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> RpcMessage[I, O]:
        if TYPE_CHECKING:
            type_ = RpcMessage[I, O]
        else:
            i = self._typevar_to_args[I]
            o = self._typevar_to_args[O]
            type_ = RpcMessage[i, o]

        while True:
            message = await self._raw_mailbox.next()
            try:
                msg: RpcMessage[I, O] = deserialize(message, type=type_)
            except msgspec.ValidationError:
                breakpoint()
                logger.warning(
                    "Cannot deserialize message in `RpcActor` mailbox type=%s, message=%s",
                    repr(type_),
                    repr(message[:30] + "..." if len(message) > 30 else message),
                )
                continue
            else:
                return msg


@runtime_generic
class RpcRef[*I, O]:
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    def __init__(self, actor_address: ActorAddress) -> None:
        assert hasattr(self, "_typevar_to_args")

        self.actor_address = actor_address

    async def __call__(self, *inputs: *I, timeout: float | None = None) -> O:
        from actorium.system import _get_system

        if TYPE_CHECKING:
            reply_to = Future[O]()
            msg = RpcMessage[tuple[*I], O](
                inputs=inputs,
                reply_to=reply_to.actor,
            )

        else:
            i = self._typevar_to_args[I]
            o = self._typevar_to_args[O]

            reply_to = Future[o]()
            msg = RpcMessage[tuple[*i], o](
                inputs=inputs,
                reply_to=reply_to.actor,
            )

        _get_system().call_actor_soon(self.actor_address, serialize(msg))

        with fail_after(timeout):
            return await reply_to.result()
