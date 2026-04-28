from __future__ import annotations

from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Literal, get_args

from anyio import create_task_group, fail_after
from pydantic import BaseModel

from ..core import Actor, ActorAddress, Mailbox, Ref, Timeout, spawn
from ._generic import generic_function
from .future import future

__all__ = [
    "CallRpc",
    "RpcRef",
    "rpc",
]


class CallRpc[In, Out](BaseModel):
    "Actor message for telling the RPC actor to perform an RPC call."

    type: Literal["call-rpc"] = "call-rpc"

    # Input payload.
    input: In

    # Actor where the output should be send to.
    reply_to: Ref[Out]


class RpcActor[In, Out](Actor[CallRpc[In, Out]]):
    def __init__(self, func: Callable[[In], Coroutine[Any, Any, Out]]) -> None:
        self.func = func

    def message_type(self) -> type[CallRpc[In, Out]]:
        if not TYPE_CHECKING:
            In, Out = get_args(self.__orig_class__)
        return CallRpc[In, Out]

    def actor_ref(self, actor_address: ActorAddress) -> RpcRef[In, Out]:
        if not TYPE_CHECKING:
            In, Out = get_args(self.__orig_class__)
        return RpcRef[In, Out](actor_address=actor_address)

    async def run(self, mailbox: Mailbox[CallRpc[In, Out]]) -> None:
        async with create_task_group() as tg:
            async for msg in mailbox:
                tg.start_soon(self._handle_rpc_call, msg)
                # TODO: Our introspection is not able to pick up the `msg` type here
                #       due to the generics...

    async def _handle_rpc_call(self, msg: CallRpc[In, Out]) -> None:
        result = await self.func(msg.input)
        msg.reply_to.tell(result)


class RpcRef[In, Out](Ref[CallRpc[In, Out]]):
    """
    Reference to an RPC actor with helper methods for Calling remote functions.
    """

    @classmethod
    def message_type(cls) -> type:
        in_type, out_type = cls.__pydantic_generic_metadata__["args"]
        return CallRpc[in_type, out_type]  # type:ignore

    async def ask(self, value: In, timeout: float | None = None) -> Out | Timeout:
        if not TYPE_CHECKING:
            In, Out = self.__pydantic_generic_metadata__["args"]

        async with future[Out]() as (f, reply_to):
            self.tell(CallRpc[In, Out](input=value, reply_to=reply_to))
            try:
                with fail_after(timeout):
                    return await f
            except TimeoutError:
                return Timeout()


@generic_function
@asynccontextmanager
async def rpc[In, Out](
    func: Callable[[In], Coroutine[Any, Any, Out]],
) -> AsyncGenerator[RpcRef[In, Out]]:
    """
    Register a new RPC actor.

    Usage::

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc[int, int](double_it) as double_actor:
            assert await double_actor.ask(2) == 4
    """
    async with spawn(RpcActor[In, Out], func) as ref:
        yield ref
