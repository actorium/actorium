import asyncio
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from typing import Any, Literal

from pydantic import BaseModel

from ..actors import ActorRef, ActorRegistration, spawn

__all__ = [
    "CallRpc",
    "Rpc",
    "rpc",
]


class CallRpc[In, Out](BaseModel):
    "Actor message for telling the RPC actor to perform an RPC call."

    type: Literal["call-rpc"] = "call-rpc"

    # Input payload.
    input: In

    # Actor where the output should be send to.
    reply_to: ActorRef[Out]


class _RpcActor[In, Out]:
    def __init__(self, func: Callable[[In], Coroutine[Any, Any, Out]]) -> None:
        self.func = func

    async def receive(self, msg: CallRpc[In, Out]) -> None:
        result = await self.func(msg.input)
        await msg.reply_to.send(result)


class Rpc[In, Out](ActorRef[CallRpc[In, Out]]):
    """
    Reference to an RPC actor with helper methods for Calling remote functions.
    """

    async def call(self, value: In) -> Out:
        f = asyncio.Future[Out]()

        def get_result(msg: Out) -> None:
            f.set_result(msg)

        async with spawn(get_result) as reply_to:
            await self.send(CallRpc[In, Out](input=value, reply_to=reply_to))
            return await f


def rpc[In, Out](
    func: Callable[[In], Coroutine[Any, Any, Out]],
    /,
    register: ActorRegistration[Rpc[In, Out]] | None = None,
) -> AbstractAsyncContextManager[Rpc[In, Out]]:
    """
    Register a new RPC actor.

    Usage::

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc(double_it) as double_actor:
            assert await double_actor.call(2) == 4
    """
    rpc_actor = _RpcActor[In, Out](func)
    return Rpc[In, Out].spawn(rpc_actor.receive, register=register)
