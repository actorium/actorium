from collections.abc import Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Literal

from anyio import fail_after
from pydantic import BaseModel

from ..actors import Actor, ActorRef, spawn
from .future import create_future

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
    reply_to: ActorRef[Out]


class RpcActor[In, Out](Actor[CallRpc[In, Out]]):
    def __init__(
        self,
        in_type: type[In],
        out_type: type[Out],
        func: Callable[[In], Coroutine[Any, Any, Out]],
        /,
    ) -> None:
        self._in_type = in_type
        self._out_type = out_type
        self.func = func

    def message_type(self) -> type[CallRpc[In, Out]]:
        return CallRpc[self._in_type, self._out_type]  # type:ignore

    async def receive(self, msg: CallRpc[In, Out]) -> None:
        # TODO: Our introspection is not able to pick up the `msg` type here
        #       due to the generics...

        result = await self.func(msg.input)
        msg.reply_to.tell(result)


class RpcRef[In, Out](ActorRef[CallRpc[In, Out]]):
    """
    Reference to an RPC actor with helper methods for Calling remote functions.
    """

    @classmethod
    def message_type(cls) -> type:
        in_type, out_type = cls.__pydantic_generic_metadata__["args"]
        return CallRpc[in_type, out_type]

    async def ask(self, value: In, timeout: float | None = None) -> Out:
        generic_types = self.__pydantic_generic_metadata__["args"]
        in_type = generic_types[0]
        out_type = generic_types[1]

        async with create_future(out_type) as (future, reply_to):
            self.tell(CallRpc[in_type, out_type](input=value, reply_to=reply_to))
            with fail_after(timeout):
                return await future.result()


@asynccontextmanager
async def rpc[In, Out](
    in_type: type[In],
    out_type: type[Out],
    /,
    func: Callable[[In], Coroutine[Any, Any, Out]],
) -> AsyncGenerator[RpcRef[In, Out]]:
    """
    Register a new RPC actor.

    Usage::

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc(int, int, double_it) as double_actor:
            assert await double_actor.ask(2) == 4
    """
    async with spawn(RpcActor[In, Out], in_type, out_type, func) as (rpc_actor, ref):
        yield ref.wrap(RpcRef[in_type, out_type])
