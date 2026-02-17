import types
from collections.abc import Callable, Coroutine
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, Literal, get_args

from anyio import fail_after
from pydantic import BaseModel

from ..actors import Actor, ActorRef, spawn
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
    reply_to: ActorRef[Out]


class RpcActor[In, Out](Actor[CallRpc[In, Out]]):
    def __init__(self, func: Callable[[In], Coroutine[Any, Any, Out]]) -> None:
        self.func = func

    def message_type(self) -> type[CallRpc[In, Out]]:
        in_type, out_type = get_args(self.__orig_class__)  # type:ignore
        return CallRpc[in_type, out_type]  # type:ignore

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
        return CallRpc[in_type, out_type]  # type:ignore

    async def ask(self, value: In, timeout: float | None = None) -> Out:
        if TYPE_CHECKING:
            async with future[Out]() as (f, reply_to):
                self.tell(CallRpc[In, Out](input=value, reply_to=reply_to))
                with fail_after(timeout):
                    return await f.result()
        else:
            in_type, out_type = self.__pydantic_generic_metadata__["args"]

            async with future[out_type]() as (f, reply_to):
                self.tell(CallRpc[in_type, out_type](input=value, reply_to=reply_to))
                with fail_after(timeout):
                    return await f.result()


class rpc[In, Out]:
    """
    Register a new RPC actor.

    Usage::

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc[int, int](double_it) as double_actor:
            assert await double_actor.ask(2) == 4
    """

    def __init__(self, func: Callable[[In], Coroutine[Any, Any, Out]]) -> None:
        self.func = func

    async def __aenter__(self) -> RpcRef[In, Out]:
        self._stack = await AsyncExitStack().__aenter__()

        if TYPE_CHECKING:
            rpc_actor, ref = await self._stack.enter_async_context(
                spawn(RpcActor[In, Out], self.func)
            )
            return ref.wrap(RpcRef[In, Out])
        else:
            in_type, out_type = get_args(self.__orig_class__)
            rpc_actor, ref = await self._stack.enter_async_context(
                spawn(RpcActor[in_type, out_type], self.func)
            )
            return ref.wrap(RpcRef[in_type, out_type])

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        return await self._stack.__aexit__(exc_type, exc_value, traceback)
