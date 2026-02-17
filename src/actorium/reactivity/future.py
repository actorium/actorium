import asyncio
import types
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, get_args

from ..actors import Actor, ActorRef, spawn

__all__ = [
    "Future",
    "future",
]


class Future[T](Actor[T]):
    def __init__(self) -> None:
        self._future = asyncio.Future[T]()

    def message_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    async def receive(self, msg: T) -> None:
        self._future.set_result(msg)

    async def result(self) -> T:
        return await self._future


class future[T]:
    """
    Factory for spawning a future.
    """

    async def __aenter__(self) -> tuple[Future[T], ActorRef[T]]:
        self._stack = await AsyncExitStack().__aenter__()
        if TYPE_CHECKING:
            return await self._stack.enter_async_context(spawn(Future[T]))
        else:
            type_ = get_args(self.__orig_class__)[0]
            return await self._stack.enter_async_context(spawn(Future[type_]))

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        return await self._stack.__aexit__(exc_type, exc_value, traceback)
