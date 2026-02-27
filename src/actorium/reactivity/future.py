import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, get_args

from ..actors import Actor, ActorRef, spawn
from ._generic import generic_function

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


@generic_function
@asynccontextmanager
async def future[T]() -> AsyncGenerator[tuple[Future[T], ActorRef[T]]]:
    """
    Factory for spawning a future.
    """
    async with spawn(Future[T]) as (fut, ref):
        yield fut, ref
