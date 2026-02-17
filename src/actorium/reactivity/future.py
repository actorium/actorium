import asyncio
from contextlib import asynccontextmanager
from typing import get_args

from ..actors import Actor, ActorRef, spawn

__all__ = [
    "Future",
    "create_future",
]


class Future[T](Actor[T]):
    def __init__(self) -> None:
        self._future = asyncio.Future[T]()

    def message_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]

    async def receive(self, msg: T) -> None:
        self._future.set_result(msg)

    async def result(self) -> T:
        return await self._future


@asynccontextmanager
async def create_future[T](type_: type[T]) -> tuple[Future[T], ActorRef[T]]:
    async with spawn(Future[type_]) as (future, ref):  # type:ignore
        yield future, ref
