import asyncio
from typing import TYPE_CHECKING

from actorium.system import spawn
from actorium.utils import generic_class_getitem

from .simple import SimpleActor

__all__ = [
    "FutureActor",
    "Future",
]


class FutureActor[T](SimpleActor[T]):
    def __init__(self, fut: asyncio.Future[T]) -> None:
        self._future = fut

    async def actor_run(self) -> None:
        async for (msg,) in self.mailbox:
            self._future.set_result(msg)
            return

    async def result(self) -> T:
        return await self._future


class Future[T]:
    __class_getitem__ = generic_class_getitem

    def __init__(self) -> None:
        if not hasattr(self, "_args"):
            raise RuntimeError("Future not instantiated with type parameter.")

        if not TYPE_CHECKING:
            T = self._args[0]

        self._asyncio_future = asyncio.Future[T]()
        self.actor = spawn(FutureActor[T], self._asyncio_future)

    async def result(self) -> T:
        return await self._asyncio_future
