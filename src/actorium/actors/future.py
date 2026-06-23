import asyncio
from typing import TYPE_CHECKING

from actorium.runtime_generic import runtime_generic
from actorium.system import spawn

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


@runtime_generic
class Future[T]:
    def __init__(self) -> None:
        if not hasattr(self, "_typevar_to_args"):
            raise RuntimeError("Future not instantiated with type parameter.")

        if not TYPE_CHECKING:
            t = self._typevar_to_args[T]

        self._asyncio_future = asyncio.Future[t]()
        self.actor = spawn(FutureActor[t], self._asyncio_future)

    async def result(self) -> T:
        return await self._asyncio_future
