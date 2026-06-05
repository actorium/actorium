import asyncio
from typing import TYPE_CHECKING, get_args

from actorium.system import spawn

from .simple import SimpleActor

__all__ = [
    "FutureActor",
    "Future",
]


class FutureActor[T](SimpleActor[T]):
    def __init__(self, fut: asyncio.Future[T]) -> None:
        self._future = fut

    def message_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    async def actor_run(self) -> None:
        async for (msg,) in self.mailbox:
            self._future.set_result(msg)
            return

    async def result(self) -> T:
        return await self._future


class Future[T]:
    # __orig_class__ is not available in __init__, so we use __class_getitem__
    # as a workaround.
    def __class_getitem__(cls, item: type) -> type:
        class _Future(cls):  # type: ignore
            _type = item

        return _Future

    def __init__(self) -> None:
        if not hasattr(self, "_type"):
            raise RuntimeError("Future not instantiated with type parameter.")

        if not TYPE_CHECKING:
            T = self._type

        self._asyncio_future = asyncio.Future[T]()
        self.actor = spawn(FutureActor[T], self._asyncio_future)

    async def result(self) -> T:
        return await self._asyncio_future
