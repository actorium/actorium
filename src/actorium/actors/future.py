import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, get_args

from ..core import Actor, Mailbox, Ref, spawn
from ._generic import generic_function

__all__ = [
    "Future",
    "future",
]


class Future[T](Actor[T]):
    def __init__(self, fut: asyncio.Future[T]) -> None:
        self._future = fut

    def message_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    async def run(self, mailbox: Mailbox[T]) -> None:
        msg = await mailbox.next()
        self._future.set_result(msg)

    async def result(self) -> T:
        return await self._future


if TYPE_CHECKING:

    class future[T]:
        async def __aenter__(self) -> tuple[asyncio.Future[T], Ref[T]]: ...
        async def __aexit__(self, *_: object) -> None: ...

else:

    @generic_function
    @asynccontextmanager
    async def future[T]() -> AsyncGenerator[tuple[asyncio.Future[T], Ref[T]]]:
        """
        Factory for spawning a future.
        """
        fut = asyncio.Future[T]()
        async with spawn(Future[T], fut) as ref:
            yield fut, ref
