from collections.abc import Callable, Coroutine
from typing import Any

from anyio import sleep

__all__ = [
    "assert_soon_equal",
]


async def assert_soon_equal[T](
    obj1: Callable[[], T | Coroutine[Any, Any, T]], obj2: T
) -> None:
    async def get() -> T:
        value = obj1()
        if isinstance(value, Coroutine):
            return await value
        return value

    for i in range(1000):
        if await get() == obj2:
            return
        await sleep(0.01)

    assert await get() == obj2
