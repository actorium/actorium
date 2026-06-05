from collections.abc import Callable
from typing import Never

from anyio import sleep

from .simple import SimpleActor

__all__ = [
    "CallAfterTimeout",
]


class CallAfterTimeout(SimpleActor[Never]):
    """
    Call the given function after the given amount of seconds.

    Warning: the given function (which can be a lambda expression) should not
    capture any mutable state. It can capture actor references (they are
    immutable).
    """

    def __init__(self, seconds: float, func: Callable[[], None]) -> None:
        self._seconds = seconds
        self._func = func

    async def actor_run(self) -> None:
        await sleep(self._seconds)
        self._func()
