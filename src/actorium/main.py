import contextvars
from typing import Any, Callable, Coroutine, Never

import anyio

from .actor import ActorFactory, BaseActor
from .system import ActorSystem

__all__ = [
    "run",
    "create_actor_system_and_run",
]


def run[A: BaseActor, R, *P](factory: ActorFactory[A, R, *P], /, *args: *P) -> None:
    async def main() -> None:
        await ActorSystem.create_and_run(factory, *args)

    ctx = contextvars.copy_context()
    ctx.run(anyio.run, main)


def create_actor_system_and_run[T](func: Callable[[], Coroutine[Any, Any, T]]) -> T:
    result: list[T] = []
    from .actors.simple import SimpleActor

    class Main(SimpleActor[Never]):
        async def actor_run(self) -> None:
            result.append(await func())

    async def main() -> None:
        await ActorSystem.create_and_run(Main)

    ctx = contextvars.copy_context()
    ctx.run(anyio.run, main)

    assert len(result) == 1
    return result[0]
