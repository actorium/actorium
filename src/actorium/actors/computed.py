from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

from ..core import Actor, Mailbox, spawn
from .signals import SignalRef, signal

__all__ = [
    "computed",
]


@asynccontextmanager
async def computed[T, U, V](
    type_: type[T],
    /,
    func: Callable[[T, U], Coroutine[Any, Any, V]],
    reactive1: SignalRef[T],
    reactive2: SignalRef[U],
) -> AsyncGenerator[SignalRef[V]]:
    """
    Produces a reactive `SignalRef` actor for which it's value is computed
    using `func`, observing `reactive1` and `reactive2`. Sometimes also called
    a 'memo'.

    Usage::

        async def evaluate(value1, value2) -> int:
            " Whenever `ref1` or `ref2` change, this computed is reevaluated."
            ...

        async with computed(evaluate, ref1, ref2) -> ref3: ...

    Evaluation is eager, cached and the cache is invalidated when any of the
    observed actors change.

    A computed can be used as a simple "effect" if the returned `ref` is not
    needed.
    """
    value1 = await reactive1.get()
    value2 = await reactive2.get()

    initial = await func(value1, value2)

    async with signal[type_](initial) as result:

        class Update1(Actor[T]):
            async def run(self, mailbox: Mailbox[T]) -> None:
                nonlocal value1

                async for value1 in mailbox:
                    new_value = await func(value1, value2)
                    result.set(new_value)

            def message_type(self) -> type[T]:
                return reactive1.data_type()

        class Update2(Actor[U]):
            async def run(self, mailbox: Mailbox[U]) -> None:
                nonlocal value2

                async for value2 in mailbox:
                    new_value = await func(value1, value2)
                    result.set(new_value)

            def message_type(self) -> type[U]:
                return reactive2.data_type()

        async with (
            # Spawn two actors for receiving corresponding updates.
            spawn(Update1) as update1_actor,
            spawn(Update2) as update2_actor,
            # Subscribe to updates coming from both reactive objects.
            reactive1.subscribe(update1_actor),
            reactive2.subscribe(update2_actor),
        ):
            yield result
