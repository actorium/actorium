import inspect
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

from ..actors import Actor, spawn
from .signals import SignalReader, signal

__all__ = [
    "computed",
]


@asynccontextmanager
async def computed[T, U, V](
    type_: type[T],
    /,
    func: Callable[[T, U], Coroutine[Any, Any, V]],
    reactive1: SignalReader[T],
    reactive2: SignalReader[U],
) -> AsyncGenerator[SignalReader[V]]:
    """
    Produces a reactive `SignalReader` actor for which it's value is computed
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
    func_argument_types = list(inspect.signature(func).parameters.values())

    async with signal(type_, initial) as (result, set_result):

        class Update1(Actor[T]):
            async def receive(self, msg: T) -> None:
                nonlocal value1
                value1 = msg

                new_value = await func(value1, value2)
                await set_result(new_value)

            def message_type(self) -> type[T]:
                return reactive1.message_type()

        class Update2(Actor[U]):
            async def receive(self, msg: U) -> None:
                nonlocal value2
                value2 = msg

                new_value = await func(value1, value2)
                await set_result(new_value)

            def message_type(self) -> type[T]:
                return reactive2.message_type()

            receive.__annotations__ = {"msg": func_argument_types[1].annotation}

        async with (
            # Spawn two actors for receiving corresponding updates.
            spawn(Update1) as (_, update1_actor),
            spawn(Update2) as (_, update2_actor),
            # Subscribe to updates coming from both reactive objects.
            reactive1.subscribe(update1_actor),
            reactive2.subscribe(update2_actor),
        ):
            yield result
