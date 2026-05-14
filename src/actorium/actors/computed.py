from collections.abc import Callable, Coroutine
from inspect import signature
from typing import TYPE_CHECKING, Any, final

from ..core import Actor, Mailbox, spawn
from .signals import signal

__all__ = [
    "computed",
]


@final
class _Unknown:
    pass


def computed[T, U, V](
    reactive1: signal[T],
    reactive2: signal[U],
    name: str | None = None,
) -> Callable[
    [Callable[[T, U], Coroutine[Any, Any, V]]],
    signal[V],
]:

    def decorator(func: Callable[[T, U], Coroutine[Any, Any, V]]) -> signal[V]:
        """
        Produces a reactive `SignalRef` actor for which it's value is computed-
        using `func`, observing `reactive1` and `reactive2`. Sometimes also called
        a 'memo'.

        Usage::

            @computed(ref1, ref2)
            async def ref3(value1, value2) -> int:
                " Whenever `ref1` or `ref2` change, this computed is reevaluated."
                ...


        Evaluation is eager, cached and the cache is invalidated when any of the
        observed actors change.

        A computed can be used as a simple "effect" if the returned `ref` is not
        needed.
        """
        value1: T | _Unknown = _Unknown()
        value2: U | _Unknown = _Unknown()

        sig = signature(func)

        if TYPE_CHECKING:
            result = signal[V].new()
        else:
            result = signal[sig.return_annotation].new(name=name)

        async def recompute() -> None:
            if not isinstance(value1, _Unknown) and not isinstance(value2, _Unknown):
                new_value = await func(value1, value2)
                result.set(new_value)

        class Update1(Actor[T]):
            async def run(self, mailbox: Mailbox[T]) -> None:
                nonlocal value1

                async with reactive1.subscribe(mailbox.ref()):
                    async for value1 in mailbox:
                        await recompute()

            def message_type(self) -> type[T]:
                return reactive1.data_type()

        class Update2(Actor[U]):
            async def run(self, mailbox: Mailbox[U]) -> None:
                nonlocal value2

                async with reactive2.subscribe(mailbox.ref()):
                    async for value2 in mailbox:
                        await recompute()

            def message_type(self) -> type[U]:
                return reactive2.data_type()

        # Spawn two actors for receiving corresponding updates.
        # Subscribe to updates coming from both reactive objects.
        spawn(Update1)
        spawn(Update2)

        return result

    return decorator
