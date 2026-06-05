from collections.abc import Callable
from inspect import signature
from typing import TYPE_CHECKING, Any, Union, final

from typemap_extensions import Iter

from actorium.system import spawn

from .behaviors import BehaviorActor, behavior
from .signals import Signal, SignalRef, SignalSubscribeWithId, Undefined

__all__ = [
    "computed",
]


@final
class _Unknown:
    pass


def computed[*T, V](
    *reactives: *[SignalRef[t] for t in Iter[tuple[*T]]],
    name: str | None = None,
) -> Callable[[Callable[[*T], V]], SignalRef[V]]:
    """
    Decorator for producing a new reactive `signal` based on the given signals
    by applying the decorated function on the input signals whenever any value
    changes. Sometimes also called a 'memo'.

    Usage::

        @computed(ref1, ref2)
        def ref3(value1, value2) -> int:
            " Whenever `ref1` or `ref2` change, this computed is reevaluated."
            ...


    Evaluation is eager, cached and the cache is invalidated when any of the
    observed actors change.

    A computed can be used as a simple "effect" if the returned `ref` is not
    needed.
    """

    def decorator(func: Callable[[*T], V]) -> SignalRef[V]:
        sig = signature(func)

        if not TYPE_CHECKING:
            V = sig.return_annotation

        result = spawn(Signal[V], Undefined, name=name)

        # Spawn observer actor that calls the given func when we receive
        # updates.
        if not TYPE_CHECKING:  # mypy crashes on the following line.
            spawn(_Observer[*T, V], func, result, *reactives)

        return result

    return decorator


# class _Observer[*T, V](Actor[tuple[int, Union[*T]]]):
class _Observer[*T, V](BehaviorActor):
    """
    Actor that subscribes to all the given signals, recomputes the outcome
    using the given callable and stores it in the signal.
    """

    def __init__(
        self,
        func: Callable[[*T], V],
        result: SignalRef[V],
        *reactives: *[SignalRef[t] for t in Iter[tuple[*T]]],
    ) -> None:
        self._func = func
        self._result = result
        self._reactives = reactives

        self._values: list[Union[*T] | _Unknown] = [_Unknown() for _ in self._reactives]

    async def actor_init(self) -> None:
        for i, reactive in enumerate(self._reactives):
            # typing: `Union[*T]` instead of `Any`.
            spawn(SignalSubscribeWithId[Any], reactive, self.actor_ref().changed, i)

    @behavior
    # async def changed(self, id: int, value: Union[*T]) -> None:
    def changed(self, id: int, value: Any) -> None:
        self._values[id] = value

        # Recompute when there are no Unknowns left.
        if any(isinstance(val, _Unknown) for val in self._values):
            return

        new_value = self._func(*self._values)  # type: ignore
        self._result.set(new_value)
