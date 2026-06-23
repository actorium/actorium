import time
from functools import cache, wraps
from types import FunctionType
from typing import Any, Callable, Iterable, Protocol, TypeVar

__all__ = [
    "TtlMap",
    "TtlSet",
    "GenericFunction",
    "generic_function",
]


class TtlMap[K, V]:
    def __init__(self) -> None:
        self._data: dict[K, V] = {}
        self._valid_until: dict[K, float] = {}

    def __repr__(self) -> str:
        return f"TtlMap({self._data!r})"

    def set(self, k: K, v: V, ttl_seconds: float) -> None:
        self._data[k] = v
        self._valid_until[k] = time.time() + ttl_seconds

    def items(self) -> Iterable[tuple[K, V]]:
        now = time.time()

        for k in list(self._data):  # `self.get` can remove expired entries.
            v = self.get(k, _now=now)
            if v is not None:
                yield k, v

    def keys(self) -> Iterable[K]:
        for k, _ in self.items():
            yield k

    def items_with_remaining_ttl(self) -> Iterable[tuple[K, V, float]]:
        now = time.time()

        for k in list(self._data):  # `self.get` can remove expired entries.
            v = self.get(k, _now=now)
            if v is not None:
                yield k, v, self._valid_until[k] - now

    def get(self, k: K, _now: float | None = None) -> V | None:
        now = _now or time.time()
        try:
            v = self._data[k]
            valid_until = self._valid_until[k]
        except KeyError:
            return None
        else:
            if valid_until >= now:
                return v

            # Key expired.
            del self._data[k]
            del self._valid_until[k]
            return None

    def pop(self, k: K) -> None:
        try:
            del self._data[k]
            del self._valid_until[k]
        except KeyError:
            pass


class TtlSet[K]:
    def __init__(self) -> None:
        self._map = TtlMap[K, int]()

    def add(self, k: K, ttl_seconds: float) -> None:
        # We store `1` values instead of `None` in order to easily distinguish
        # in the `get()` call.
        self._map.set(k, 1, ttl_seconds=ttl_seconds)

    def discard(self, k: K) -> None:
        self._map.pop(k)

    def __len__(self) -> int:
        # Number of items that are not expired.
        return len(list(self.iter()))

    def iter(self) -> Iterable[K]:
        for k, _ in self._map.items():
            yield k


class GenericFunction[*P, R](Protocol):
    def __call__(self, *a: *P) -> R: ...
    def __getitem__(self, params: Any) -> Callable[[*P], R]: ...


def generic_function[*P, R](func: Callable[[*P], R]) -> GenericFunction[*P, R]:
    """
    Function decorator that allows calling a generic function with type
    parameters, and expose the actual types within the function.

    See: https://github.com/python/typing/discussions/2199
    """

    class wrapper:
        @cache
        def __getitem__(
            self, type_params: TypeVar | tuple[TypeVar, ...]
        ) -> Callable[[*P], R]:
            if not isinstance(type_params, tuple):
                type_params = (type_params,)

            # Map typevar names to types that we receive in vars.
            typevar_name_to_type = {
                name: type_ for type_, name in zip(type_params, func.__type_params__)
            }

            # Helper for creating a function closure.
            def make_cell(value: object) -> Any:
                def inner() -> object:
                    return value

                return inner.__closure__[0]  # type:ignore[index]

            # Create a new closure for the given function by replacing the type
            # variables with the actual types.
            def replace_closure(f: FunctionType) -> tuple[Any, ...]:
                closure = []
                for cell in f.__closure__:  # type:ignore[union-attr]
                    contents = cell.cell_contents
                    if isinstance(contents, TypeVar):
                        closure.append(make_cell(typevar_name_to_type[contents]))
                    elif hasattr(contents, "__closure__"):
                        closure.append(make_cell(replace_func(contents)))
                    else:
                        closure.append(cell)
                return tuple(closure)

            def replace_func(f: FunctionType) -> FunctionType:
                return FunctionType(
                    f.__code__,
                    f.__globals__,
                    name=f.__name__,
                    argdefs=f.__defaults__,
                    closure=replace_closure(f),
                )

            return replace_func(func)  # type:ignore[return-value,arg-type]

        # `__call__` staticmethod to make `inspect.signature` work on the
        # `GenericFunction`.
        @staticmethod
        @wraps(func)
        def __call__(*a: *P) -> R:
            return func(*a)

    wrapper.__doc__ = func.__doc__
    return wrapper()
