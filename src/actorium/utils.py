import time
from collections.abc import Sequence
from functools import cache, wraps
from types import FunctionType, GenericAlias
from typing import Any, Callable, Iterable, Protocol, TypeVar, TypeVarTuple, get_origin

from typing_extensions import TypeForm

__all__ = [
    "TtlMap",
    "TtlSet",
    "GenericFunction",
    "generic_function",
    "substitute_type",
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


@classmethod
@cache
def generic_class_getitem(cls, items: Any) -> type:
    if not isinstance(items, tuple):
        return cls[(items,)]

    if "*" in repr(items):
        breakpoint()

    parameters = cls.__parameters__
    all_items = items
    _typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm] = {}

    while parameters and isinstance(parameters[0], TypeVar):
        _typevar_to_args[parameters[0]] = items[0]
        parameters = parameters[1:]
        items = items[1:]

    while parameters and isinstance(parameters[-1], TypeVar):
        _typevar_to_args[parameters[-1]] = items[-1]
        parameters = parameters[:-1]
        items = items[:-1]

    if parameters:
        assert len(parameters) == 1
        assert isinstance(parameters[0], TypeVarTuple)
        _typevar_to_args[parameters[0]] = items
        items = ()

    if items:
        raise TypeError(f"Too many type parameters given for class {cls!r}")

    return type(
        f"{cls.__name__}[{', '.join(getattr(i, '__name__', repr(i)) for i in all_items)}]",

        # XXX: here instead of inheriting from 'cls', inherit from
        #      `substitute_type(cls.__bases__)`. Also, for all the new methods
        #      defined on `cls`, wrap them, substituting types.
        #      Doing that, should make the class non-generic at initialization
        #      time.
        (cls,),
        {
            "_args": all_items,
            "_typevar_to_args": _typevar_to_args,
            "_generic_cls": cls,
            "__module__": cls.__module__,
        },
    )


def substitute_type(type_definition: TypeForm[Any], cls: type) -> TypeForm[Any]:
    """
    Resolve type annotation of a method/attribute of a generic class.
    """
    if not hasattr(cls, "_typevar_to_args"):
        return type_definition

    typevars = []
    typevar_values = []
    for t, v in cls._typevar_to_args.items():
        typevars.append(t)
        typevar_values.append(v)

    return _substitute_type(type_definition, typevars, typevar_values)


def _substitute_type(
    type_definition: TypeForm[Any],
    type_params: Sequence[TypeVar],
    args: Sequence[type],
) -> TypeForm[Any]:
    if len(type_params) != len(args):
        raise RuntimeError("Type parameters not specified for behavior actor.")

    if isinstance(type_definition, TypeVar):
        # Lookup.
        for t, a in zip(type_params, args):
            if type_definition == t:
                return a
        raise RuntimeError("Type parameter not found.")

    if isinstance(type_definition, GenericAlias):
        cls = get_origin(type_definition)
        return cls[
            *[_substitute_type(a, type_params, args) for a in type_definition.__args__]
        ]

    # If this is generic class.
    if hasattr(type_definition, "_typevar_to_args"):
        return type_definition._generic_cls[
            *[_substitute_type(arg, type_params, args) for arg in type_definition._args]
        ]

    return type_definition
