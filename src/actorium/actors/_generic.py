from functools import cache, wraps
from types import FunctionType
from typing import Any, Callable, Protocol, TypeVar

__all__ = [
    "GenericFunction",
    "generic_function",
]


class GenericFunction[**P, R](Protocol):
    def __call__(self, *a: P.args, **kw: P.kwargs) -> R: ...
    def __getitem__(self, params: Any) -> Callable[P, R]: ...


def generic_function[**P, R](func: Callable[P, R]) -> GenericFunction[P, R]:
    """
    Function decorator that allows calling a generic function with type
    parameters, and expose the actual types within the function.

    See: https://github.com/python/typing/discussions/2199
    """

    class wrapper:
        @cache
        def __getitem__(
            self, type_params: TypeVar | tuple[TypeVar, ...]
        ) -> Callable[P, R]:
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

            return replace_func(func)  # type:ignore[arg-type]

        # `__call__` staticmethod to make `inspect.signature` work on the
        # `GenericFunction`.
        @staticmethod
        @wraps(func)
        def __call__(*a: P.args, **kw: P.kwargs) -> R:
            return func(*a, **kw)

    wrapper.__doc__ = func.__doc__
    return wrapper()
