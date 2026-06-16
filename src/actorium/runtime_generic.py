from collections.abc import Callable
from functools import wraps
from types import GenericAlias
from typing import Any, TypeVar, TypeVarTuple, get_origin

from typing_extensions import TypeForm

__all__ = [
    "runtime_generic",
]


def runtime_generic[T: type[Any]](type_: T) -> T:
    """
    Decorator for better runtime support for generics:

    When the generic class is indexed with [type], this substitutes type
    parameters across all methods and attributes of the decorated generic.
    """

    class NewClass(type_):
        @classmethod
        def __class_getitem__(cls, items: Any) -> T:
            if not isinstance(items, tuple):
                return cls[(items,)]

            # Map type parameters.
            parameters = type_.__parameters__
            typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm] = {}

            while parameters and isinstance(parameters[0], TypeVar):
                typevar_to_args[parameters[0]] = items[0]
                parameters = parameters[1:]
                items = items[1:]

            while parameters and isinstance(parameters[-1], TypeVar):
                typevar_to_args[parameters[-1]] = items[-1]
                parameters = parameters[:-1]
                items = items[:-1]

            if parameters:
                assert len(parameters) == 1
                assert isinstance(parameters[0], TypeVarTuple)
                typevar_to_args[parameters[0]] = items
                items = ()

            if items:
                raise TypeError(f"Too many type parameters given for class {cls!r}")

            return cls._generic_substitute_(typevar_to_args)

        @classmethod
        def _generic_substitute_(
            cls, typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm]
        ) -> T:
            print("_generic_substitute_", cls)
            parameters = type_.__parameters__

            # Compute new name.
            def type_name(t: type) -> str:
                if hasattr(t, "__name__"):
                    name = t.__name__
                else:
                    name = str(t)

                if hasattr(t, "__args__"):
                    index_repr = ", ".join(type_name(p) for p in t.__args__)
                    return f"{name}[{index_repr}]"

                return name

            index_repr = ", ".join(type_name(typevar_to_args[p]) for p in parameters)
            new_name = f"{type_.__name__}[{index_repr}]"

            # Wrap all methods of `type_`.
            def wrap_one_method(method: Callable[..., Any]) -> Callable[..., Any]:
                class new_method:
                    def __call__(self, *args, **kwargs):
                        return method(*args, **kwargs)

                    @property
                    def __annotations__(self):
                        print('__annotations__', method, method.__annotations__, typevar_to_args)
                        return {
                            name: _substitute_types(annotation, typevar_to_args)
                            for name, annotation in method.__annotations__.items()
                        }

                return new_method()

            wrapped_attributes = {}

            for name in dir(type_):
                print("name=", name)
                if name == "__annotate__":
                    continue
                attr = getattr(type_, name)
                if attr == getattr(object, name, None):
                    # Don't wrap methods inheriting from 'object'.
                    continue
                if callable(attr):
                    wrapped_attributes[name] = wrap_one_method(attr)

            return type(
                new_name,
                (type_,),
                {
                    **wrapped_attributes,
                    "_typevar_to_args": typevar_to_args,
                    "__module__": type_.__module__,
                },
            )

    NewClass.__name__ = type_.__name__
    return NewClass


def _substitute_types(
    type_definition: TypeForm[Any],
    typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm],
) -> TypeForm[Any]:
    if isinstance(type_definition, TypeVar):
        # Lookup.
        try:
            return typevar_to_args[type_definition]
        except KeyError:
            raise RuntimeError("Type parameter not found.")

    if isinstance(type_definition, GenericAlias):
        cls = get_origin(type_definition)
        return cls[
            *[_substitute_types(a, typevar_to_args) for a in type_definition.__args__]
        ]

    # If this is generic class.
    if hasattr(type_definition, "_generic_substitute_"):
        return type_definition._generic_substitute_(typevar_to_args)
    #        return type_definition._generic_cls[
    #            *[
    #                _substitute_types(arg, type_params, args)
    #                for arg in type_definition._args
    #            ]
    #        ]

    return type_definition
