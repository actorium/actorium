from types import GenericAlias, new_class
from typing import Any, TypeVar, TypeVarTuple, get_origin

from msgspec import Struct
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
    original_class_getitem = type_.__class_getitem__

    @classmethod
    def __class_getitem__(cls, items: Any) -> T:
        parameters = cls.__parameters__

        if not isinstance(items, tuple):
            return cls[(items,)]

        # Map type parameters.
        typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm] = {}
        params = parameters[:]

        while params and isinstance(params[0], TypeVar):
            typevar_to_args[params[0]] = items[0]
            params = params[1:]
            items = items[1:]

        while params and isinstance(params[-1], TypeVar):
            typevar_to_args[params[-1]] = items[-1]
            params = params[:-1]
            items = items[:-1]

        if params:
            assert len(params) == 1
            assert isinstance(params[0], TypeVarTuple)
            typevar_to_args[params[0]] = items
            items = ()

        if items:
            raise TypeError(f"Too many type parameters given for class {cls!r}")

        return cls._generic_substitute_(typevar_to_args)

    @classmethod
    def _generic_substitute_(
        cls, typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm]
    ) -> T:
        parameters = cls.__parameters__

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

        type_params = [typevar_to_args[p] for p in parameters]
        index_repr = ", ".join(type_name(typevar_to_args[p]) for p in parameters)
        new_name = f"{cls.__name__}[{index_repr}]"

        typevar_to_args_from_parent = {}

        for base in cls.__orig_bases__:
            try:
                tv_to_args = getattr(base, "_typevar_to_args")
            except AttributeError:
                continue
            else:
                typevar_to_args_from_parent.update(
                    {
                        t: _substitute_types(value, typevar_to_args)
                        for t, value in tv_to_args.items()
                    }
                )

        if issubclass(cls, Struct):
            base = original_class_getitem(tuple(type_params))
        else:
            base = cls

        return new_class(
            new_name,
            (base,),
            {},
            lambda ns: ns.update(
                {
                    # **wrapped_attributes,
                    # "__type_params__": cls.__type_params__,
                    # "__parameters__": cls.__parameters__,
                    "_typevar_to_args": {
                        **typevar_to_args_from_parent,
                        **typevar_to_args,
                    },
                    "__module__": cls.__module__,
                }
            ),
        )

    type_.__class_getitem__ = __class_getitem__
    type_._generic_substitute_ = _generic_substitute_
    return type_


def _substitute_types(
    type_definition: TypeForm[Any],
    typevar_to_args: dict[TypeVar | TypeVarTuple, TypeForm],
) -> TypeForm[Any]:
    if isinstance(type_definition, TypeVar):
        # Lookup.
        try:
            return typevar_to_args[type_definition]
        except KeyError:
            # Can't resolve yet.type_
            return type_definition
            # raise RuntimeError(f"Type parameter not found: {type_definition}")

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
