import msgspec
import pytest
from msgspec import Struct

from actorium.runtime_generic import runtime_generic
from actorium.serialization import deserialize


def test_name() -> None:
    @runtime_generic
    class G[T]:
        pass

    assert G[int].__name__ == "G[int]"
    assert G[bool].__name__ == "G[bool]"

    class X:
        pass

    assert G[X].__name__ == "G[X]"
    assert G[list[X]].__name__ == "G[list[X]]"
    assert G[G[X]].__name__ == "G[G[X]]"
    assert G[G[list[X]]].__name__ == "G[G[list[X]]]"
    assert G[list[G[X]]].__name__ == "G[list[G[X]]]"


def test_typevar_to_args() -> None:
    @runtime_generic
    class G[T]:
        pass

    (T,) = G.__type_params__
    assert G[int]._typevar_to_args == {T: int}


def test_typevar_to_args_two() -> None:
    @runtime_generic
    class G[T, U]:
        pass

    T, U = G.__type_params__
    assert G[int, str]._typevar_to_args == {T: int, U: str}


def test_typevar_inheritance() -> None:
    @runtime_generic
    class G[T]:
        pass

    @runtime_generic
    class H[U](G[list[U]]):
        pass

    (T,) = G.__type_params__
    (U,) = H.__type_params__
    assert H[int]._typevar_to_args == {T: list[int], U: int}


def test_typevar_inheritance_3_layers() -> None:
    @runtime_generic
    class G[T]:
        pass

    @runtime_generic
    class H[U](G[list[U]]):
        pass

    @runtime_generic
    class I[V, W](H[tuple[V, W]]):
        pass

    (T,) = G.__type_params__
    (U,) = H.__type_params__
    (V, W) = I.__type_params__

    assert I[int, str]._typevar_to_args == {
        T: list[tuple[int, str]],
        U: tuple[int, str],
        V: int,
        W: str,
    }


def test_msgspec_struct() -> None:
    @runtime_generic
    class S[T](Struct):
        value: T

    (T,) = S.__type_params__
    assert S[int]._typevar_to_args == {T: int}

    # Ensure that deserialization works as expected. Even with overridden
    # __class_getitem__, the type should be passed on to `Struct`.
    s = deserialize('{"value": 123}', type=S)
    assert s.value == 123
    s = deserialize('{"value": "abc"}', type=S)
    assert s.value == "abc"
    s = deserialize('{"value": 123}', type=S[int])
    assert s.value == 123
    s = deserialize('{"value": "abc"}', type=S[str])
    assert s.value == "abc"

    with pytest.raises(msgspec.ValidationError):
        s = deserialize('{"value": 123}', type=S[str])
    with pytest.raises(msgspec.ValidationError):
        s = deserialize('{"value": "123"}', type=S[int])


def test_no_decorator_on_subclass() -> None:
    @runtime_generic
    class G[T]:
        pass

    # Intentionally, no `@runtime_generic` decorator here.
    # Everything should still work.
    class H[U](G[list[U]]):
        pass

    (T,) = G.__type_params__
    (U,) = H.__type_params__
    assert H[int]._typevar_to_args == {T: list[int], U: int}


"""
def test_substitute_generic_method_input() -> None:
    @runtime_generic
    class G[T]:
        def f(self, t: T) -> None:
            pass

    T = G.__type_params[0]
    assert G[int]._typevar_to_args == {T: int}
    # assert G[int]().f.__annotations__ == {
    #    "t": int,
    #    "return": None,
    # }


def test_substitute_generic_method_output() -> None:
    @runtime_generic
    class G[T]:
        def f(self) -> T:
            pass

    assert G[int]().f.__annotations__ == {
        "return": int,
    }


def test_substitute_generic_method_generic_input() -> None:
    @runtime_generic
    class G[T]:
        def f(self, t: list[T]) -> None:
            pass

    assert G[int]().f.__annotations__ == {
        "t": list[int],
        "return": None,
    }


def test_substitute_generic_method_generic_input_2() -> None:
    @runtime_generic
    class G[T]:
        def f(self, t: G[T]) -> None:
            pass

    assert G[int]().f.__annotations__ == {
        "t": G[int],
        "return": None,
    }
"""
