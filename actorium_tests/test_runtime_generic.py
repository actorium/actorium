from actorium.runtime_generic import runtime_generic


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


def test_substitute_generic_method_input() -> None:
    @runtime_generic
    class G[T]:
        def f(self, t: T) -> None:
            pass

    assert G[int]().f.__annotations__ == {
        "t": int,
        "return": None,
    }


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
