from actorium.utils import generic_class_getitem


def test_generic_class_one_typevar() -> None:
    class G[A]:
        __class_getitem__ = generic_class_getitem

    assert G[int]._args == (int,)
    assert G[int]._typevar_to_args == {
        G.__parameters__[0]: int,
    }

    assert G[(int,)]._args == (int,)
    assert G[(int,)]._typevar_to_args == {
        G.__parameters__[0]: int,
    }


def test_generic_class_multiple_typevars() -> None:
    class G[A, B, C]:
        __class_getitem__ = generic_class_getitem

    assert G[int, float, bool]._args == (int, float, bool)
    assert G[int, float, bool]._typevar_to_args == {
        G.__parameters__[0]: int,
        G.__parameters__[1]: float,
        G.__parameters__[2]: bool,
    }


def test_generic_class_end_variadic() -> None:
    class G[A, *B]:
        __class_getitem__ = generic_class_getitem

    assert G[int, float, bool]._args == (int, float, bool)
    assert G[int, float, bool]._typevar_to_args == {
        G.__parameters__[0]: int,
        G.__parameters__[1]: (float, bool),
    }


def test_generic_class_start_variadic() -> None:
    class G[*A, B]:
        __class_getitem__ = generic_class_getitem

    assert G[int, float, bool]._args == (int, float, bool)
    assert G[int, float, bool]._typevar_to_args == {
        G.__parameters__[0]: (int, float),
        G.__parameters__[1]: bool,
    }
