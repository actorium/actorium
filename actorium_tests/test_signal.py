import pytest

from actorium import BehaviorRef, create_actor_system_and_run, spawn
from actorium.actors import CallAfterTimeout, Signal, Undefined


def test_signal_set_get() -> None:
    async def main() -> None:
        number, set_number = spawn(Signal[int], 10)

        assert await number.get() == 10
        set_number(20)
        assert await number.get() == 20

    create_actor_system_and_run(main)


def test_signal_unset_with_timeout() -> None:
    """
    Calling '.get()' on a signal that doesn't have a value set should timeout.
    """

    async def main() -> None:
        number, set_number = spawn(Signal[int], Undefined)

        with pytest.raises(TimeoutError):
            await number.get(timeout=0.01)

    create_actor_system_and_run(main)


def test_signal_unblock_get_with_set() -> None:
    """
    Calling '.get()' when the value is not yet set, but will be set soon.
    The `get()` should unblock right after the `set` completes.
    """

    async def main() -> None:
        number, set_number = spawn(Signal[int], Undefined)

        spawn(CallAfterTimeout, 0.01, lambda: set_number(10))

        result = await number.get()
        assert result == 10

    create_actor_system_and_run(main)


def test_types() -> None:
    assert BehaviorRef[Signal[int]] == BehaviorRef[Signal[int]]
