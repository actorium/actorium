import pytest

from actorium import Mailbox, SimpleActor, run, spawn
from actorium.actors import CallAfterTimeout, Signal, Undefined


def test_signal_set_get() -> None:
    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = spawn(Signal[int], 10)

            assert await number.get() == 10
            number.set(20)
            assert await number.get() == 20

    run(Main)


def test_signal_unset_with_timeout() -> None:
    """
    Calling '.get()' on a signal that doesn't have a value set should timeout.
    """

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = spawn(Signal[int], Undefined)

            with pytest.raises(TimeoutError):
                await number.get(timeout=0.01)

    run(Main)


def test_signal_unblock_get_with_set() -> None:
    """
    Calling '.get()' when the value is not yet set, but will be set soon.
    The `get()` should unblock right after the `set` completes.
    """

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = spawn(Signal[int], Undefined)

            spawn(CallAfterTimeout, 0.01, lambda: number.set(10))

            result = await number.get()
            assert result == 10

    run(Main)
