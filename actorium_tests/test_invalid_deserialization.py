from anyio import sleep

from actorium import create_actor_system_and_run, spawn
from actorium.actors import Signal


def test_invalid_message() -> None:
    async def main() -> None:
        number, set_number = spawn(Signal[int], 10)

        set_number("abc")  # String is an invalid type.
        set_number("20")  # We should not convert types (from str to int).
        set_number([20])  # List of integers, also ignored.

        # We should not crash, but nothing should have happened. An error will
        # be logged.
        await sleep(0.1)  # Allow mailboxes to be processed.
        assert await number.get() == 10

    create_actor_system_and_run(main)
