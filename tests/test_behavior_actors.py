from actorium import Actor, Mailbox, run, spawn
from actorium.core import BehaviorActor, behavior


def test_behavior_actors() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @behavior
        async def double_it(self, value: int) -> int:
            return value * 2

        @behavior
        async def plus_one(self, value: int) -> int:
            return value + 1

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            nonlocal did_run

            ref = spawn(Calc)
            result = await ref.be.double_it(4)
            assert result == 8

            result = await ref.be.plus_one(4)
            assert result == 5
            did_run = True

    run(Main)

    # Ensure that we did wait until the main actor terminated.
    assert did_run


def test_behavior_multiple_arguments() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @behavior
        async def sum(self, a: int, b: int) -> int:
            return a + b

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            nonlocal did_run

            ref = spawn(Calc)
            result = await ref.be.sum(1, 2)
            assert result == 3

            did_run = True

    run(Main)
    assert did_run
