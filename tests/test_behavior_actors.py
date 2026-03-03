from actorium import Actor, Mailbox, run, spawn
from actorium.core import BehaviorActor, behavior


def test_behavior_actors() -> None:
    class Calc(BehaviorActor):
        @behavior
        async def double_it(self, value: int) -> int:
            return value * 2

        @behavior
        async def plus_one(self, value: int) -> int:
            return value + 1

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            async with spawn(Calc) as ref:
                result = await ref.be.double_it(4)
                assert result == 8

                result = await ref.be.plus_one(4)
                assert result == 5

    run(Main)
