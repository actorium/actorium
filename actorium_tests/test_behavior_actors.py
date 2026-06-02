from actorium import BehaviorActor, Mailbox, SimpleActor, rpc, run, spawn


def test_behavior_actors() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @rpc
        async def double_it(self, value: int) -> int:
            return value * 2

        @rpc
        async def plus_one(self, value: int) -> int:
            return value + 1

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            nonlocal did_run

            ref = spawn(Calc)
            result = await ref.rpc.double_it(4)
            assert result == 8

            result = await ref.rpc.plus_one(4)
            assert result == 5
            did_run = True

    run(Main)

    # Ensure that we did wait until the main actor terminated.
    assert did_run


def test_behavior_multiple_arguments() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @rpc
        async def sum(self, a: int, b: int) -> int:
            return a + b

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            nonlocal did_run

            ref = spawn(Calc)
            result = await ref.rpc.sum(1, 2)
            assert result == 3

            did_run = True

    run(Main)
    assert did_run
