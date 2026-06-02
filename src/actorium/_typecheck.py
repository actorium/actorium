from typing import assert_type

from .actors import SimpleActor, BehaviorActor, Mailbox, SimpleRef, behavior, rpc
from .system import spawn
from .types import ActorAddress

__all__ = []


async def __test_pydantic_actor_type_inference() -> None:
    """
    Quick self-test for the type checker to ensure that the type inference
    works as expected.
    """

    class Collector(SimpleActor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None: ...

    ref = spawn(Collector)
    assert_type(ref, SimpleRef[int])

    class CollectorWithArgs(SimpleActor[int]):
        def __init__(self, a: int, b: str) -> None: ...
        async def run(self, mailbox: Mailbox[int]) -> None: ...

    spawn(CollectorWithArgs, 1, "text")

    class CustomRef(SimpleRef[int]):
        pass

    class CollectorWithCustomRef(SimpleActor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None: ...
        def actor_ref(self, actor_address: ActorAddress) -> CustomRef:
            return CustomRef(actor_address=actor_address)

    ref2 = spawn(CollectorWithCustomRef)
    assert_type(ref2, CustomRef)


async def __test_behavior_actor_type_inference() -> None:
    class Calc(BehaviorActor):
        @behavior
        def say_hello(self, name: str) -> None:
            print(f"hello, {name}")

        @rpc
        async def double_it(self, value: int) -> int:
            return value * 2

        @rpc
        async def is_even(self, value: int) -> bool:
            return value % 2 == 0

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            ref = spawn(Calc)

            ref.be.say_hello("Jonathan")

            result = await ref.rpc.double_it(4)
            assert_type(result, int)

            result = await ref.rpc.is_even(4)
            assert_type(result, bool)
