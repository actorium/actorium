from typing import assert_type

from .actors import BehaviorActor, BehaviorRef, SimpleActor, SimpleRef, behavior, rpc
from .system import spawn

__all__ = []


async def __test_pydantic_actor_type_inference() -> None:
    """
    Quick self-test for the type checker to ensure that the type inference
    works as expected.
    """

    class Collector(SimpleActor[int]):
        async def actor_run(self) -> None: ...

    ref = spawn(Collector)
    assert_type(ref, SimpleRef[int])

    class CollectorWithArgs(SimpleActor[int]):
        def __init__(self, a: int, b: str) -> None: ...
        async def actor_run(self) -> None: ...

    spawn(CollectorWithArgs, 1, "text")

    class CustomRef(SimpleRef[int]):
        pass

    class CollectorWithCustomRef(SimpleActor[int]):
        async def actor_run(self) -> None: ...
        def actor_ref(self) -> CustomRef:
            return CustomRef(actor_address=super().actor_ref().actor_address)

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
        async def actor_run(self) -> None:
            ref = spawn(Calc)
            assert_type(ref, BehaviorRef[Calc])

            ref.say_hello("Jonathan")

            result = await ref.double_it(4)
            assert_type(result, int)

            result = await ref.is_even(4)
            assert_type(result, bool)


async def __test_generic_behavior_actor_type_inference() -> None:
    class Calc[T: int | str](BehaviorActor):
        @behavior
        def say_hello(self, value: T) -> None:
            print(f"hello, {value}")

        @rpc
        async def return_value(self, value: T) -> T:
            return value

    class Main(SimpleActor[None]):
        async def actor_run(self) -> None:
            ref = spawn(Calc[int])
            assert_type(ref, BehaviorRef[Calc[int]])

            ref.say_hello(123)

            result = await ref.return_value(4)
            assert_type(result, int)
