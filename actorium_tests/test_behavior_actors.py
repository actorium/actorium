from actorium import BehaviorActor, BehaviorRef, create_actor_system_and_run, rpc, spawn
from actorium.actors.behaviors import _RpcMethod


def test_behavior_actors() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @rpc
        async def double_it(self, value: int) -> int:
            return value * 2

        @rpc
        async def plus_one(self, value: int) -> int:
            return value + 1

    async def main() -> None:
        nonlocal did_run

        ref = spawn(Calc)
        result = await ref.double_it(4)
        assert result == 8

        result = await ref.plus_one(4)
        assert result == 5
        did_run = True

    create_actor_system_and_run(main)

    # Ensure that we did wait until the main actor terminated.
    assert did_run


def test_behavior_multiple_arguments() -> None:
    did_run = False

    class Calc(BehaviorActor):
        @rpc
        async def sum(self, a: int, b: int) -> int:
            return a + b

    async def main() -> None:
        nonlocal did_run

        ref = spawn(Calc)
        result = await ref.sum(1, 2)
        assert result == 3

        did_run = True

    create_actor_system_and_run(main)
    assert did_run


def test_generic_behavior_actor() -> None:
    did_run = False

    class GenericBehaviorActor[T](BehaviorActor):
        @rpc
        async def return_value(self, value: T) -> T:
            return value

    async def main() -> None:
        nonlocal did_run

        int_ref = spawn(GenericBehaviorActor[int])
        str_ref = spawn(GenericBehaviorActor[str])

        result = await int_ref.return_value(4)
        assert result == 4

        result2 = await str_ref.return_value("hello")
        assert result2 == "hello"

        did_run = True

    create_actor_system_and_run(main)
    assert did_run


def test_generic_behavior_actor_type_inference() -> None:
    class GenericBehaviorActor[T](BehaviorActor):
        @rpc
        async def return_value(self, value: list[T], value2: T) -> tuple[T, T]:
            return value[0], value[0]

    assert isinstance(GenericBehaviorActor[int].return_value, _RpcMethod)
    instance = GenericBehaviorActor[int]()
    assert instance.return_value.get_input_types(instance) == [list[int], int]
    assert instance.return_value.get_output_type(instance) == tuple[int, int]


def test_types() -> None:
    class Calc(BehaviorActor):
        pass

    assert BehaviorRef[Calc] == BehaviorRef[Calc]

    # Generic
    class GenericBehaviorActor[T](BehaviorActor):
        pass

    assert GenericBehaviorActor[int] == GenericBehaviorActor[int]
    assert "test_behavior_actors.GenericBehaviorActor[int]" in repr(
        GenericBehaviorActor[int]()
    )
