from actorium import create_actor_system_and_run, spawn
from actorium.actors import Signal, computed

from .utils import assert_soon_equal


def test_computed() -> None:
    async def main() -> None:
        # Create two reactive objects, number1 and number2
        number1, set_number1 = spawn(Signal[int], 0)
        number2, set_number2 = spawn(Signal[int], 0)

        # Check type parameters.
        # assert number1._t == int
        # assert number2._t == int
        # assert isinstance(number1, signal[int])
        # assert isinstance(number2, signal[int])

        # The computation
        @computed(number1, number2)
        def the_sum(value1: int, value2: int) -> int:
            return value1 + value2

        # Check type for computed and type parameter.
        # assert isinstance(the_sum, signal[int])
        # assert the_sum._t == int

        # Create a reactive computation.
        assert await the_sum.get() == 0

        # Change source objects.
        set_number1(10)
        set_number2(20)

        # Changes should propagate.
        await assert_soon_equal(the_sum.get, 30)

    create_actor_system_and_run(main)
