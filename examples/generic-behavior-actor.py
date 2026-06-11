#!/usr/bin/env python

"""
More complex example of a generic `BehaviorActor`.
"""

from typing import Never, Protocol, Self

from actorium import BehaviorActor, SimpleActor, rpc, run, spawn
from actorium.utils import generic_class_getitem


class SupportsMultiply(Protocol):
    def __mul__(self, count: int) -> Self: ...


class Calc[T: SupportsMultiply](BehaviorActor):
    __class_getitem__ = generic_class_getitem

    @rpc
    async def double_it(self, value: T) -> T:
        return value * 2


class Main(SimpleActor[Never]):
    async def actor_run(self) -> None:
        int_ref = spawn(Calc[int])
        str_ref = spawn(Calc[str])

        result = await int_ref.double_it(4)
        print("Double of 4 is", result)

        result_2 = await str_ref.double_it("hello")
        print("hello*2 is", result_2)

        # Type checking and runtime validation should fail when passing '4'.
        # result_3 = await str_ref.double_it("4")

        # The following should fail at runtime, because `Calc` was spawned
        # without type parameters.
        # any_ref = spawn(Calc)
        # result_4 = await any_ref.double_it(5)


if __name__ == "__main__":
    run(Main)
