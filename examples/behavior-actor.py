#!/usr/bin/env python

from typing import Never

from actorium import BehaviorActor, SimpleActor, rpc, run, spawn


class Calc(BehaviorActor):
    @rpc
    async def double_it(self, value: int) -> int:
        return value * 2

    @rpc
    async def plus_one(self, value: int) -> int:
        return value + 1


class Main(SimpleActor[Never]):
    async def actor_run(self) -> None:
        calc_ref = spawn(Calc)

        result = await calc_ref.double_it(4)
        print("Double of 4 is", result)


if __name__ == "__main__":
    run(Main)
