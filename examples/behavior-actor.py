#!/usr/bin/env python

from actorium import Actor, BehaviorActor, Mailbox, behavior, run, spawn


class Calc(BehaviorActor):
    @behavior
    async def double_it(self, value: int) -> int:
        return value * 2

    @behavior
    async def plus_one(self, value: int) -> int:
        return value + 1


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        calc_ref = spawn(Calc)

        result = await calc_ref.be.double_it(4)
        print("Double of 4 is", result)


if __name__ == "__main__":
    run(Main)
