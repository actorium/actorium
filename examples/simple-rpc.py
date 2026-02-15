#!/usr/bin/env python
from anyio import run

from actorium import ActorSystem
from actorium.reactivity import rpc


async def double_it(number: int) -> int:
    return number * 2


async def example() -> None:
    async with ActorSystem.create():
        # Register RPC call and obtain an actor reference.
        async with rpc(double_it) as double_ref:
            result = await double_ref.call(10)
            print("result:", result)


if __name__ == "__main__":
    run(example)
