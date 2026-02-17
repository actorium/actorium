#!/usr/bin/env python
from anyio import run

from actorium import actor_system, tcp_listener
from actorium.reactivity import rpc


async def double_it(number: int) -> int:
    return number * 2


async def example() -> None:
    async with (
        actor_system(),
        tcp_listener(host="localhost", port=9000),
    ):
        # Register RPC call and obtain an actor reference.
        async with rpc(int, int, double_it) as double_ref:
            result = await double_ref.ask(10)
            print("result:", result)


if __name__ == "__main__":
    run(example)
