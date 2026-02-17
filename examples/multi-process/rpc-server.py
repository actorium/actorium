#!/usr/bin/env python
from anyio import run, sleep_forever

from actorium import actor_system, register
from actorium.reactivity import rpc
from actorium.transports import TcpListener


async def double_it(number: int) -> int:
    print("Got double-it RPC request!")
    return number * 2


async def example() -> None:
    async with (
        TcpListener.create(host="localhost", port=9000) as tcp_listener,
        actor_system(listeners=[tcp_listener]),
        rpc(int, int, double_it) as double_ref,
        register(double_ref, "double"),
    ):
        # Register RPC call and obtain an actor reference.
        await sleep_forever()


if __name__ == "__main__":
    run(example)
