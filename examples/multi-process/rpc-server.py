#!/usr/bin/env python
from anyio import sleep_forever

from actorium import Actor, Mailbox, get_system, run, spawn
from actorium.actors import rpc
from actorium.transports import TcpServer


async def double_it(number: int) -> int:
    print("Got double-it RPC request!")
    return number * 2


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        system = get_system()

        async with spawn(TcpServer, "localhost", 9000):
            async with (
                rpc[int, int](double_it) as double_ref,
                system.register(double_ref, "double"),
            ):
                # Register RPC call and obtain an actor reference.
                await sleep_forever()


if __name__ == "__main__":
    run(Main)
