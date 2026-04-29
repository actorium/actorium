#!/usr/bin/env python
from anyio import sleep_forever

from actorium import Actor, BehaviorActor, Mailbox, behavior, get_system, run, spawn
from actorium.transports import TcpServer


class Calculator(BehaviorActor):
    @behavior
    async def double_it(self, number: int) -> int:
        print("Got double-it RPC request!")
        return number * 2


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        async with (
            spawn(TcpServer, "localhost", 9000),
            spawn(Calculator) as calculator_ref,
            get_system().register(calculator_ref, "calc"),
        ):
            # Register RPC call and obtain an actor reference.
            await sleep_forever()


if __name__ == "__main__":
    run(Main)
