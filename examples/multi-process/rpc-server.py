#!/usr/bin/env python
from typing import Never

from anyio import sleep_forever

from actorium import BehaviorActor, SimpleActor, rpc, run, spawn
from actorium.transports import TcpServer


class Calculator(BehaviorActor):
    @rpc
    async def double_it(self, number: int) -> int:
        print("Got double-it RPC request!")
        return number * 2


class Main(SimpleActor[Never]):
    async def actor_run(self) -> None:
        spawn(TcpServer, "localhost", 9000)
        spawn(Calculator, name="calc")

        # Register RPC call and obtain an actor reference.
        await sleep_forever()


if __name__ == "__main__":
    run(Main)
