#!/usr/bin/env python

from typing import Never

from anyio import sleep

from actorium import BehaviorActor, BehaviorRef, SimpleActor, lookup, rpc, run, spawn
from actorium.transports import TcpClient


class Calculator(BehaviorActor):
    """
    Stub of the actual actor. Only needed to derive the available methods.

    Usually it's better to import the actual actor implementation, but if not
    available, a stub works as well.
    """

    @rpc
    async def double_it(self, number: int) -> int:
        raise NotImplementedError


class Main(SimpleActor[Never]):
    async def actor_run(self) -> None:
        spawn(TcpClient, "localhost", 9000)

        for i in range(100):
            try:
                calc = await lookup("calc", BehaviorRef[Calculator], timeout=1)
                result = await calc.double_it(10, timeout=1)

                print("got result", result)
                await sleep(0.3)
            except TimeoutError as e:
                print(f"Got timeout: {e}")


if __name__ == "__main__":
    run(Main)
