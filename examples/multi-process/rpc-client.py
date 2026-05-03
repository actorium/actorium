#!/usr/bin/env python

from anyio import sleep

from actorium import (
    Actor,
    BehaviorActor,
    BehaviorRef,
    Mailbox,
    Timeout,
    behavior,
    get_system,
    run,
    spawn,
)
from actorium.transports import TcpClient


class Calculator(BehaviorActor):
    @behavior
    async def double_it(self, number: int) -> int:
        raise NotImplementedError


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        system = get_system()

        async with spawn(TcpClient, "localhost", 9000):
            for i in range(100):
                calc = await system.lookup("calc", BehaviorRef[Calculator], timeout=1)
                if isinstance(calc, Timeout):
                    print("Timeout while trying to resolve RPC endpoint.")
                else:
                    result = await calc.be.double_it(10, timeout=1)
                    if isinstance(result, Timeout):
                        print("Timeout while calling RPC endpoint.")
                    else:
                        print("got result", result)
                await sleep(0.3)


if __name__ == "__main__":
    run(Main)
