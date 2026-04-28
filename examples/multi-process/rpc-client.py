#!/usr/bin/env python

from anyio import sleep

from actorium import Actor, Mailbox, Timeout, get_system, run, spawn
from actorium.actors import RpcRef
from actorium.transports import TcpClient


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        system = get_system()

        async with spawn(TcpClient, "localhost", 9000):
            for i in range(100):
                double_ref = await system.lookup("double", RpcRef[int, int], timeout=1)
                if isinstance(double_ref, Timeout):
                    print("Timeout while trying to resolve RPC endpoint.")
                else:
                    result = await double_ref.ask(10, timeout=1)
                    if isinstance(result, Timeout):
                        print("Timeout while calling RPC endpoint.")
                    else:
                        print("got result", result)
                await sleep(0.3)


if __name__ == "__main__":
    run(Main)
