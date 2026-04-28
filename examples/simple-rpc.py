#!/usr/bin/env python

from actorium import Actor, Mailbox, run
from actorium.actors import rpc


async def double_it(number: int) -> int:
    return number * 2


class Main(Actor[None]):
    async def run(self, mailbox: Mailbox[None]) -> None:
        # Register RPC call and obtain an actor reference.
        async with rpc[int, int](double_it) as double_ref:
            result = await double_ref.ask(10)
            print("result:", result)


if __name__ == "__main__":
    run(Main)
