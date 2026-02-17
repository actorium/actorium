#!/usr/bin/env python
from anyio import run

from actorium import TcpAddress, actor_system, name_resolver
from actorium.reactivity.rpc import RpcRef
from actorium.transports import TcpListener


async def example() -> None:
    async with (
        TcpListener.create(host="localhost", port=9001) as tcp_listener,
        actor_system(listeners=[tcp_listener]),
    ):
        resolver = name_resolver(
            peer_addresses=[TcpAddress(host="localhost", port=9000)],
        )
        double_ref = await resolver.resolve("double", RpcRef[int, int])

        result = await double_ref.ask(10)
        print("got result", result)


if __name__ == "__main__":
    run(example)
