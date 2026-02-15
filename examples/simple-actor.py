#!/usr/bin/env python
from anyio import run, sleep

from actorium import ActorSystem, spawn


async def my_actor(msg: str) -> None:
    "Simple actor that prints whatever it receives."
    print(f"Received {msg} in actor.")


async def example() -> None:
    async with ActorSystem.create():
        # Spawn actor and obtain a reference to it.
        async with spawn(my_actor) as actor_ref:
            # Send messages to this actor through the reference.
            await actor_ref.send("Hello")
            await actor_ref.send("World")

            # This simple actor doesn't acknowledge delivery, allow some time
            # for the messages to be processed.
            await sleep(1)


if __name__ == "__main__":
    run(example)
