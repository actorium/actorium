#!/usr/bin/env python
from anyio import run, sleep

from actorium import Actor, actor_system, spawn


class MyActor(Actor[str]):
    async def receive(self, msg: str) -> None:
        "Simple actor that prints whatever it receives."
        print(f"Received {msg} in actor.")


async def example() -> None:
    async with actor_system():
        # Spawn actor and obtain a reference to it.
        async with spawn(MyActor) as (_, actor_ref):
            # Send messages to this actor through the reference.
            actor_ref.tell("Hello")
            actor_ref.tell("World")

            # This simple actor doesn't acknowledge delivery, allow some time
            # for the messages to be processed.
            await sleep(1)


if __name__ == "__main__":
    run(example)
