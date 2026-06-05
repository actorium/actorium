#!/usr/bin/env python

from typing import Never

from anyio import sleep

from actorium import SimpleActor, run, spawn


class MyActor(SimpleActor[str]):
    async def actor_run(self) -> None:
        "Simple actor that prints whatever it receives."
        async for (msg,) in self.mailbox:
            print(f"Received {msg} in actor.")


class Main(SimpleActor[Never]):
    async def actor_run(self) -> None:
        actor_ref = spawn(MyActor)

        # Send messages to this actor through the reference.
        actor_ref.tell("Hello")
        actor_ref.tell("World")

        # This simple actor doesn't acknowledge delivery, allow some time
        # for the messages to be processed.
        await sleep(1)


if __name__ == "__main__":
    run(Main)
