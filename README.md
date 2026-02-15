# actorium
A modern implementation of Actors in Python for distributed concurrency

Warning: this library is a work in progress. Not recommended for use in
production.

## Installation

```sh
uv pip install actorium
```

## Features

- Pydantic based message serialization/deserialization.
- Structured concurrency through anyio (for as much as structured concurrency
  is possible in an actor system).
- Registration of actors under a given name.


## Example usage

```python
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
```

## TODO:

- Multiple transports for IPC.
- Ability to spawn an actor straight into a new subprocess.
- Many helper functions on top.
- Lots of testing. See whether the architecture makes sense.
- Ensure we don't leak memory over time.
- Handle (log?) messages that can't be routed to an actor.
- Helpers for automatic respawning actors (in subprocesses) when they crash.
- Type checking, set up CI/CD pipeline.

## Philosophy

The architecture is still taking shape, given that the library is not yet
finished. But inspiration is taken from structured concurrency and actor
systems like we know. The idea is to achieve a framework that will allow for
multiple asyncio eventloops to communicate with each other through actors.
