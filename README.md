# actorium

A modern implementation of Actors in Python for distributed concurrency.

Warning: this library is a work in progress. Not yet recommended for use in
production.

## Installation

```sh
uv pip install actorium
```

## Key features

- Pydantic based message serialization/deserialization.
- Build with support for static typing in mind (no `cast`, no `Any`)..
- Structured concurrency for managing actor lifetimes.
- Registration of actors under a given name.
- Actor implementations for reactivity.


## Example usage

```python
#!/usr/bin/env python
from anyio import run, sleep, Actor
from actorium import actor_system, spawn

class MyActor(Actor[str]):
    "Simple actor that prints whatever it receives."

    async def receive(msg: str) -> None:
        print(f"Received {msg} in actor.")

async def example() -> None:
    async with actor_system():
        # Spawn actor and obtain a reference to it.
        async with spawn(MyActor) as (my_actor, actor_ref):
            # Thanks to type inference, Mypy knows that:
            # - `my_actor` is of type `MyActor`.
            # - `actor_ref` is of type `ActorRef[str]`.
            # Further, messages are automatically serialized/deserialized using
            # a Pydantic `TypeAdapter[str]`.

            # Send messages to this actor through the reference.
            actor_ref.tell("Hello")
            actor_ref.tell("World")

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
