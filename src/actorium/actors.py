from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import cache
from typing import TYPE_CHECKING, Literal, Protocol, assert_type
from uuid import uuid4

from anyio import create_memory_object_stream, create_task_group
from pydantic import BaseModel, TypeAdapter

from .addresses import ActorId, Address
from .system import current_actor_system

__all__ = [
    "Actor",
    "spawn",
    "ActorRef",
]


class Actor[T](ABC):
    def message_type(self) -> type[T]:
        return self.receive.__annotations__["msg"]  # type:ignore

    @abstractmethod
    async def receive(self, msg: T, /) -> None: ...

    @cache
    def __type_adapter(self) -> TypeAdapter[T]:
        type_adapter: TypeAdapter[T] = TypeAdapter(self.message_type())
        return type_adapter

    async def receive_serialized(self, serialized_msg: str, /) -> None:
        msg: T = self.__type_adapter().validate_json(serialized_msg)
        await self.receive(msg)


class ActorFactory[A, T, **P](Protocol):
    """
    Actor protocol: a class *definition*, not instance, which:

    - can be initialized through the given paramspec (specified by `__call__`
      here).
    - has a `receive` method which accepts actor messages. Note that on the
      class definition, `receive` is unbound, so it takes the actor `A` as
      first argument.
    """

    def __call__(self, *args: P.args, **kwars: P.kwargs) -> A: ...

    def message_type(self, state: A) -> type[T]: ...

    async def receive(self, state: A, msg: T, /) -> None: ...
    async def receive_serialized(self, state: A, msg: str, /) -> None: ...


@asynccontextmanager
async def spawn[A, T, **P](
    factory: ActorFactory[A, T, P], /, *args: P.args, **kwargs: P.kwargs
) -> AsyncGenerator[tuple[A, ActorRef[T]]]:
    """
    Context manager for spawning a new actor.

    The first argument `factory` is the actor class to be instantiated, the
    optional arguments and keyword arguments that follow are passed to the
    factory to instantiate the actor.

    Example usage::

        class Collector(Actor[int]):
            " Actor class. "
            def __init__(self, param: str)-> None: ...
            async def receive(self, msg: int) -> None: ...

        async with spawn(Collector, param="some-param") as (actor, ref): ...
    """
    system = current_actor_system()
    actor = factory(*args, **kwargs)
    actor_id = uuid4()
    addresses = system.addresses()

    message_type: type[T] = factory.message_type(actor)

    sender, receiver = create_memory_object_stream[str](max_buffer_size=math.inf)

    async def forward_messages() -> None:
        """
        Received actor message from over the network. Deserialize and feed
        into this actor.
        """
        with receiver:
            async for message in receiver:
                try:
                    await factory.receive_serialized(actor, message)

                except Exception as e:
                    breakpoint()
                    print("got exception during message handling", e)  # TODO!

    async with (
        create_task_group() as tg,
        sender,
        current_actor_system().listen(actor_id=actor_id, callback=sender.send_nowait),
    ):
        tg.start_soon(forward_messages)

        if TYPE_CHECKING:
            ref = ActorRef[T](addresses=addresses, actor_id=actor_id)
        else:
            ref = ActorRef[message_type](addresses=addresses, actor_id=actor_id)

        yield actor, ref

        tg.cancel_scope.cancel()


async def __test_type_inference() -> None:
    """
    Quick self-test for the type checker to ensure that the above type
    inference works as expected.
    """

    class Collector(Actor[int]):
        async def receive(self, msg: int) -> None: ...

    async with spawn(Collector) as (a, b):
        assert_type(a, Collector)
        assert_type(b, ActorRef[int])

    class CollectorWithArgs(Actor[int]):
        def __init__(self, a: int, b: str) -> None: ...
        async def receive(self, msg: int) -> None: ...

    async with spawn(CollectorWithArgs, 1, b="text"):
        pass


class _Wrapper[T, R](Protocol):
    def __call__(self, *, addresses: tuple[Address, ...], actor_id: ActorId) -> R:
        "Constructor."

    def tell(self, instance: ActorRef[T], message: T) -> None: ...


class ActorRef[T](BaseModel):
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    # Discriminator, for when it's used in a union with other types.
    type_: Literal["actor-ref"] = "actor-ref"

    # List of addresses where this actor *might* be available. The easiest path
    # is preferred if possible, but (especially) in case of a named actor, the
    # actor might not be available there, and we can try the others.
    # Tuple, because it needs to be hashable. (e.g., in the set of
    # subscriptions in a 'computed').
    addresses: tuple[Address, ...]

    actor_id: ActorId

    model_config = {"frozen": True}

    def model_post_init(self, __context: object) -> None:
        TypeAdapter(self.message_type())

    @classmethod
    def message_type(cls) -> type[T]:
        try:
            return cls.__pydantic_generic_metadata__["args"][0]  # type:ignore
        except IndexError:
            return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type:ignore

    def wrap[R](self, type_: _Wrapper[T, R]) -> R:
        return type_(
            addresses=self.addresses,
            actor_id=self.actor_id,
        )

    def tell(self, message: T) -> None:
        """
        Send message to the underlying actor.
        """
        type_adapter: TypeAdapter[T] = TypeAdapter(self.message_type())

        serialized_message = type_adapter.dump_json(message).decode()

        current_actor_system().send_to_actor(
            self.addresses, self.actor_id, serialized_message
        )
