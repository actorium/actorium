"""
Implementation of cross-process reactivity.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Coroutine
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, Protocol, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, TypeAdapter

from .system import ActorSystem, get_actor_system

__all__ = [
    "ActorRegistration",
    "Actor",
    "spawn",
    "ActorRef",
]


@dataclass
class ActorRegistration[T]:
    """
    Registration point.
    """

    name: str


class Actor[T](Protocol):
    """
    Actor protocol: anything can can receive messages of type `T`.
    """

    def __call__(self, msg: T, /) -> Coroutine[Any, Any, None] | None: ...


def spawn[T](
    implementation: Actor[T],
    /,
    system: ActorSystem | None = None,
    register: ActorRegistration[T] | None = None,
) -> AbstractAsyncContextManager[ActorRef[T]]:
    """
    Context manager for spawning a new actor.
    """
    return ActorRef[T].spawn(implementation, system=system, register=register)


class ActorRef[T](BaseModel):
    """
    Reference/handle to an actor that has been spawned somewhere, possibly in
    another process.

    This handle is a serializable `BaseModel` itself so that we can send it as
    part of a message to any other actor.
    """

    # Discriminator, for when it's used in a union with other types.
    type: Literal["actor-ref"] = "actor-ref"

    actor_system_uuid: UUID
    actor_uuid: UUID

    model_config = {"frozen": True}

    async def send(self, message: T) -> None:
        """
        Send message to the underlying actor.
        """
        message_type = self.__pydantic_generic_metadata__["args"][0]
        type_adapter = TypeAdapter(message_type)

        serialized_message = type_adapter.dump_json(message)

        system = get_actor_system()

        # TODO: If this actor is local, then don't serialized/deserialize, but
        #       send straight to the actual actor.

        await system.redis_client.rpush(
            f"actor-queue:{self.actor_uuid}", serialized_message
        )
        await system.redis_client.rpush(
            f"actors-ready:{self.actor_system_uuid}", str(self.actor_uuid)
        )

    @classmethod
    @asynccontextmanager
    async def spawn(
        cls,
        implementation: Actor[T],
        /,
        system: ActorSystem | None = None,
        register: ActorRegistration[T] | None = None,
    ) -> AsyncGenerator[Self]:
        """
        Constructor: start a new actor and yield a referenec to that actor.
        """
        if system is None:
            system = get_actor_system()

        receive_annotations = implementation.__annotations__
        message_type = receive_annotations["msg"]
        type_adapter = TypeAdapter(message_type)

        actor_uuid = uuid4()

        async def send_serialized(message: str) -> None:
            """
            Received actor message from over the network. Deserialize and feed
            into this actor.
            """
            decoded_message: T = type_adapter.validate_json(message)
            result = implementation(decoded_message)
            if isinstance(result, Coroutine):
                await result

        system._registered_actors[actor_uuid] = send_serialized

        ref = cls(
            actor_system_uuid=system._actor_system_uuid,
            actor_uuid=actor_uuid,
        )

        try:
            if register is None:
                yield ref
            else:
                async with system._register_using_name(ref, register.name):
                    yield ref
        finally:
            del system._registered_actors[actor_uuid]

    @classmethod
    async def from_registration(
        cls, registration: ActorRegistration[T], system: ActorSystem | None = None
    ) -> ActorRef[T] | None:
        """
        Get reference to an actor that was registered under the given
        registration.
        """
        if system is None:
            system = get_actor_system()

        actor_data = await system._get_actor_data_using_name(registration.name)

        if actor_data is None:
            return None

        return cls.model_validate(actor_data)
