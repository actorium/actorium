"""
Implementation of cross-process reactivity.
"""

import json
import threading
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Self
from uuid import UUID, uuid4

import redis.asyncio as redis
from anyio import create_task_group, move_on_after
from anyio.abc import TaskGroup
from pydantic import TypeAdapter

if TYPE_CHECKING:
    from .actors import ActorRef

__all__ = [
    "get_actor_system",
    "ActorSystem",
    "NoActorSystemConfiguredError",
]


class _CurrentSystem(threading.local):
    def __init__(self) -> None:
        super().__init__()
        self.actor_system: ActorSystem | None = None


_CURRENT_SYSTEM = _CurrentSystem()


class NoActorSystemConfiguredError(Exception):
    pass


def get_actor_system() -> "ActorSystem":
    system = _CURRENT_SYSTEM.actor_system
    if system is None:
        raise NoActorSystemConfiguredError()
    return system


class ActorSystem:
    """
    Actor system: keep track of currently running actors.
    """

    def __init__(self, redis_client: redis.Redis, tg: TaskGroup) -> None:
        self.redis_client = redis_client
        self.tg = tg
        # Actor UUID to actor receive func.
        self._registered_actors: dict[
            UUID, Callable[[Any], Coroutine[Any, Any, None]]
        ] = {}

        self._actor_system_uuid = uuid4()

    @classmethod
    @asynccontextmanager
    async def create(cls) -> AsyncGenerator[Self]:
        pool = redis.ConnectionPool.from_url("redis://localhost")
        client = redis.Redis.from_pool(pool)
        try:
            async with create_task_group() as tg:
                instance = cls(client, tg)
                tg.start_soon(instance._subscribe)
                _CURRENT_SYSTEM.actor_system = instance
                try:
                    yield instance
                finally:
                    _CURRENT_SYSTEM.actor_system = None

                tg.cancel_scope.cancel()
        finally:
            with move_on_after(1.0, shield=True):
                await client.aclose()

    async def _subscribe(self) -> None:
        while True:
            # Wait until there is any message available for our process.
            # TODO: probably we can pop *all* to reduce the number of Redis roundtrips.
            _, actor_queue_name_bytes = await self.redis_client.blpop(
                [f"actors-ready:{self._actor_system_uuid}"]
            )
            actor_queue_name = actor_queue_name_bytes.decode()

            # Now, look at the specific actor.
            _, actor_message = await self.redis_client.blpop(
                [f"actor-queue:{actor_queue_name}"]
            )

            func = self._registered_actors.get(UUID(actor_queue_name))
            if func is None:
                print("Received message for unknown actor.")
                # Dead letter queue!
            else:
                await func(actor_message.decode())

    @asynccontextmanager
    async def _register_using_name(
        self, actor: ActorRef[Any], name: str
    ) -> AsyncGenerator[None]:
        from .actors import ActorRef

        await self.redis_client.set(
            f"named-actor:{name}", TypeAdapter(ActorRef[Any]).dump_json(actor)
        )
        try:
            yield
        finally:
            with move_on_after(1.0, shield=True):
                await self.redis_client.delete(f"named-actor:{name}")

    async def _get_actor_data_using_name[T](self, name: str) -> dict[str, Any] | None:
        result = await self.redis_client.get(f"named-actor:{name}")
        if result:
            return json.loads(result.decode())
        return None
