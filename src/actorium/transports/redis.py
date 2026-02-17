from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Callable, Self

import redis.asyncio as redis
from anyio import create_task_group, move_on_after
from anyio.abc import TaskGroup

from ..addresses import ActorId, Address, RedisAddress
from .base import Listener, Outbox, SendResult

__all__ = [
    "RedisOutbox",
    "RedisListener",
]


class RedisOutbox(Outbox):
    def __init__(self, redis_client: redis.Redis, connection_string: str) -> None:
        self.redis_client = redis_client
        self.connection_string = connection_string

    @classmethod
    @asynccontextmanager
    async def create(
        # TODO: don't take a correction string in `create` here. Dynamically
        # create new connections according to what we get in
        # `find_actor_outbox`.
        cls,
        *,
        connection_string: str = "redis://localhost",
    ) -> AsyncGenerator[Self]:
        pool = redis.ConnectionPool.from_url(connection_string)
        client = redis.Redis.from_pool(pool)
        try:
            instance = cls(client, connection_string)
            yield instance

        finally:
            with move_on_after(1.0, shield=True):
                await client.aclose()

    async def send_to_actor(
        self, addresses: Sequence[Address], actor_id: ActorId, serialized_message: str
    ) -> SendResult:
        for address in addresses:
            if not isinstance(address, RedisAddress):
                continue

            await self.redis_client.rpush(f"actor-queue:{actor_id}", serialized_message)  # type:ignore[misc]
            await self.redis_client.rpush(  # type:ignore[misc]
                f"actors-ready:{address.node_id}", str(actor_id)
            )

            return SendResult.MESSAGE_SENT

        return SendResult.NO_ADDRESS_HANDLED_HERE


class RedisListener(Listener):
    def __init__(
        self, redis_client: redis.Redis, tg: TaskGroup, address: RedisAddress
    ) -> None:
        self.redis_client = redis_client
        self.tg = tg
        self.address = address

        # Actor UUID to actor receive func.
        self._callback: Callable[[ActorId, str], None] | None = None

    def addresses(self) -> list[Address]:
        return [self.address]

    @classmethod
    @asynccontextmanager
    async def create(cls, address: RedisAddress) -> AsyncGenerator[Self]:
        pool = redis.ConnectionPool.from_url(address.connection_string)
        client = redis.Redis.from_pool(pool)
        try:
            async with create_task_group() as tg:
                instance = cls(client, tg, address)
                tg.start_soon(instance._subscribe)
                yield instance

                tg.cancel_scope.cancel()
        finally:
            with move_on_after(1.0, shield=True):
                await client.aclose()

    @asynccontextmanager
    async def listen(
        self, callback: Callable[[ActorId, str], None]
    ) -> AsyncGenerator[None]:
        self._callback = callback

        try:
            yield
        finally:
            self._callback = None

    @asynccontextmanager
    async def register(
        self, addresses: Sequence[Address], actor_id: ActorId, name: str
    ) -> AsyncGenerator[None]:
        yield  # TODO!

    async def _subscribe(self) -> None:
        while True:
            # Wait until there is any message available for our process.
            # TODO: probably we can pop *all* to reduce the number of Redis roundtrips.
            _, actor_queue_name_bytes = await self.redis_client.blpop(  # type:ignore[misc]
                [f"actors-ready:{self.address.node_id}"]
            )
            actor_queue_name = actor_queue_name_bytes.decode()

            # Now, look at the specific actor.
            _, actor_message = await self.redis_client.blpop(  # type:ignore[misc]
                [f"actor-queue:{actor_queue_name}"]
            )

            callback = self._callback
            if callback is None:
                print("Received message for unknown actor.")
                # Dead letter queue!
            else:
                callback(actor_queue_name, actor_message.decode())
