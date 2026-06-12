from asyncio import Future, sleep
from collections import defaultdict
from typing import Never

from anyio import move_on_after
from msgspec import Struct

from actorium.utils import TtlMap, TtlSet

from .behaviors import BehaviorActor, BehaviorRef, behavior, rpc
from .simple import SimpleActor, SimpleRef

__all__ = [
    "Registry",
    "Registration",
]


class RegistrySubscribeNotification[T](Struct):
    name: str
    value: T


class Registry[T](BehaviorActor):
    """
    Object registration (key/value store) where actors can store objects or
    actor references under a given name. The registration has to be kept alive,
    which can be done through a `Registration` actor.

    Example usage::

            # Create a registry.
            registry = spawn(Registry[int])

            # Store a value.
            spawn(Registration[int], registry, "name1", 1)

            # Retrieve a value.
            result = await registry.rpc.get("name1", 1)
    """

    def __init__(self) -> None:
        self._registered_data: TtlMap[str, T] = TtlMap()
        self._get_waiters: dict[str, set[Future[T]]] = defaultdict(set)
        self._subscriptions: TtlSet[SimpleRef[RegistrySubscribeNotification[T]]] = (
            TtlSet()
        )

    @behavior
    def publish(self, name: str, value: T, ttl_seconds: float) -> None:
        self._registered_data.set(name, value, ttl_seconds=ttl_seconds)

        for waiter in self._get_waiters[name]:
            waiter.set_result(value)
        del self._get_waiters[name]

    @behavior
    def unpublish(self, name: str) -> None:
        self._registered_data.pop(name)

    @rpc
    async def get(self, name: str, timeout: float) -> T | None:
        result = self._registered_data.get(name)
        if result is not None:
            return result

        f = Future[T]()

        self._get_waiters[name].add(f)
        try:
            with move_on_after(timeout):
                return await f
        finally:
            self._get_waiters[name].discard(f)

        return None

    @rpc
    async def keys(self) -> list[str]:
        return list(self._registered_data.keys())

    @behavior
    def subscribe(
        self, reply_to: SimpleRef[RegistrySubscribeNotification[T]], ttl_seconds: float
    ) -> None:
        self._subscriptions.add(reply_to, ttl_seconds)
        for name, value in self._registered_data.items():
            reply_to.tell(RegistrySubscribeNotification(name=name, value=value))

    @behavior
    def unsubscribe(self, actor: SimpleRef[RegistrySubscribeNotification[T]]) -> None:
        self._subscriptions.discard(actor)


# type RegistryRef[T] = BehaviorRef[Registry[T]]
class RegistryRef[T](BehaviorRef[Registry[T]]):
    pass


class Registration[T](SimpleActor[Never]):
    """
    Keep a name registration alive for a given value by publishing it
    periodically.
    """

    def __init__(
        self, registry_ref: BehaviorRef[Registry[T]], name: str, value: T
    ) -> None:
        self.registry_ref = registry_ref
        self.name = name
        self.value = value

    async def actor_run(self) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self.registry_ref.publish(self.name, self.value, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self.registry_ref.unpublish(self.name)
