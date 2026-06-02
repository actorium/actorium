from asyncio import Future, sleep
from collections import defaultdict
from functools import cache
from typing import TYPE_CHECKING, Never, get_args

from anyio import move_on_after

from actorium.types import ActorAddress
from actorium.utils import TtlMap

from .behaviors import BehaviorActor, BehaviorRef, behavior, rpc
from .simple import Mailbox, SimpleActor

__all__ = [
    "Registry",
    "RegistryRef",
    "Registration",
]


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

    def actor_ref(self, actor_address: ActorAddress) -> RegistryRef[T]:
        if not TYPE_CHECKING:
            T = get_args(self.__orig_class__)[0]
        return RegistryRef[T](actor_address=actor_address)

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


class RegistryRef[T](BehaviorRef[Registry[T]]):
    @cache
    @staticmethod
    def __class_getitem__(item: type) -> type:
        class _RegistryRef(RegistryRef):  # type: ignore
            # Actor class that the behavior is pointing to.
            _a = Registry[item]  # type: ignore
            _t = item

        return _RegistryRef


class Registration[T](SimpleActor[Never]):
    """
    Keep a name registration alive for a given value by publishing it
    periodically.
    """

    def __init__(self, registry_ref: RegistryRef[T], name: str, value: T) -> None:
        self.registry_ref = registry_ref
        self.name = name
        self.value = value

    async def run(self, mailbox: Mailbox[Never]) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self.registry_ref.be.publish(self.name, self.value, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self.registry_ref.be.unpublish(self.name)
