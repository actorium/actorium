from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Literal

from anyio import fail_after
from pydantic import BaseModel

from ..actors import Actor, ActorRef
from ..addresses import Address
from .future import future

__all__ = [
    "Registry",
    "NameResolver",
]


class Register(BaseModel):
    type: Literal["register"] = "register"
    name: str
    actor_ref: ActorRef[Any]


class Unregister(BaseModel):
    type: Literal["unregister"] = "unregister"
    name: str


class Lookup(BaseModel):
    type: Literal["lookup"] = "lookup"
    name: str
    reply_to: ActorRef[LookupResultMessage]


type RegistryMessage = Register | Unregister | Lookup
type LookupResultMessage = ActorRef[Any] | None


class Registry(Actor[RegistryMessage]):
    def __init__(self) -> None:
        self._name_to_actor_ref: dict[str, ActorRef[Any]] = {}

    async def receive(self, msg: RegistryMessage) -> None:
        if isinstance(msg, Register):
            self._name_to_actor_ref[msg.name] = msg.actor_ref
        elif isinstance(msg, Unregister):
            self._name_to_actor_ref.pop(msg.name)
        elif isinstance(msg, Lookup):
            actor_ref = self._name_to_actor_ref.get(msg.name)
            msg.reply_to.tell(actor_ref)

    @contextmanager
    def register(self, actor_ref: ActorRef[Any], name: str) -> Generator[None]:
        self._name_to_actor_ref[name] = actor_ref
        try:
            yield
        finally:
            del self._name_to_actor_ref[name]


class RegistryRef(ActorRef[RegistryMessage]):
    async def lookup(
        self, name: str, timeout: float | None = None
    ) -> ActorRef[Any] | None:
        async with future[LookupResultMessage]() as (f, reply_to):
            self.tell(Lookup(name=name, reply_to=reply_to))
            with fail_after(timeout):
                return await f


class NameResolver:
    def __init__(self, addresses: list[Address]) -> None:
        self.addresses = addresses

    async def resolve[T: ActorRef[Any]](
        self,
        name: str,
        type_: type[T],
        /,
        *,
        timeout: float | None = None,
    ) -> T | None:
        for address in self.addresses:
            registry_ref = RegistryRef(actor_id="REGISTRY", addresses=[address])
            actor_ref = await registry_ref.lookup(name=name, timeout=timeout)

            if actor_ref is None:
                continue  # Try next address.

            return type_(
                addresses=actor_ref.addresses,
                actor_id=actor_ref.actor_id,
            )

        return None  # Not found.
