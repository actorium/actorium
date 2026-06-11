from uuid import UUID

from msgspec import Struct

__all__ = [
    "ActorId",
    "SystemId",
    "ActorAddress",
]


type ActorId = UUID
type SystemId = UUID


class ActorAddress(Struct, frozen=True):
    actor_id: ActorId
    system_id: SystemId
