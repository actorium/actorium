from dataclasses import dataclass
from uuid import UUID

__all__ = [
    "ActorId",
    "SystemId",
    "ActorAddress",
]


type ActorId = UUID
type SystemId = UUID


@dataclass(frozen=True)
class ActorAddress:
    actor_id: ActorId
    system_id: SystemId
