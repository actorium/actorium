from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

__all__ = [
    "ActorId",
    "SystemId",
    "ActorAddress",
]


type ActorId = UUID | Literal["REGISTRY"] | Literal["SYSTEM"]
type SystemId = UUID


@dataclass(frozen=True)
class ActorAddress:
    actor_id: ActorId
    system_id: SystemId
