from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, final
from uuid import UUID

__all__ = [
    "ActorId",
    "SystemId",
    "ActorAddress",
    "Timeout",
]


type ActorId = UUID | Literal["REGISTRY"] | Literal["SYSTEM"]
type SystemId = UUID


@dataclass(frozen=True)
class ActorAddress:
    actor_id: ActorId
    system_id: SystemId


@final
class Timeout:
    "Sentinel value for when a timeout is returned."
