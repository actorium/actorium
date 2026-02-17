from __future__ import annotations

from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

__all__ = [
    "Host",
    "TcpAddress",
    "RedisAddress",
    "InMemoryAddress",
    "Address",
    "ActorId",
]

type Host = str | IPv4Address | IPv6Address


class TcpAddress(BaseModel):
    port: int
    host: Host

    model_config = {"frozen": True}


class RedisAddress(BaseModel):
    node_id: str
    connection_string: str

    model_config = {"frozen": True}


class InMemoryAddress(BaseModel):
    process_hash: str
    interpreter_hash: UUID
    listener_id: UUID
    model_config = {"frozen": True}


class UnixSocketAddress(BaseModel):
    path: Path


type Address = TcpAddress | UnixSocketAddress | InMemoryAddress | RedisAddress
type ActorId = UUID | Literal["REGISTRY"]
