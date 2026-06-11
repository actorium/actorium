from typing import Any, NewType
from uuid import UUID

import msgspec

from .types import ActorAddress

__all__ = [
    "serialize",
    "deserialize",
    "SerializedData",
]

SerializedData = NewType("SerializedData", str)


def _enc_hook(obj: Any) -> Any:
    from .actors.simple import SimpleRef
    from .actors.simple_rpc import RpcRef

    if isinstance(obj, SimpleRef):
        return {
            "type": "simpleRef",
            "actorId": obj.actor_address.actor_id,
            "systemId": obj.actor_address.system_id,
        }
    if isinstance(obj, RpcRef):
        return {
            "type": "rpcRef",
            "actorId": obj.actor_address.actor_id,
            "systemId": obj.actor_address.system_id,
        }

    raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


def _dec_hook(type: type, obj: Any) -> Any:
    from .actors.simple import SimpleRef
    from .actors.simple_rpc import RpcRef

    # `type` here is the value of the custom type annotation being decoded.
    if issubclass(type, SimpleRef):
        assert obj["type"] == "simpleRef"

        return type(
            actor_address=ActorAddress(
                actor_id=UUID(obj["actorId"]),
                system_id=UUID(obj["systemId"]),
            )
        )
    if issubclass(type, RpcRef):
        assert obj["type"] == "rpcRef"

        return type(
            actor_address=ActorAddress(
                actor_id=UUID(obj["actorId"]),
                system_id=UUID(obj["systemId"]),
            )
        )

    # Raise a NotImplementedError for other types
    raise NotImplementedError(f"Objects of type {type} are not supported")


def deserialize[T](value: SerializedData, *, type: type[T]) -> T:
    decoder = msgspec.json.Decoder(type, dec_hook=_dec_hook)
    return decoder.decode(value.encode())


def serialize(value: object) -> SerializedData:
    encoder = msgspec.json.Encoder(enc_hook=_enc_hook)
    return SerializedData(encoder.encode(value).decode())
