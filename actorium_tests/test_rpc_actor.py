from uuid import uuid4

from actorium import ActorAddress, RpcActor, RpcRef, SimpleRef
from actorium.actors.simple_rpc import RpcMessage


def test_type_caching() -> None:
    assert RpcMessage[tuple[int], int] == RpcMessage[tuple[int], int]

    assert RpcActor[int] == RpcActor[int]
    assert RpcActor[int] == RpcActor[(int,)]

    assert RpcRef[int] == RpcRef[int]
    assert RpcRef[int] == RpcRef[(int,)]


def test_rpc_message_creation() -> None:
    actor_address = ActorAddress(actor_id=uuid4(), system_id=uuid4())
    reply_to = SimpleRef[int](actor_address=actor_address)

    rpc_message = RpcMessage[tuple[int], int](inputs=(10,), reply_to=reply_to)
    assert rpc_message.inputs == (10,)
    assert rpc_message.reply_to == reply_to
