from __future__ import annotations

from ipaddress import IPv4Address
from logging import getLogger
from typing import assert_never

from anyio import (
    BrokenResourceError,
    connect_tcp,
    create_task_group,
    create_tcp_listener,
    move_on_after,
    sleep,
    sleep_forever,
)
from anyio.abc import SocketStream
from pydantic import TypeAdapter

from ..actors.future import future
from ..core import Actor, Mailbox, Ref
from ..core.system import (
    MessageForActor,
    PublishMessage,
    PublishRoute,
    Register,
    RegisterRoute,
    Unregister,
    get_system,
    spawn,
)
from ._line_protocol import LineReceiver

__all__ = [
    "TcpServer",
    "TcpClient",
]

type Host = IPv4Address | str
type RouterMessage = MessageForActor | PublishRoute | Register | Unregister

_adapter: TypeAdapter[RouterMessage] = TypeAdapter(RouterMessage)

_logger = getLogger(__name__)


class TcpServer(Actor[None]):
    """
    Actor that listens on the given host/port and accept connection from
    another actor system through a `TcpClient`.
    """

    def __init__(self, host: Host, port: int) -> None:
        self.host = host
        self.port = port

    async def run(self, mailbox: Mailbox[None]) -> None:
        backoff = 0.5
        while True:
            try:
                listener = await create_tcp_listener(
                    local_host=self.host, local_port=self.port
                )
            except OSError:
                _logger.exception(
                    "Failed to listen on port: %s. Trying again in %s seconds",
                    self.port,
                    backoff,
                )
                await sleep(backoff)
                backoff = min(20, backoff * 2)
            else:
                break

        try:
            await listener.serve(self._handle_connection)
        finally:
            with move_on_after(1.0, shield=True):
                await listener.aclose()

    async def _handle_connection(self, client: SocketStream) -> None:
        async with spawn(_TcpConnection, client):
            await sleep_forever()


class TcpClient(Actor[None]):
    def __init__(self, host: Host, port: int) -> None:
        self.host = host
        self.port = port

    async def run(self, mailbox: Mailbox[None]) -> None:
        backoff_seconds = 0.5
        while True:
            try:
                client = await connect_tcp(self.host, self.port)
            except OSError:
                await sleep(backoff_seconds)
                backoff_seconds *= 1.5
            else:
                backoff_seconds = 0.5
                async with client:
                    async with future[None]() as (done_future, ref):
                        async with spawn(_TcpConnection, client, ref):
                            await done_future


class _TcpConnection(Actor[MessageForActor | PublishMessage]):
    def __init__(
        self, client: SocketStream, done_future: Ref[None] | None = None
    ) -> None:
        self._client = client
        self._done_future = done_future

    async def run(self, mailbox: Mailbox[MessageForActor | PublishMessage]) -> None:
        system = get_system()

        async with (
            create_task_group() as tg,
            system.subscribe_state(mailbox.ref()),
        ):
            tg.start_soon(self._read_tcp_stream, mailbox)

            try:
                async for msg in mailbox:
                    await self._client.send(msg.model_dump_json().encode() + b"\n")
            except BrokenResourceError:
                # Other side went away. Cancel and leave.
                tg.cancel_scope.cancel()
                return

    async def _read_tcp_stream(
        self, mailbox: Mailbox[MessageForActor | PublishMessage]
    ) -> None:
        "Read incoming TCP messages."
        line_receiver = LineReceiver(self._client)
        system = get_system()

        published_names: dict[str, Register] = {}

        async for line in line_receiver:
            msg = _adapter.validate_json(line)

            match msg:
                case MessageForActor():
                    system.tell(msg)
                case PublishRoute(system_id=system_id):
                    system.tell(
                        RegisterRoute(system_id=system_id, gateway=mailbox.ref())
                    )
                case Register(name=name):
                    system.tell(msg)
                    published_names[name] = msg
                case Unregister(name=name, unregister_key=unregister_key):
                    system.tell(Unregister(name=name, unregister_key=unregister_key))
                    if (
                        name in published_names
                        and published_names[name].unregister_key == unregister_key
                    ):
                        del published_names[name]
                case _:
                    assert_never(msg)

        # If the TCP connection drops, immediately tell the system to
        # unregister all routes/names that were published through this actor.
        for publish_name in published_names.values():
            system.tell(
                Unregister(
                    name=publish_name.name, unregister_key=publish_name.unregister_key
                )
            )

        if self._done_future is not None:
            self._done_future.tell(None)
