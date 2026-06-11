from ipaddress import IPv4Address
from logging import getLogger
from typing import Never

from anyio import (
    BrokenResourceError,
    connect_tcp,
    create_memory_object_stream,
    create_task_group,
    create_tcp_listener,
    move_on_after,
    sleep,
)
from anyio.abc import SocketStream
from anyio.streams.memory import MemoryObjectSendStream

from actorium.actors import SimpleActor
from actorium.serialization import deserialize, serialize
from actorium.system import GatewayMessage, connect_gateway

from ._line_protocol import LineReceiver

__all__ = [
    "TcpServer",
    "TcpClient",
    "handle_tcp_connection",
]

type Host = IPv4Address | str


_logger = getLogger(__name__)


class TcpServer(SimpleActor[Never]):
    """
    Actor that listens on the given host/port and accepts connections from
    another actor system through a `TcpClient`.
    """

    def __init__(self, host: Host, port: int) -> None:
        self.host = host
        self.port = port

    async def actor_run(self) -> None:
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
        await handle_tcp_connection(client)


class TcpClient(SimpleActor[Never]):
    def __init__(self, host: Host, port: int) -> None:
        self.host = host
        self.port = port

    async def actor_run(self) -> None:
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
                    await handle_tcp_connection(client)


async def handle_tcp_connection(client: SocketStream) -> None:
    writer, reader = create_memory_object_stream[GatewayMessage]()

    async def tcp_to_system(writer: MemoryObjectSendStream[GatewayMessage]) -> None:
        "Read incoming TCP messages."
        line_receiver = LineReceiver(client)

        async for line in line_receiver:
            msg = deserialize(line, type=GatewayMessage)
            await writer.send(msg)

    async with (
        create_task_group() as tg,
        connect_gateway(reader) as system_to_tcp,
    ):
        tg.start_soon(tcp_to_system, writer)

        try:
            async for msg in system_to_tcp:
                await client.send(serialize(msg).encode() + b"\n")
        except BrokenResourceError:
            # Other side went away. Cancel and leave.
            tg.cancel_scope.cancel()
            return
