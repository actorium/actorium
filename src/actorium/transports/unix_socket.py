from __future__ import annotations

from pathlib import Path

from anyio import connect_unix, create_unix_listener, move_on_after, sleep
from anyio.abc import SocketStream

from ..core import Actor, Mailbox
from .tcp import handle_tcp_connection

__all__ = [
    "UnixServer",
    "UnixClient",
]


class UnixServer(Actor[None]):
    """
    Actor that listens on the given unix path and accept connection from
    another actor system through a `UnixClient`.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    async def run(self, mailbox: Mailbox[None]) -> None:
        listener = await create_unix_listener(self.path)

        try:
            await listener.serve(self._handle_connection)
        finally:
            with move_on_after(1.0, shield=True):
                await listener.aclose()

    async def _handle_connection(self, client: SocketStream) -> None:
        await handle_tcp_connection(client)


class UnixClient(Actor[None]):
    def __init__(self, path: Path) -> None:
        self.path = path

    async def run(self, mailbox: Mailbox[None]) -> None:
        backoff_seconds = 0.5
        while True:
            try:
                client = await connect_unix(self.path)
            except OSError:
                await sleep(backoff_seconds)
                backoff_seconds *= 1.5
            else:
                backoff_seconds = 0.5
                async with client:
                    await handle_tcp_connection(client)
