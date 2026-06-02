from anyio import create_task_group, to_thread

from actorium import Mailbox, SimpleActor, SimpleRef, lookup, run, spawn
from actorium.transports import TcpClient, TcpServer

from .utils import assert_soon_equal


async def test_tcp_protocol() -> None:
    received_items = []

    class Collector(SimpleActor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                received_items.append(msg)

    def thread_1() -> None:
        class Thread1(SimpleActor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                spawn(TcpServer, "localhost", 9000)
                spawn(Collector, name="our-actor")

                # Sleep until equal.
                await assert_soon_equal(lambda: received_items, [1, 2, 3])

        run(Thread1)

    def thread_2() -> None:
        class Thread2(SimpleActor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                spawn(TcpClient, "localhost", 9000)

                # Wait until this actor comes online.
                actor_ref = await lookup("our-actor", SimpleRef[int], timeout=5)

                actor_ref.tell(1)
                actor_ref.tell(2)
                actor_ref.tell(3)

                # Sleep until equal.
                await assert_soon_equal(lambda: received_items, [1, 2, 3])

        run(Thread2)

    async with create_task_group() as tg:
        tg.start_soon(to_thread.run_sync, thread_1)
        tg.start_soon(to_thread.run_sync, thread_2)

    assert received_items == [1, 2, 3]
