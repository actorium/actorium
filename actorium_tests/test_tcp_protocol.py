from anyio import create_task_group, to_thread

from actorium import SimpleActor, SimpleRef, create_actor_system_and_run, lookup, spawn
from actorium.transports import TcpClient, TcpServer

from .utils import assert_soon_equal


async def test_tcp_protocol() -> None:
    received_items: list[int] = []

    class Collector(SimpleActor[int]):
        async def actor_run(self) -> None:
            async for (msg,) in self.mailbox:
                received_items.append(msg)

    def thread_1() -> None:
        async def main() -> None:
            spawn(TcpServer, "localhost", 9000)
            spawn(Collector, name="our-actor")

            # Sleep until equal.
            await assert_soon_equal(lambda: received_items, [1, 2, 3])

        create_actor_system_and_run(main)

    def thread_2() -> None:
        async def main() -> None:
            spawn(TcpClient, "localhost", 9000)

            # Wait until this actor comes online.
            actor_ref = await lookup("our-actor", SimpleRef[int], timeout=5)

            actor_ref.tell(1)
            actor_ref.tell(2)
            actor_ref.tell(3)

            # Sleep until equal.
            await assert_soon_equal(lambda: received_items, [1, 2, 3])

        create_actor_system_and_run(main)

    async with create_task_group() as tg:
        tg.start_soon(to_thread.run_sync, thread_1)
        tg.start_soon(to_thread.run_sync, thread_2)

    assert received_items == [1, 2, 3]
