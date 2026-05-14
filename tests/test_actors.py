import threading
from collections.abc import Callable, Coroutine
from threading import Thread
from typing import Any

from anyio import create_task_group, sleep, to_thread
from pydantic import BaseModel

from actorium import Actor, Mailbox, Ref, lookup, run, spawn
from actorium.actors import computed, signal
from actorium.transports import TcpClient, TcpServer


def test_actors() -> None:
    items: list[int] = []

    class Collector(Actor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                items.append(msg)

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            ref = spawn(Collector)

            ref.tell(1)
            ref.tell(2)
            ref.tell(3)

            await _assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)


def _skip_test_actor_registry() -> None:
    ready = threading.Event()
    received_items: list[int] = []

    def thread_1() -> None:
        nonlocal received_items

        class Receiver(Actor[int]):
            def __init__(self, items: list[int]) -> None:
                self.items = items

            async def run(self, mailbox: Mailbox[int]) -> None:
                async for msg in mailbox:
                    self.items.append(msg)

        class Thread1(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                _ref = spawn(Receiver, received_items, name="our-actor")
                ready.set()
                await _assert_soon_equal(lambda: received_items, [1, 2, 3])
                return

        run(Thread1)

    def thread_2() -> None:
        class Thread2(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                ready.wait()

                collector = await lookup("our-actor", Ref[int], timeout=1)

                collector.tell(1)
                collector.tell(2)
                collector.tell(3)
                return

        run(Thread2)

    th1 = Thread(target=thread_1)
    th2 = Thread(target=thread_2)
    th1.start()
    th2.start()
    th1.join()
    th2.join()

    assert received_items == [1, 2, 3]


class EchoMsg(BaseModel):
    value: int
    reply_to: Ref[int]


def test_send_actor_to_actor() -> None:
    items: list[int] = []

    class EchoActor(Actor[EchoMsg]):
        async def run(self, mailbox: Mailbox[EchoMsg]) -> None:
            async for msg in mailbox:
                msg.reply_to.tell(msg.value)

    class Receiver(Actor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                items.append(msg)

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            echo_ref = spawn(EchoActor)
            receiver_ref = spawn(Receiver)

            echo_ref.tell(EchoMsg(value=1, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=2, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=3, reply_to=receiver_ref))

            await _assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)
    assert items == [1, 2, 3]


def test_ref() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = signal[int].new(10)

            assert await number.get() == 10
            number.set(20)
            assert await number.get() == 20

    run(Main)


def test_ref_with_registration() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = signal[int].new(10, name="our-actor")

            assert await number.get() == 10
            number.set(20)

            number2 = await lookup("our-actor", signal[int], timeout=1)
            assert number2 is not None
            assert await number2.get() == 20

    run(Main)


def test_computed() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            # Create two reactive objects, number1 and number2
            number1 = signal[int].new(0)
            number2 = signal[int].new(0)

            # Check type parameters.
            assert isinstance(number1, signal[int])
            assert isinstance(number2, signal[int])

            # The computation
            @computed(number1, number2)
            async def the_sum(value1: int, value2: int) -> int:
                return value1 + value2

            # Check type for computed and type parameter.
            assert isinstance(the_sum, signal[int])

            # Create a reactive computation.
            assert await the_sum.get() == 0

            # Change source objects.
            number1.set(10)
            number2.set(20)

            # Changes should propagate.
            await _assert_soon_equal(the_sum.get, 30)

    run(Main)


async def test_tcp_protocol() -> None:
    received_items = []

    class Collector(Actor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                received_items.append(msg)

    def thread_1() -> None:
        class Thread1(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                spawn(TcpServer, "localhost", 9000)
                spawn(Collector, name="our-actor")

                # Sleep until equal.
                await _assert_soon_equal(lambda: received_items, [1, 2, 3])

        run(Thread1)

    def thread_2() -> None:
        class Thread2(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                spawn(TcpClient, "localhost", 9000)

                # Wait until this actor comes online.
                actor_ref = await lookup("our-actor", Ref[int], timeout=5)

                actor_ref.tell(1)
                actor_ref.tell(2)
                actor_ref.tell(3)

                # Sleep until equal.
                await _assert_soon_equal(lambda: received_items, [1, 2, 3])

        run(Thread2)

    async with create_task_group() as tg:
        tg.start_soon(to_thread.run_sync, thread_1)
        tg.start_soon(to_thread.run_sync, thread_2)

    assert received_items == [1, 2, 3]


async def _assert_soon_equal[T](
    obj1: Callable[[], T | Coroutine[Any, Any, T]], obj2: T
) -> None:
    async def get() -> T:
        value = obj1()
        if isinstance(value, Coroutine):
            return await value
        return value

    for i in range(1000):
        if await get() == obj2:
            return
        await sleep(0.01)

    assert await get() == obj2
