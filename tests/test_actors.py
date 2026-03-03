import threading
from collections.abc import Callable, Coroutine
from threading import Thread
from typing import Any

from anyio import create_task_group, sleep, to_thread
from pydantic import BaseModel

from actorium import Actor, Mailbox, Ref, get_system, run, spawn
from actorium.actors import RpcRef, SignalRef, computed, rpc, signal
from actorium.transports import TcpClient, TcpServer


def test_actors() -> None:
    items: list[int] = []

    class Collector(Actor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                items.append(msg)

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            async with spawn(Collector) as ref:
                ref.tell(1)
                ref.tell(2)
                ref.tell(3)

                await _assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)


def test_actor_registry() -> None:
    system1 = None
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
                nonlocal system1

                system1 = get_system()

                async with (
                    spawn(Receiver, received_items) as ref,
                    system1.register(ref, "our-actor"),
                ):
                    system1 = get_system()
                    assert system1 is not None
                    ready.set()
                    await _assert_soon_equal(lambda: received_items, [1, 2, 3])
                    return

        run(Thread1)

    def thread_2() -> None:
        class Thread2(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                ready.wait()

                assert system1 is not None
                collector = await system1.lookup("our-actor", Ref[int])
                assert collector is not None

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
            async with (
                spawn(EchoActor) as echo_ref,
                spawn(Receiver) as receiver_ref,
            ):
                echo_ref.tell(EchoMsg(value=1, reply_to=receiver_ref))
                echo_ref.tell(EchoMsg(value=2, reply_to=receiver_ref))
                echo_ref.tell(EchoMsg(value=3, reply_to=receiver_ref))

                await _assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)
    assert items == [1, 2, 3]


def test_ref() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            async with signal[int](10) as number:
                assert await number.get() == 10
                number.set(20)
                assert await number.get() == 20

    run(Main)


def test_ref_with_registration() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            system = get_system()

            async with (
                signal[int](10) as number,
                system.register(number, "our-actor"),
            ):
                assert await number.get() == 10
                number.set(20)

                number2 = await system.lookup("our-actor", SignalRef[int])
                assert number2 is not None
                assert await number2.get() == 20

    run(Main)


def test_rpc_actor() -> None:
    async def double_it(value: int) -> int:
        return value * 2

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            async with rpc[int, int](double_it) as double_actor:
                assert await double_actor.ask(2) == 4
                assert await double_actor.ask(3) == 6

    run(Main)


def test_rpc_actor_with_registration() -> None:
    async def double_it(value: int) -> int:
        return value * 2

    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            system = get_system()

            async with (
                rpc[int, int](double_it) as double_actor,
                system.register(double_actor, name="double-it"),
            ):
                assert await double_actor.ask(2) == 4
                assert await double_actor.ask(3) == 6

                double_actor_2 = await system.lookup("double-it", RpcRef[int, int])
                assert double_actor_2 is not None
                assert await double_actor_2.ask(5) == 10

    run(Main)


def test_computed() -> None:
    class Main(Actor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            # Create two reactive objects, number1 and number2
            async with (
                signal[int](0) as number1,
                signal[int](0) as number2,
            ):
                # The computation
                async def the_sum(value1: int, value2: int) -> int:
                    return value1 + value2

                # Create a reactive computation.
                async with computed(int, the_sum, number1, number2) as ref3:
                    assert await ref3.get() == 0

                    # Change source objects.
                    number1.set(10)
                    number2.set(20)

                    # Changes should propagate.
                    await _assert_soon_equal(ref3.get, 30)

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
                async with (
                    spawn(TcpServer, "localhost", 9000),
                    spawn(Collector) as ref,
                    get_system().register(ref, "our-actor"),
                ):
                    # Sleep until equal.
                    await _assert_soon_equal(lambda: received_items, [1, 2, 3])

        run(Thread1)

    def thread_2() -> None:
        class Thread2(Actor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                async with spawn(TcpClient, "localhost", 9000):
                    # Wait until this actor comes online.
                    while True:
                        actor_ref = await get_system().lookup("our-actor", Ref[int])
                        if actor_ref is None:
                            await sleep(0.1)
                        else:
                            break

                    assert actor_ref is not None
                    actor_ref.tell(1)
                    actor_ref.tell(2)
                    actor_ref.tell(3)

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
