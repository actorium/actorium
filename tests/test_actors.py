import threading
from collections.abc import Callable, Coroutine
from typing import Any

from anyio import create_task_group, run, sleep, to_thread
from pydantic import BaseModel

from actorium import (
    Actor,
    ActorRef,
    TcpAddress,
    actor_system,
    name_resolver,
    register,
    spawn,
)
from actorium.reactivity import SignalRef, computed, rpc, signal
from actorium.reactivity.rpc import RpcRef
from actorium.transports import TcpListener


async def test_actors() -> None:
    items: list[int] = []

    class Collector(Actor[int]):
        async def receive(self, msg: int) -> None:
            items.append(msg)

    async with actor_system():
        async with spawn(Collector) as ref:
            ref.tell(1)
            ref.tell(2)
            ref.tell(3)

            await _assert_soon_equal(lambda: items, [1, 2, 3])


async def test_actor_registry() -> None:
    ready = threading.Event()
    received_items: list[int] = []
    thread1_addresses = []

    async def thread_1() -> None:
        nonlocal received_items, thread1_addresses

        async with actor_system() as system:
            thread1_addresses = system.addresses()

            class Receiver(Actor[int]):
                def __init__(self, items: list[int]) -> None:
                    self.items = items

                async def receive(self, msg: int) -> None:
                    self.items.append(msg)

            async with (
                spawn(Receiver, received_items) as ref,
                register(ref, "our-actor"),
            ):
                ready.set()
                await _assert_soon_equal(lambda: received_items, [1, 2, 3])
                return

    async def thread_2() -> None:
        async with actor_system():
            ready.wait()

            resolver = name_resolver(peer_addresses=thread1_addresses)
            collector = await resolver.resolve("our-actor", ActorRef[int])
            assert collector is not None

            collector.tell(1)
            collector.tell(2)
            collector.tell(3)
            return

    async with create_task_group() as tg:
        tg.start_soon(to_thread.run_sync, run, thread_1)
        tg.start_soon(to_thread.run_sync, run, thread_2)

    assert received_items == [1, 2, 3]


class EchoMsg(BaseModel):
    value: int
    reply_to: ActorRef[int]


async def test_send_actor_to_actor() -> None:
    async with actor_system():
        items: list[int] = []

        class EchoActor(Actor[EchoMsg]):
            async def receive(self, msg: EchoMsg) -> None:
                msg.reply_to.tell(msg.value)

        class Receiver(Actor[int]):
            async def receive(self, msg: int) -> None:
                items.append(msg)

        async with (
            spawn(EchoActor) as echo_ref,
            spawn(Receiver) as receiver_ref,
        ):
            echo_ref.tell(EchoMsg(value=1, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=2, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=3, reply_to=receiver_ref))

            await _assert_soon_equal(lambda: items, [1, 2, 3])


async def test_ref() -> None:
    async with actor_system():
        async with signal[int](10) as number:
            assert await number.get() == 10
            number.set(20)
            assert await number.get() == 20


async def test_ref_with_registration() -> None:
    async with actor_system():
        async with (
            signal[int](10) as number,
            register(number, "our-actor"),
        ):
            assert await number.get() == 10
            number.set(20)

            number2 = await name_resolver().resolve("our-actor", SignalRef[int])
            assert number2 is not None
            assert await number2.get() == 20


async def test_rpc_actor() -> None:
    async with actor_system():

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc[int, int](double_it) as double_actor:
            assert await double_actor.ask(2) == 4
            assert await double_actor.ask(3) == 6


async def test_rpc_actor_with_registration() -> None:
    async with actor_system():

        async def double_it(value: int) -> int:
            return value * 2

        async with (
            rpc[int, int](double_it) as double_actor,
            register(double_actor, name="double-it"),
        ):
            assert await double_actor.ask(2) == 4
            assert await double_actor.ask(3) == 6

            double_actor_2 = await name_resolver().resolve(
                "double-it", RpcRef[int, int]
            )
            assert double_actor_2 is not None
            assert await double_actor_2.ask(5) == 10


async def test_computed() -> None:
    async with actor_system():
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


async def test_tcp_protocol() -> None:
    received_items = []

    class Collector(Actor[int]):
        async def receive(self, msg: int) -> None:
            received_items.append(msg)

    async def thread_1() -> None:
        async with (
            TcpListener.create(host="localhost", port=9000) as tcp_listener,
            actor_system(listeners=[tcp_listener]),
            spawn(Collector) as ref,
            register(ref, "our-actor"),
        ):
            # Sleep until equal.
            await _assert_soon_equal(lambda: received_items, [1, 2, 3])

    async def thread_2() -> None:
        async with actor_system():
            resolver = name_resolver(
                peer_addresses=[TcpAddress(host="localhost", port=9000)]
            )

            # Wait until this actor comes online.
            while True:
                actor_ref = await resolver.resolve("our-actor", ActorRef[int])
                if actor_ref is None:
                    await sleep(0.1)
                else:
                    break

            assert actor_ref is not None
            actor_ref.tell(1)
            actor_ref.tell(2)
            actor_ref.tell(3)

    async with create_task_group() as tg:
        tg.start_soon(to_thread.run_sync, run, thread_1)
        tg.start_soon(to_thread.run_sync, run, thread_2)

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
