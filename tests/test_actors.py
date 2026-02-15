from collections.abc import Callable, Coroutine
from typing import Any

from anyio import sleep
from pydantic import BaseModel

from actorium import ActorRef, ActorRegistration, ActorSystem, spawn
from actorium.reactivity import Rpc, SignalReader, computed, rpc, signal


async def test_actors() -> None:
    async with ActorSystem.create():
        items: list[int] = []

        async def collect(msg: int) -> None:
            items.append(msg)

        async with spawn(collect) as collector:
            await collector.send(1)
            await collector.send(2)
            await collector.send(3)

            await _assert_soon_equal(lambda: items, [1, 2, 3])


async def test_actor_registry() -> None:
    entry = ActorRegistration[int]("our-actor")

    async with ActorSystem.create():
        async with ActorSystem.create() as system2:
            items: list[int] = []

            async def receive(msg: int) -> None:
                items.append(msg)

            async with spawn(receive, register=entry):
                collector = await ActorRef[int].from_registration(entry, system2)

                await collector.send(1)
                await collector.send(2)
                await collector.send(3)

                await _assert_soon_equal(lambda: items, [1, 2, 3])


class EchoMsg(BaseModel):
    value: int
    reply_to: ActorRef[int]


async def test_send_actor_to_actor() -> None:
    entry = ActorRegistration[EchoMsg]("our-actor")

    async with (
        ActorSystem.create() as system,
        ActorSystem.create() as system2,
    ):

        async def echo_receiver(msg: EchoMsg) -> None:
            await msg.reply_to.send(msg.value)

        items: list[int] = []

        async def receiver(msg: int) -> None:
            items.append(msg)

        async with (
            spawn(echo_receiver, system=system, register=entry),
            spawn(receiver, system=system2) as receiver_instance,
        ):
            echo_instance = await ActorRef[EchoMsg].from_registration(
                entry, system=system2
            )

            await echo_instance.send(EchoMsg(value=1, reply_to=receiver_instance))
            await echo_instance.send(EchoMsg(value=2, reply_to=receiver_instance))
            await echo_instance.send(EchoMsg(value=3, reply_to=receiver_instance))

            await _assert_soon_equal(lambda: items, [1, 2, 3])


async def test_ref() -> None:
    async with ActorSystem.create():
        async with signal(initial=10) as (number, set_number):
            assert await number.get() == 10
            await set_number(20)
            assert await number.get() == 20


async def test_ref_with_registration() -> None:
    entry = ActorRegistration[int]("our-actor")

    async with (
        ActorSystem.create(),
        ActorSystem.create() as system2,
    ):
        async with signal(initial=10, register=entry) as (number, set_number):
            assert await number.get() == 10
            await set_number(20)

            number2 = await SignalReader[int].from_registration(entry, system=system2)
            assert await number2.get() == 20


async def test_rpc_actor() -> None:
    async with ActorSystem.create():

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc(double_it) as double_actor:
            assert await double_actor.call(2) == 4
            assert await double_actor.call(3) == 6


async def test_rpc_actor_with_registration() -> None:
    entry = ActorRegistration[Rpc[int, int]]("our-actor")

    async with (
        ActorSystem.create(),
        ActorSystem.create() as system2,
    ):

        async def double_it(value: int) -> int:
            return value * 2

        async with rpc(double_it, register=entry) as double_actor:
            assert await double_actor.call(2) == 4
            assert await double_actor.call(3) == 6

            double_actor_2 = await Rpc[int, int].from_registration(
                entry, system=system2
            )
            assert await double_actor_2.call(5) == 10


async def test_computed() -> None:
    async with ActorSystem.create():
        # Create two reactive objects, number1 and number2
        async with (
            signal(initial=0) as (number1, set_number1),
            signal(initial=0) as (number2, set_number2),
        ):
            # The computation
            async def the_sum(value1: int, value2: int) -> int:
                return value1 + value2

            # Create a reactive computation.
            async with computed(the_sum, number1, number2) as ref3:
                assert await ref3.get() == 0

                # Change source objects.
                await set_number1(10)
                await set_number2(20)

                # Changes should propagate.
                await _assert_soon_equal(ref3.get, 30)


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
