import threading
from threading import Thread

from pydantic import BaseModel

from actorium import Mailbox, SimpleActor, SimpleRef, lookup, run, spawn
from actorium.actors import Signal, SignalRef

from .utils import assert_soon_equal


def test_actors() -> None:
    items: list[int] = []

    class Collector(SimpleActor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                items.append(msg)

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            ref = spawn(Collector)

            ref.tell(1)
            ref.tell(2)
            ref.tell(3)

            await assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)


def _skip_test_actor_registry() -> None:
    ready = threading.Event()
    received_items: list[int] = []

    def thread_1() -> None:
        nonlocal received_items

        class Receiver(SimpleActor[int]):
            def __init__(self, items: list[int]) -> None:
                self.items = items

            async def run(self, mailbox: Mailbox[int]) -> None:
                async for msg in mailbox:
                    self.items.append(msg)

        class Thread1(SimpleActor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                _ref = spawn(Receiver, received_items, name="our-actor")
                ready.set()
                await assert_soon_equal(lambda: received_items, [1, 2, 3])
                return

        run(Thread1)

    def thread_2() -> None:
        class Thread2(SimpleActor[None]):
            async def run(self, mailbox: Mailbox[None]) -> None:
                ready.wait()

                collector = await lookup("our-actor", SimpleRef[int], timeout=1)

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
    reply_to: SimpleRef[int]


def test_send_actor_to_actor() -> None:
    items: list[int] = []

    class EchoActor(SimpleActor[EchoMsg]):
        async def run(self, mailbox: Mailbox[EchoMsg]) -> None:
            async for msg in mailbox:
                msg.reply_to.tell(msg.value)

    class Receiver(SimpleActor[int]):
        async def run(self, mailbox: Mailbox[int]) -> None:
            async for msg in mailbox:
                items.append(msg)

    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            echo_ref = spawn(EchoActor)
            receiver_ref = spawn(Receiver)

            echo_ref.tell(EchoMsg(value=1, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=2, reply_to=receiver_ref))
            echo_ref.tell(EchoMsg(value=3, reply_to=receiver_ref))

            await assert_soon_equal(lambda: items, [1, 2, 3])

    run(Main)
    assert items == [1, 2, 3]


def test_ref_with_registration() -> None:
    class Main(SimpleActor[None]):
        async def run(self, mailbox: Mailbox[None]) -> None:
            number = spawn(Signal[int], 10, name="our-actor")

            assert await number.get() == 10
            number.set(20)

            number2 = await lookup("our-actor", SignalRef[int], timeout=1)
            assert number2 is not None
            assert await number2.get() == 20

    run(Main)
