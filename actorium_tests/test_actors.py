import threading
from threading import Thread

from msgspec import Struct

from actorium import (
    SignalReader,
    SignalWriter,
    SimpleActor,
    SimpleRef,
    create_actor_system_and_run,
    lookup,
    spawn,
)
from actorium.actors import Signal

from .utils import assert_soon_equal


def test_actors() -> None:
    items: list[int] = []

    class Collector(SimpleActor[int]):
        async def actor_run(self) -> None:
            async for (msg,) in self.mailbox:
                items.append(msg)

    async def main() -> None:
        ref = spawn(Collector)

        ref.tell(1)
        ref.tell(2)
        ref.tell(3)

        await assert_soon_equal(lambda: items, [1, 2, 3])

    create_actor_system_and_run(main)


def _skip_test_actor_registry() -> None:
    ready = threading.Event()
    received_items: list[int] = []

    def thread_1() -> None:
        nonlocal received_items

        class Receiver(SimpleActor[int]):
            def __init__(self, items: list[int]) -> None:
                self.items = items

            async def actor_run(self) -> None:
                async for (msg,) in self.mailbox:
                    self.items.append(msg)

        async def main() -> None:
            _ref = spawn(Receiver, received_items, name="our-actor")
            ready.set()
            await assert_soon_equal(lambda: received_items, [1, 2, 3])

        create_actor_system_and_run(main)

    def thread_2() -> None:
        async def main() -> None:
            ready.wait()

            collector = await lookup("our-actor", SimpleRef[int], timeout=1)

            collector.tell(1)
            collector.tell(2)
            collector.tell(3)

        create_actor_system_and_run(main)

    th1 = Thread(target=thread_1)
    th2 = Thread(target=thread_2)
    th1.start()
    th2.start()
    th1.join()
    th2.join()

    assert received_items == [1, 2, 3]


class EchoMsg(Struct):
    value: int
    reply_to: SimpleRef[int]


def test_send_actor_to_actor() -> None:
    items: list[int] = []

    class EchoActor(SimpleActor[EchoMsg]):
        async def actor_run(self) -> None:
            async for (msg,) in self.mailbox:
                msg.reply_to.tell(msg.value)

    class Receiver(SimpleActor[int]):
        async def actor_run(self) -> None:
            async for (msg,) in self.mailbox:
                items.append(msg)

    async def main() -> None:
        echo_ref = spawn(EchoActor)
        receiver_ref = spawn(Receiver)

        echo_ref.tell(EchoMsg(value=1, reply_to=receiver_ref))
        echo_ref.tell(EchoMsg(value=2, reply_to=receiver_ref))
        echo_ref.tell(EchoMsg(value=3, reply_to=receiver_ref))

        await assert_soon_equal(lambda: items, [1, 2, 3])

    create_actor_system_and_run(main)
    assert items == [1, 2, 3]


def test_ref_with_registration() -> None:
    async def main() -> None:
        number, set_number = spawn(Signal[int], 10, name="our-actor")

        assert await number.get() == 10
        set_number(20)

        number2, set_number2 = await lookup(
            "our-actor", tuple[SignalReader[int], SignalWriter[int]], timeout=1
        )
        assert number2 is not None
        assert await number2.get() == 20

    create_actor_system_and_run(main)


def test_type_caching() -> None:
    assert SimpleActor[int] == SimpleActor[int]
    assert SimpleActor[int] == SimpleActor[(int,)]

    assert SimpleRef[int] == SimpleRef[int]
    assert SimpleRef[int] == SimpleRef[(int,)]

    # Check that when inheriting, `__class_getitem__` returns a
    # subtype of the inherited class.
    class NewSimpleActor[T](SimpleActor[T]):
        pass

    assert NewSimpleActor[int] == NewSimpleActor[int]
    assert issubclass(NewSimpleActor[int], NewSimpleActor)
