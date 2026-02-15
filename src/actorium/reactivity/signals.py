import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Literal

from anyio import move_on_after
from pydantic import BaseModel

from ..actors import ActorRef, ActorRegistration, spawn

__all__ = [
    # Messages.
    "Subscribe",
    "Unsubscribe",
    "Get",
    "SignalReaderMsg",
    # ActorRef.
    "SignalReader",
    "signal",
]


class Subscribe[T](BaseModel):
    type: Literal["subscribe"] = "subscribe"
    actor: ActorRef[T]


class Unsubscribe[T](BaseModel):
    type: Literal["unsubscribe"] = "unsubscribe"
    actor: ActorRef[T]


class Get[T](BaseModel):
    type: Literal["get"] = "get"
    reply_to: ActorRef[T]


type SignalReaderMsg[T] = Subscribe[T] | Unsubscribe[T] | Get[T]


class _Signal[T]:
    def __init__(self, initial: T) -> None:
        self.value = initial
        self.subscriptions: set[ActorRef[T]] = set()

    async def set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        for subscription in self.subscriptions:
            await subscription.send(value)

    async def receive(self, msg: SignalReaderMsg[T]) -> None:
        if isinstance(msg, Get):
            await msg.reply_to.send(self.value)
        elif isinstance(msg, Subscribe):
            self.subscriptions.add(msg.actor)
        elif isinstance(msg, Unsubscribe):
            self.subscriptions.discard(msg.actor)


class SignalReader[T](ActorRef[SignalReaderMsg[T]]):
    """
    Reference to a `signal` actor with helper methods for getting the state or
    subscribing to state changes.
    """

    async def get(self) -> T:
        f = asyncio.Future[T]()

        async def get_result(msg: T) -> None:
            f.set_result(msg)

        async with spawn(get_result) as reply_to:
            await self.send(Get[T](reply_to=reply_to))
            return await f

    @asynccontextmanager
    async def subscribe(self, reply_to: ActorRef[T]) -> AsyncGenerator[None]:
        """
        Subscribe to `ref` changes, tell the actor to send the updates to the
        given `reply_to` actor.
        """
        await self.send(Subscribe[T](actor=reply_to))
        try:
            yield
        finally:
            with move_on_after(1, shield=True):
                await self.send(Unsubscribe[T](actor=reply_to))


type SignalSetter[T] = Callable[[T], Coroutine[Any, Any, None]]


@asynccontextmanager
async def signal[T](
    initial: T, register: ActorRegistration[T] | None = None
) -> AsyncGenerator[tuple[SignalReader[T], SignalSetter[T]]]:
    """
    Create a reactive signal. This produces an actor for observing the signal
    and retrieving its value, and a setter for storing a new value.

    Usage::

        async with signal(initial=...) as (count, set_count):
            await set_count(...)
            value = count.get()
    """
    signal = _Signal(initial)

    async with SignalReader[T].spawn(
        signal.receive, register=register
    ) as signal_reader:
        yield signal_reader, signal.set
