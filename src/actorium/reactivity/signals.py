from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Literal

from pydantic import BaseModel

from ..actors import Actor, ActorRef, spawn
from .future import create_future

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


class _Signal[T](Actor[SignalReaderMsg[T]]):
    def __init__(self, type_: type[T], initial: T) -> None:
        self._type = type_
        self.value = initial
        self.subscriptions: set[ActorRef[T]] = set()

    def message_type(self) -> type[T]:
        return SignalReaderMsg[self._type]

    async def set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        for subscription in self.subscriptions:
            subscription.tell(value)

    async def receive(self, msg: SignalReaderMsg[T]) -> None:
        if isinstance(msg, Get):
            msg.reply_to.tell(self.value)
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
        async with create_future(self.message_type()) as (future, reply_to):
            self.tell(Get[T](reply_to=reply_to))
            return await future.result()

    @asynccontextmanager
    async def subscribe(self, reply_to: ActorRef[T]) -> AsyncGenerator[None]:
        """
        Subscribe to `ref` changes, tell the actor to send the updates to the
        given `reply_to` actor.
        """
        self.tell(Subscribe[T](actor=reply_to))
        try:
            yield
        finally:
            self.tell(Unsubscribe[T](actor=reply_to))


type SignalSetter[T] = Callable[[T], Coroutine[Any, Any, None]]


@asynccontextmanager
async def signal[T](
    type_: type[T], /, initial: T
) -> AsyncGenerator[tuple[SignalReader[T], SignalSetter[T]]]:
    """
    Create a reactive signal. This produces an actor for observing the signal
    and retrieving its value, and a setter for storing a new value.

    Usage::

        async with signal(int, initial=0) as (count, set_count):
            await set_count(...)
            value = count.get()
    """
    async with spawn(_Signal[T], type_, initial) as (signal, ref):
        yield ref.wrap(SignalReader[type_]), signal.set  # type:ignore
