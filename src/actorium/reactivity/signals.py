from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Literal, assert_never, get_args

from pydantic import BaseModel

from ..actors import Actor, ActorRef, spawn
from ._generic import generic_function
from .future import future

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
type SignalReaderMsgType[T] = type[Subscribe[T] | Unsubscribe[T] | Get[T]]


class _Signal[T](Actor[SignalReaderMsg[T]]):
    def __init__(self, initial: T) -> None:
        self.value = initial
        self.subscriptions: set[ActorRef[T]] = set()

    def data_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    def message_type(self) -> SignalReaderMsgType[T]:
        return SignalReaderMsg[self.data_type()]  # type:ignore

    async def set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        for subscription in self.subscriptions:
            subscription.tell(value)

    async def receive(self, msg: SignalReaderMsg[T]) -> None:
        match msg:
            case Get(reply_to=reply_to):
                reply_to.tell(self.value)
            case Subscribe(actor=actor):
                self.subscriptions.add(actor)
            case Unsubscribe(actor=actor):
                self.subscriptions.discard(actor)
            case _ as x:
                assert_never(x)


class SignalReader[T](ActorRef[SignalReaderMsg[T]]):
    """
    Reference to a `signal` actor with helper methods for getting the state or
    subscribing to state changes.
    """

    async def get(self) -> T:
        if not TYPE_CHECKING:
            T = self.data_type()

        async with future[T]() as (f, reply_to):
            self.tell(Get[T](reply_to=reply_to))
            return await f.result()

    @classmethod
    def data_type(cls) -> type[T]:
        return cls.__pydantic_generic_metadata__["args"][0]  # type:ignore

    @classmethod
    def message_type(cls) -> SignalReaderMsgType[T]:
        return SignalReaderMsg[cls.data_type()]  # type:ignore

    @asynccontextmanager
    async def subscribe(self, reply_to: ActorRef[T]) -> AsyncGenerator[None]:
        """
        Subscribe to `ref` changes, tell the actor to send the updates to the
        given `reply_to` actor.
        """
        if not TYPE_CHECKING:
            T = self.data_type()

        self.tell(Subscribe[T](actor=reply_to))
        try:
            yield
        finally:
            self.tell(Unsubscribe[T](actor=reply_to))


type SignalSetter[T] = Callable[[T], Coroutine[Any, Any, None]]


@generic_function
@asynccontextmanager
async def signal[T](initial: T) -> tuple[SignalReader[T], SignalSetter[T]]:
    """
    Create a reactive signal. This produces an actor for observing the signal
    and retrieving its value and a setter for storing a new value.

    Usage::

        async with signal[int](initial=0) as (count, set_count):
            await set_count(...)
            value = count.get()
    """
    async with spawn(_Signal[T], initial) as (signal, ref):
        yield ref.wrap(SignalReader[T]), signal.set
