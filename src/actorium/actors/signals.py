from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Literal, assert_never, get_args

from pydantic import BaseModel

from ..core import Actor, ActorAddress, Mailbox, Ref, spawn
from ._generic import generic_function
from .future import future

__all__ = [
    # Messages.
    "Subscribe",
    "Unsubscribe",
    "Get",
    "SignalMsg",
    # Ref.
    "SignalRef",
    "signal",
]


class Subscribe[T](BaseModel):
    type: Literal["subscribe"] = "subscribe"
    actor: Ref[T]


class Unsubscribe[T](BaseModel):
    type: Literal["unsubscribe"] = "unsubscribe"
    actor: Ref[T]


class Get[T](BaseModel):
    type: Literal["get"] = "get"
    reply_to: Ref[T]


class Set[T](BaseModel):
    type: Literal["get"] = "get"
    value: T


type SignalMsg[T] = Subscribe[T] | Unsubscribe[T] | Get[T] | Set[T]
type SignalMsgType[T] = type[Subscribe[T] | Unsubscribe[T] | Get[T]]


class _Signal[T](Actor[SignalMsg[T]]):
    def __init__(self, initial: T) -> None:
        self.value = initial
        self.subscriptions: set[Ref[T]] = set()

    def data_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    def actor_ref(self, actor_address: ActorAddress) -> SignalRef[T]:
        if not TYPE_CHECKING:
            T = self.data_type()
        return SignalRef[T](actor_address=actor_address)

    def message_type(self) -> SignalMsgType[T]:
        return SignalMsg[self.data_type()]  # type:ignore

    def _set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        for subscription in self.subscriptions:
            subscription.tell(value)

    async def run(self, mailbox: Mailbox[SignalMsg[T]]) -> None:
        async for msg in mailbox:
            match msg:
                case Get(reply_to=reply_to):
                    reply_to.tell(self.value)
                case Set(value=value):
                    self._set(value)
                case Subscribe(actor=actor):
                    self.subscriptions.add(actor)
                case Unsubscribe(actor=actor):
                    self.subscriptions.discard(actor)
                case _ as x:
                    assert_never(x)


class SignalRef[T](Ref[SignalMsg[T]]):
    """
    Reference to a `signal` actor with helper methods for getting, setting or
    subscribing to the state.
    """

    async def get(self) -> T:
        if not TYPE_CHECKING:
            T = self.data_type()

        async with future[T]() as (f, reply_to):
            self.tell(Get[T](reply_to=reply_to))
            return await f

    def set(self, value: T) -> None:
        if not TYPE_CHECKING:
            T = self.data_type()

        self.tell(Set[T](value=value))

    @classmethod
    def data_type(cls) -> type[T]:
        return cls.__pydantic_generic_metadata__["args"][0]  # type:ignore

    @classmethod
    def message_type(cls) -> SignalMsgType[T]:
        return SignalMsg[cls.data_type()]  # type:ignore

    @asynccontextmanager
    async def subscribe(self, reply_to: Ref[T]) -> AsyncGenerator[None]:
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


@generic_function
@asynccontextmanager
async def signal[T](initial: T) -> AsyncGenerator[SignalRef[T]]:
    """
    Create a reactive signal. This produces an actor for observing the signal
    and retrieving its value and a setter for storing a new value.

    Usage::

        async with signal[int](initial=0) as count:
            count.set(...)
            value = await count.get()
    """
    async with spawn(_Signal[T], initial) as ref:
        yield ref
