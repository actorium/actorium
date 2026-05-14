from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Literal, assert_never, final, get_args

from pydantic import BaseModel

from ..core import Actor, ActorAddress, Mailbox, Ref, spawn
from .future import Future

__all__ = [
    # Messages.
    "Subscribe",
    "Unsubscribe",
    "Get",
    "SignalMsg",
    # Ref.
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


@final
class _Unknown:
    "Sentinel for when the value of a signal is still unknown."


class _SignalActor[T](Actor[SignalMsg[T]]):
    def __init__(self, initial: T | _Unknown) -> None:
        self.value = initial
        self.subscriptions: set[Ref[T]] = set()
        self.get_waiters: set[Ref[T]] = set()

    def data_type(self) -> type[T]:
        return get_args(self.__orig_class__)[0]  # type:ignore

    def actor_ref(self, actor_address: ActorAddress) -> signal[T]:
        if not TYPE_CHECKING:
            T = self.data_type()
        return signal[T](actor_address=actor_address)

    def message_type(self) -> SignalMsgType[T]:
        return SignalMsg[self.data_type()]  # type:ignore

    def _set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        if len(self.get_waiters) > 0:
            for reply_to in self.get_waiters:
                reply_to.tell(value)
            self.get_waiters = set()

        for subscription in self.subscriptions:
            subscription.tell(value)

    async def run(self, mailbox: Mailbox[SignalMsg[T]]) -> None:
        async for msg in mailbox:
            match msg:
                case Get(reply_to=reply_to):
                    # If unknown, wait until a value is set.
                    if isinstance(self.value, _Unknown):
                        self.get_waiters.add(reply_to)
                    else:
                        reply_to.tell(self.value)
                case Set(value=value):
                    self._set(value)
                case Subscribe(actor=actor):
                    self.subscriptions.add(actor)
                    if not isinstance(self.value, _Unknown):
                        actor.tell(self.value)
                case Unsubscribe(actor=actor):
                    self.subscriptions.discard(actor)
                case _ as x:
                    assert_never(x)


class signal[T](Ref[SignalMsg[T]]):
    """
    Reference to a `signal` actor with helper methods for getting, setting or
    subscribing to the state.
    """

    @classmethod
    def new(
        cls,
        initial: T | _Unknown = _Unknown(),
        name: str | None = None,
    ) -> signal[T]:
        if not TYPE_CHECKING:
            T = cls.data_type()

        return spawn(_SignalActor[T], initial, name=name)

    async def get(self) -> T:
        if not TYPE_CHECKING:
            T = self.data_type()

        f = Future[T]()
        self.tell(Get[T](reply_to=f.actor))
        return await f.result()

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
