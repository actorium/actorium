from asyncio import Future
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, ClassVar, Never, final, get_args

from anyio import sleep
from typing_extensions import TypeForm

from actorium.system import spawn
from actorium.types import ActorAddress
from actorium.utils import TtlSet

from .behaviors import BehaviorActor, BehaviorRef, behavior, rpc
from .simple import Mailbox, SimpleActor, SimpleRef

__all__ = [
    "Undefined",
    "Signal",
    "SignalRef",
    "SignalSubscribe",
    "SignalSubscribeWithId",
]


@final
class _UndefinedType:
    """
    Sentinel value for a signal that has not yet a value set.
    `get()` calls will block until the first `set()`.
    """


Undefined = _UndefinedType()


class _SubscriptionWithId[T](BehaviorActor):
    "Subscription interface."

    @behavior
    def changed(self, id: int, value: T) -> None: ...


class Signal[T](BehaviorActor):
    def __init__(self, initial: T | _UndefinedType = Undefined) -> None:
        self.value = initial
        self.subscriptions: TtlSet[SimpleRef[T]] = TtlSet()
        self.get_waiters: set[Future[T]] = set()

    def actor_ref(self, actor_address: ActorAddress) -> SignalRef[T]:
        if not TYPE_CHECKING:
            T = get_args(self.__orig_class__)[0]
        return SignalRef[T](actor_address=actor_address)

    @behavior
    def set(self, value: T) -> None:
        if self.value == value:
            return

        self.value = value

        if len(self.get_waiters) > 0:
            for waiter in self.get_waiters:
                waiter.set_result(value)
            self.get_waiters = set()

        for subscription in self.subscriptions.iter():
            subscription.tell(value)

    @rpc
    async def get(self) -> T:
        # If unknown, wait until a value is set.
        if isinstance(self.value, _UndefinedType):
            f = Future[T]()
            self.get_waiters.add(f)
            return await f

        return self.value

    @behavior
    def subscribe(self, actor: SimpleRef[T], ttl_seconds: float) -> None:
        self.subscriptions.add(actor, ttl_seconds)
        if not isinstance(self.value, _UndefinedType):
            actor.tell(self.value)

    @behavior
    def unsubscribe(self, actor: SimpleRef[T]) -> None:
        self.subscriptions.discard(actor)


@dataclass(frozen=True)
class SignalRef[T](BehaviorRef[Signal[T]]):
    """
    Reference to a `signal` actor with helper methods for getting, setting or
    subscribing to the state.
    """

    _t: ClassVar[TypeForm[T] | None] = None

    # __orig_class__ is not available in __init__, so we use __class_getitem__
    # as a workaround.
    @cache
    @staticmethod
    def __class_getitem__(item: TypeForm[T]) -> type:
        class _SignalRef(SignalRef):  # type: ignore
            _t = item
            _a = Signal[item]  # type: ignore

        return _SignalRef

    async def get(self, *, timeout: float | None = None) -> T:
        # NOTE: type:ignore because of a mypy bug.
        #       inference of `self.be` is wrong at this point.
        return await self.rpc.get(timeout=timeout)  # type: ignore

    def set(self, value: T) -> None:
        self.be.set(value)


class SignalSubscribe[T](SimpleActor[Never]):
    def __init__(self, signal_ref: SignalRef[T], reply_to: SimpleRef[T]) -> None:
        self.signal_ref = signal_ref
        self.reply_to = reply_to

    async def run(self, mailbox: Mailbox[Never]) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self.signal_ref.be.subscribe(self.reply_to, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self.signal_ref.be.unsubscribe(self.reply_to)


class SignalSubscribeWithId[T](SimpleActor[T]):
    def __init__(
        self,
        signal_ref: SignalRef[T],
        reply_to: BehaviorRef[_SubscriptionWithId[T]],
        id: int,
    ) -> None:
        self._signal_ref = signal_ref
        self._reply_to = reply_to
        self._id = id

    async def run(self, mailbox: Mailbox[T]) -> None:
        # Tell signal to send updates here.
        spawn(SignalSubscribe, self._signal_ref, mailbox.ref())

        # Forward to `reply_to` address with ID.
        async for value in mailbox:
            self._reply_to.be.changed(self._id, value)
