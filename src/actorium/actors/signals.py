from asyncio import Future
from typing import TYPE_CHECKING, Never, final

from anyio import sleep
from msgspec import Struct

from actorium.system import spawn
from actorium.utils import TtlSet, generic_class_getitem

from .behaviors import BehaviorActor, behavior, rpc
from .simple import SimpleActor, SimpleRef
from .simple_rpc import RpcRef

__all__ = [
    "Undefined",
    "Signal",
    "SignalReader",
    "SignalWriter",
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


class SignalReader[T](Struct):
    get: RpcRef[T]
    subscribe: SimpleRef[SimpleRef[T], float]
    unsubscribe: SimpleRef[SimpleRef[T]]


class SignalWriter[T](SimpleRef[T]):
    pass


class Signal[T](BehaviorActor):
    __class_getitem__ = generic_class_getitem

    def __init__(self, initial: T | _UndefinedType = Undefined) -> None:
        if not TYPE_CHECKING:
            T = self._args[0]

        self.value = initial
        self.subscriptions: TtlSet[SimpleRef[T]] = TtlSet()
        self.get_waiters: set[Future[T]] = set()

    def actor_ref(self) -> tuple[SignalReader[T], SimpleRef[T]]:
        behavior_ref = super().actor_ref()

        if not TYPE_CHECKING:
            T = self._args[0]

        reader = SignalReader[T](
            get=behavior_ref.get,
            subscribe=behavior_ref.subscribe,
            unsubscribe=behavior_ref.unsubscribe,
        )
        writer = behavior_ref.set
        return reader, writer

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


class SignalSubscribe[T](SimpleActor[Never]):
    def __init__(self, signal_reader: SignalReader[T], reply_to: SimpleRef[T]) -> None:
        self._signal_reader = signal_reader
        self._reply_to = reply_to

    async def actor_run(self) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self._signal_reader.subscribe(self._reply_to, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self._signal_reader.unsubscribe(self._reply_to)


class SignalSubscribeWithId[T](SimpleActor[T]):
    def __init__(
        self,
        signal_reader: SignalReader[T],
        reply_to: SimpleRef[int, T],
        id: int,
    ) -> None:
        self._signal_reader = signal_reader
        self._reply_to = reply_to
        self._id = id

    async def actor_run(self) -> None:
        # Tell signal to send updates here.
        spawn(SignalSubscribe[T], self._signal_reader, self.actor_ref())

        # Forward to `reply_to` address with ID.
        async for (value,) in self.mailbox:
            self._reply_to(self._id, value)
