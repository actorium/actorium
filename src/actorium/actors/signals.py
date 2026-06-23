from asyncio import Future
from typing import TYPE_CHECKING, final

from msgspec import Struct

from actorium.runtime_generic import runtime_generic
from actorium.utils import TtlSet

from .behaviors import BehaviorActor, behavior, rpc
from .simple import SimpleRef
from .simple_rpc import RpcRef

__all__ = [
    "Undefined",
    "Signal",
    "SignalReader",
    "SignalWriter",
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


@runtime_generic
class Signal[T](BehaviorActor):
    def __init__(self, initial: T | _UndefinedType = Undefined) -> None:
        if TYPE_CHECKING:
            t = T
        else:
            t = self._typevar_to_args[T]

        self._value = initial
        self._subscriptions: TtlSet[SimpleRef[t]] = TtlSet()
        self._get_waiters: set[Future[t]] = set()

    def actor_ref(self) -> tuple[SignalReader[T], SimpleRef[T]]:
        behavior_ref = super().actor_ref()

        if TYPE_CHECKING:
            t = T
        else:
            t = self._typevar_to_args[T]

        reader = SignalReader[t](
            get=behavior_ref.get,
            subscribe=behavior_ref.subscribe,
            unsubscribe=behavior_ref.unsubscribe,
        )
        writer = behavior_ref.set
        return reader, writer

    @behavior
    def set(self, value: T) -> None:
        if self._value == value:
            return

        self._value = value

        if len(self._get_waiters) > 0:
            for waiter in self._get_waiters:
                waiter.set_result(value)
            self._get_waiters = set()

        for subscription in self._subscriptions.iter():
            subscription.tell(value)

    @rpc
    async def get(self) -> T:
        # If unknown, wait until a value is set.
        if isinstance(self._value, _UndefinedType):
            f = Future[T]()
            self._get_waiters.add(f)
            return await f

        return self._value

    @behavior
    def subscribe(self, reply_to: SimpleRef[T], ttl_seconds: float) -> None:
        self._subscriptions.add(reply_to, ttl_seconds)
        if not isinstance(self._value, _UndefinedType):
            reply_to.tell(self._value)

    @behavior
    def unsubscribe(self, actor: SimpleRef[T]) -> None:
        self._subscriptions.discard(actor)
