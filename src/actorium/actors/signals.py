from asyncio import Future
from typing import Never, final

from anyio import sleep

from actorium.system import spawn
from actorium.utils import TtlSet

from .behaviors import BehaviorActor, BehaviorRef, behavior, rpc
from .simple import SimpleActor, SimpleRef

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


class Signal[T](BehaviorActor):
    def __init__(self, initial: T | _UndefinedType = Undefined) -> None:
        self.value = initial
        self.subscriptions: TtlSet[SimpleRef[T]] = TtlSet()
        self.get_waiters: set[Future[T]] = set()

    #    def actor_ref(self) -> SignalRef[T]:
    #        if not TYPE_CHECKING:
    #            T = get_args(self.__orig_class__)[0]
    #        return SignalRef[T](
    #            behavior_addresses=self._behavior_addresses,
    #            rpc_addresses=self._rpc_addresses,
    #        )
    #        return SignalRef[T](set=self.actors['set'])

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


type SignalRef[T] = BehaviorRef[Signal[T]]
# @dataclass(frozen=True)
# class SignalRef[T](BehaviorRef[Signal[T]]):
#    """
#    Reference to a `signal` actor with helper methods for getting, setting or
#    subscribing to the state.
#    """
#
#    _t: ClassVar[TypeForm[T] | None] = None
#
#    # __orig_class__ is not available in __init__, so we use __class_getitem__
#    # as a workaround.
#    @cache
#    @staticmethod
#    def __class_getitem__(item: TypeForm[T]) -> type:
#        return type(
#            f"SignalRef[{item.__name__}]",
#            (SignalRef,),
#            {
#                "_t": item,
#                "_a": Signal[item],  # type: ignore[valid-type]
#            },
#        )
#
#    async def get(self, *, timeout: float | None = None) -> T:
#        # NOTE: type:ignore because of a mypy bug.
#        #       inference of `self.be` is wrong at this point.
#        return await self.rpc.get(timeout=timeout)  # type: ignore


class SignalSubscribe[T](SimpleActor[Never]):
    def __init__(self, signal_ref: SignalRef[T], reply_to: SimpleRef[T]) -> None:
        self.signal_ref = signal_ref
        self.reply_to = reply_to

    async def actor_run(self) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self.signal_ref.subscribe(self.reply_to, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self.signal_ref.unsubscribe(self.reply_to)


class SignalSubscribeWithId[T](SimpleActor[T]):
    def __init__(
        self,
        signal_ref: SignalRef[T],
        reply_to: SimpleRef[int, T],
        id: int,
    ) -> None:
        self._signal_ref = signal_ref
        self._reply_to = reply_to
        self._id = id

    async def actor_run(self) -> None:
        # Tell signal to send updates here.
        spawn(SignalSubscribe[T], self._signal_ref, self.actor_ref())

        # Forward to `reply_to` address with ID.
        async for (value,) in self.mailbox:
            self._reply_to(self._id, value)
