from typing import Never, Protocol

from anyio import sleep

from actorium.system import spawn

from .simple import SimpleActor, SimpleRef

__all__ = [
    "Subscribe",
    "SubscribeWithId",
]


class HasSubscribeBehaviors[T](Protocol):
    subscribe: SimpleRef[SimpleRef[T], float]
    unsubscribe: SimpleRef[SimpleRef[T]]


class Subscribe[T](SimpleActor[Never]):
    """
    Subscribe to the state of the given actor, and forward it to the
    `reply_to` target.

    This works for `Signal` and `Registry`.
    """

    def __init__(self, ref: HasSubscribeBehaviors[T], reply_to: SimpleRef[T]) -> None:
        self._ref = ref
        self._reply_to = reply_to

    async def actor_run(self) -> None:
        ttl_seconds = 10.0
        sleep_seconds = 5.0

        try:
            while True:
                self._ref.subscribe(self._reply_to, ttl_seconds)
                await sleep(sleep_seconds)
        finally:
            # Immediately unpublish when this actor is terminated.
            self._ref.unsubscribe(self._reply_to)


class SubscribeWithId[T](SimpleActor[T]):
    def __init__(
        self,
        ref: HasSubscribeBehaviors[T],
        reply_to: SimpleRef[int, T],
        id: int,
    ) -> None:
        self._ref = ref
        self._reply_to = reply_to
        self._id = id

    async def actor_run(self) -> None:
        # Tell signal to send updates here.
        spawn(Subscribe[T], self._ref, self.actor_ref())

        # Forward to `reply_to` address with ID.
        async for (value,) in self.mailbox:
            self._reply_to(self._id, value)
