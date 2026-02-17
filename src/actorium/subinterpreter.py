from __future__ import annotations

import multiprocessing
import pickle
from concurrent import interpreters
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Self, cast

from anyio import CancelScope, create_task_group, run, sleep_forever, to_thread
from anyio.abc import TaskGroup
from pydantic import BaseModel

from .actors import ActorClass, ActorRef, spawn
from .reactivity.future import Future
from .system import actor_system

__all__ = [
    "ActorSpawnerActorRef",
]


class ActorSpawnerActor:
    def __init__(self, tg: TaskGroup) -> None:
        self.tg = tg

    async def receive(self, msg: ActorSpawnerActorMsg) -> None:
        if isinstance(msg, StartActor):
            self.tg.start_soon(self._start, msg)

    async def _start(self, msg: StartActor) -> None:
        factory = pickle.loads(msg.factory)
        args = [pickle.loads(arg) for arg in msg.factory_args]
        kwargs = {k: pickle.loads(v) for k, v in msg.factory_kwargs.items()}

        async with spawn(factory, *args, **kwargs) as (_, ref):
            with CancelScope() as cancel_scope:

                class RunningActor:
                    async def receive(self, msg: StopActor) -> None:
                        cancel_scope.cancel()

                async with spawn(RunningActor) as (_, running_ref):
                    msg.ready_reply_to.tell(ref)
                    msg.stop_reply_to.tell(running_ref)
                    await sleep_forever()


def _main_in_subinterpreter(address_return_queue: interpreters.Queue) -> None:
    async def _main() -> None:
        async with actor_system():
            async with create_task_group() as tg:
                async with spawn(ActorSpawnerActor, tg) as (_, ref):
                    address_return_queue.put(ref)
                    await sleep_forever()

    run(_main)


def _main_in_subprocess(address_return_queue: multiprocessing.Queue[str]) -> None:
    async def _main() -> None:
        async with actor_system():
            async with create_task_group() as tg:
                async with spawn(ActorSpawnerActor, tg) as (_, ref):
                    address_return_queue.put(ref.model_dump_json())
                    await sleep_forever()

    run(_main)


class StartActor(BaseModel):
    factory: bytes  # Pickled.
    factory_args: list[bytes]  # Pickled.
    factory_kwargs: dict[str, bytes]  # Pickled.

    ready_reply_to: ActorRef[ActorRef[Any]]
    stop_reply_to: ActorRef[ActorRef[RunningActorMsg]]


class StopActor(BaseModel):
    pass


type ActorSpawnerActorMsg = StartActor  # | StartListenTcp

type RunningActorMsg = StopActor


class ActorSpawnerActorRef(ActorRef[ActorSpawnerActorMsg]):
    def __init__(self, subinterpreter_actor: ActorRef[ActorSpawnerActorMsg]) -> None:
        self._subinterpreter_actor = subinterpreter_actor

    @classmethod
    @asynccontextmanager
    async def create_in_subinterpreter(cls) -> AsyncGenerator[Self]:
        interpreter = interpreters.create()
        address_return_queue = interpreters.create_queue()

        interpreter.call(
            lambda: __import__(
                "actorium.subinterpreter"
            ).subinterpreter._main_in_subinterpreter()
        )  # _main_in_subinterpreter)#, address_return_queue)
        # Wait for a reference.
        actor_ref = cast(
            ActorRef[StartActor], await to_thread.run_sync(address_return_queue.get)
        )

        try:
            yield cls(actor_ref)
        finally:
            interpreter.close()

    @classmethod
    @asynccontextmanager
    async def create_in_subprocess(cls) -> AsyncGenerator[Self]:
        address_return_queue: multiprocessing.Queue[str] = multiprocessing.Queue()

        p = multiprocessing.Process(
            target=_main_in_subprocess, args=(address_return_queue,)
        )
        p.start()
        actor_ref = ActorRef[StartActor].model_validate_json(
            await to_thread.run_sync(address_return_queue.get)
        )

        try:
            yield cls(actor_ref)
        finally:
            p.join()

    @asynccontextmanager
    async def spawn_new[A, T, **P](
        self, factory: ActorClass[A, T, P], *args: P.args, **kwargs: P.kwargs
    ) -> AsyncGenerator[ActorRef[T]]:
        async with (
            spawn(Future[ActorRef[T]]) as (ready_future, ready_reply_to),
            spawn(Future[ActorRef[RunningActorMsg]]) as (stop_future, stop_reply_to),
        ):
            # Tell the subprocess to start a new actor.
            self._subinterpreter_actor.tell(
                StartActor(
                    factory=pickle.dumps(factory),
                    factory_args=[pickle.dumps(arg) for arg in args],
                    factory_kwargs={k: pickle.dumps(v) for k, v in kwargs.items()},
                    ready_reply_to=ready_reply_to,
                    stop_reply_to=stop_reply_to,
                )
            )
            running_actor: ActorRef[T] = await ready_future.result()
            stop_actor: ActorRef[RunningActorMsg] = await stop_future.result()
            try:
                yield running_actor
            finally:
                stop_actor.tell(StopActor())
