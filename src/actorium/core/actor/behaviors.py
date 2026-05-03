from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Literal,
    Self,
    cast,
)

from anyio import fail_after
from pydantic import BaseModel, TypeAdapter
from typemap_extensions import (
    Attrs,
    GetMemberType,
    IsAssignable,
    Iter,
    Member,
    NewProtocol,
)

from ..types import ActorAddress, Timeout
from .base import BaseActor, RawMailbox
from .pydantic import Ref

__all__ = [
    "BehaviorActor",
    "behavior",
    "BehaviorRef",
]


class _BehaviorMethod[A: BehaviorActor, *I, O]:
    def __init__(self, func: Callable[[A, *I], Coroutine[Any, Any, O]]) -> None:
        self._func = func

        input_param_names = list(inspect.signature(func).parameters)[1:]

        input_type = tuple[*(func.__annotations__[name] for name in input_param_names)]  # type: ignore
        output_type = func.__annotations__["return"]

        self.input_adapter = TypeAdapter[tuple[*I]](input_type)
        self.output_adapter = TypeAdapter[O](output_type)

    async def call(self, behavior_actor: A, *param: *I) -> O:
        return await self._func(behavior_actor, *param)

    if TYPE_CHECKING:
        # Define a `behavior_method` for inclusion in the `BehaviorRefMethods`
        # `NewProtocol`.

        @staticmethod
        async def behavior_method(
            *param: *I, timeout: float | None = None
        ) -> O | Timeout:
            raise NotImplementedError


def behavior[A: BehaviorActor, *I, O](
    method: Callable[[A, *I], Coroutine[Any, Any, O]],
) -> _BehaviorMethod[A, *I, O]:
    """
    Decorator for annotating methods of a `BehaviorActor`, exposing them
    publicly as an RPC method of the actor.
    """
    return _BehaviorMethod(method)


class _BehaviorMessage(BaseModel):
    """
    Message type sent from `BehaviorRef` to `BehaviorActor`.
    """

    behavior_name: str
    serialized_input: str  # Behavior input, JSON serialized.
    reply_to: Ref[str]  # Actor address for receiving the JSON serialized output.


class BehaviorActor(BaseActor):
    """
    Actor implementation that works by defining multiple `@behavior` decorated
    methods. Those behaviors can be called from the `BehaviorRef` proxy,
    through the `be` (short for "behaviors") attribute.

    E.g.::

        class Calc(BehaviorActor):
            @behavior
            async def double_it(self, value: int) -> int:
                return value * 2

        # Then, in another actor:
        async with spawn(Calc) as ref:
            result = await ref.be.double_it(4)
    """

    _behavior_methods_: ClassVar[Mapping[str, _BehaviorMethod[Any, Any, Any]]]

    def actor_ref(self, actor_address: ActorAddress) -> BehaviorRef[Self]:
        if not TYPE_CHECKING:
            return BehaviorRef[type(self)](actor_address=actor_address)
        else:
            return BehaviorRef[Self](actor_address=actor_address)

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        # Collect all behavior methods.
        behaviors: dict[str, _BehaviorMethod[Self, Any, Any]] = {}

        for name in dir(cls):
            try:
                value = getattr(cls, name)
            except AttributeError:
                continue
            if isinstance(value, _BehaviorMethod):
                behaviors[name] = value

        cls._behavior_methods_ = behaviors

    async def actor_run(
        self, raw_mailbox: RawMailbox, actor_address: ActorAddress
    ) -> None:
        async for msg in raw_mailbox:
            behavior_message = _BehaviorMessage.model_validate_json(msg)

            try:
                method = self._behavior_methods_[behavior_message.behavior_name]
            except KeyError:
                print("Behavior not found")
            else:
                input_data = method.input_adapter.validate_json(
                    behavior_message.serialized_input
                )
                output_data = await method.call(self, *input_data)

                # Return result.
                behavior_message.reply_to.tell(
                    method.output_adapter.dump_json(output_data).decode()
                )


class BehaviorRef[A: BehaviorActor](BaseModel):
    """
    Actor reference for any behavior actor.
    """

    model_config = {"frozen": True}

    # Discriminator, for when it's used in a union with other types.
    type_: Literal["behavior-actor-ref"] = "behavior-actor-ref"

    actor_address: ActorAddress

    @property
    def be(self) -> BehaviorRefMethods[A]:
        """
        Remote access to all behavior methods of the `BehaviorActor`.
        """
        try:
            actor_cls = self.__pydantic_generic_metadata__["args"][0]
        except IndexError:
            actor_cls = self.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type: ignore

        return cast(
            BehaviorRefMethods[A],
            _RuntimeBehaviorRefMethods[A](
                self.actor_address, behavior_methods=actor_cls._behavior_methods_
            ),
        )


type BehaviorRefMethods[A: BehaviorActor] = NewProtocol[
    *[
        # Take the `behavior_method` from the `_BehaviorMethod` attributes from
        # a `BehaviorActor`.
        Member[p.name, GetMemberType[p.type, Literal["behavior_method"]]]
        for p in Iter[Attrs[A]]
        if IsAssignable[p.type, _BehaviorMethod[Any, Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any, Any, Any]]
    ]
]


class _RuntimeBehaviorRefMethods[A: BehaviorActor]:
    def __init__(
        self,
        actor_address: ActorAddress,
        behavior_methods: dict[str, _BehaviorMethod[Any, Any, Any]],
    ) -> None:
        self.actor_address = actor_address
        self.behavior_methods = behavior_methods

    def __dir__(self) -> list[str]:
        return [name for name in self.behavior_methods.keys()]

    def __getattr__(self, name: str) -> Callable[[Any], Coroutine[Any, Any, Any]]:
        from actorium.actors.future import future

        from ..system import _get_system

        behavior_method = self.behavior_methods[name]

        async def call_behavior[*I, O](
            *params: *I, timeout: float | None = None
        ) -> O | Timeout:

            async with future[str]() as (fut, fut_ref):
                serialized_message = _BehaviorMessage(
                    behavior_name=name,
                    serialized_input=behavior_method.input_adapter.dump_json(params),  # type:ignore
                    reply_to=fut_ref,
                )
                _get_system().call_actor_threadsafe(
                    self.actor_address, serialized_message.model_dump_json()
                )

                try:
                    with fail_after(timeout):
                        serialized_return_value = await fut
                except TimeoutError:
                    return Timeout()

                result = behavior_method.output_adapter.validate_json(
                    serialized_return_value
                )
                return cast(O, result)

        return call_behavior
