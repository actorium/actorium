import inspect
import typing
from functools import cache
from types import GenericAlias
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Self,
    get_args,
    get_type_hints,
)

from anyio import create_task_group
from msgspec import Struct
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox
from actorium.utils import generic_class_getitem, substitute_type

from .simple import Mailbox, SimpleActor, SimpleRef
from .simple_rpc import RpcMailbox, RpcMessage, RpcRef

__all__ = [
    "BehaviorActor",
    "behavior",
    "BehaviorRef",
    "rpc",
]


class _BehaviorMethod[*I]:
    def __init__(self, func: Callable[[BehaviorActor, *I], None]) -> None:
        self._func = func

        input_param_names = list(inspect.signature(func).parameters)[1:]

        self.input_types: list[TypeForm[Any]] = [
            func.__annotations__[name] for name in input_param_names
        ]

        # TODO: this is not accurate! We should test whether the last param is called 'reply_to'.
        self.is_rpc_behavior = (
            len(input_param_names) > 0 and input_param_names[-1] == "reply_to"
        )

    def call(self, behavior_actor: BehaviorActor, *param: *I) -> None:
        self._func(behavior_actor, *param)

    if TYPE_CHECKING:
        # Define a `behavior_method` for inclusion in the `BehaviorRefMethods`
        # `NewProtocol`.

        behavior_signature: SimpleActor[*I]

    # TODO: cache!!

    def get_input_types(self, orig_class: type) -> list[TypeForm[Any]]:
        return [
            substitute_type(input_type, orig_class) for input_type in self.input_types
        ]


class _RpcMethod[*I, O]:
    def __init__(
        self, func: Callable[[BehaviorActor, *I], Coroutine[Any, Any, O]]
    ) -> None:
        self._func = func

        input_param_names = list(inspect.signature(func).parameters)[1:]

        self.input_types: list[TypeForm[Any]] = [
            func.__annotations__[name] for name in input_param_names
        ]
        self.output_type: TypeForm[O] = get_type_hints(self._func)["return"]

    async def call(self, behavior_actor: BehaviorActor, *param: *I) -> O:
        return await self._func(behavior_actor, *param)

    if TYPE_CHECKING:
        # Define a `behavior_method` for inclusion in the `BehaviorRefMethods`
        # `NewProtocol`.

        rpc_method: RpcRef[*I, O]

    # TODO: cache!!

    def get_input_types(self, orig_class: type) -> list[TypeForm[tuple[Any]]]:
        return [
            substitute_type(input_type, orig_class) for input_type in self.input_types
        ]

    def get_output_type(self, orig_class: type) -> TypeForm[O]:
        return substitute_type(self.output_type, orig_class)


def behavior[*I](method: Callable[[Any, *I], None]) -> _BehaviorMethod[*I]:
    """
    Decorator for annotating methods of a `BehaviorActor`, exposing them
    publicly as a behavior method of the actor.
    """
    return _BehaviorMethod(method)


def rpc[*I, O](
    method: Callable[[Any, *I], Coroutine[Any, Any, O]],
) -> _RpcMethod[*I, O]:
    """
    Create behavior that can return a response through a `SimpleRef[O]`.
    """
    return _RpcMethod(method)


class BehaviorActor(BaseActor):
    """
    Actor implementation that works by defining multiple `@behavior` or `@rpc`
    decorated methods. Those behaviors and RPC methods can be called from the
    `BehaviorRef` proxy. The difference between a "behavior" and "rpc" is that
    a "behavior" is fire-and-forget, while for "rpc" we can return a value.

    E.g.::

        class Calc(BehaviorActor):
            @behavior
            def say_hello(self, name: str) -> None:
                print(f"Hello, {name}")

            @rpc
            def double_it(self, value: int) -> None:
                return value * 2

        # Then, in another actor:
        ref = spawn(Calc)

        # Behaviors are called without 'await'. They are fire-and-forget.
        ref.say_hello("john")

        # RPC calls need to be called using await.
        result = await ref.rpc.double_it(4)
    """

    # Better support for generic behavior actors:
    __class_getitem__ = generic_class_getitem

    def actor_post_init(self, create_mailbox: Callable[[], RawMailbox]) -> None:
        # Create one mailbox for each 'behavior'.
        behavior_methods = get_behaviors_from_class(self.__class__)
        rpc_methods = get_rpc_methods_from_class(self.__class__)

        behavior_mailboxes = {}
        rpc_mailboxes = {}
        behavior_addresses = {}
        rpc_addresses = {}

        for name, be_method in behavior_methods.items():
            types = be_method.get_input_types(self.__class__)
            raw_mailbox = create_mailbox()
            behavior_mailbox = Mailbox[*types](tuple[*types], raw_mailbox)  # type:ignore[valid-type]
            behavior_mailboxes[name] = behavior_mailbox
            behavior_addresses[name] = raw_mailbox.address

        for name, rpc_method in rpc_methods.items():
            in_types = rpc_method.get_input_types(self.__class__)
            out_type = rpc_method.get_output_type(self.__class__)

            raw_mailbox = create_mailbox()
            rpc_mailbox = RpcMailbox[tuple[*in_types], out_type](  # type:ignore[valid-type]
                RpcMessage[tuple[*in_types], out_type],  # type:ignore[valid-type]
                raw_mailbox,
            )
            rpc_mailboxes[name] = rpc_mailbox
            rpc_addresses[name] = raw_mailbox.address

        self._behavior_addresses = behavior_addresses
        self._behavior_mailboxes = behavior_mailboxes
        self._rpc_addresses = rpc_addresses
        self._rpc_mailboxes = rpc_mailboxes

    def actor_ref(self) -> BehaviorRef[Self]:
        if not TYPE_CHECKING:
            if hasattr(self, "__orig_class__"):
                Self = self.__orig_class__
            else:
                Self = type(self)

        fields = {}
        orig_actor_cls = self.__class__

        for name, be_method in get_behaviors_from_class(self.__class__).items():
            fields[name] = SimpleRef[*be_method.get_input_types(orig_actor_cls)](  # type: ignore
                actor_address=self._behavior_addresses[name],
            )

        for name, rpc_method in get_rpc_methods_from_class(self.__class__).items():
            fields[name] = RpcRef[  # type: ignore[misc,operator]
                *rpc_method.get_input_types(orig_actor_cls),
                rpc_method.get_output_type(orig_actor_cls),
            ](
                actor_address=self._rpc_addresses[name],
            )

        return BehaviorRef[Self](**fields)

    async def actor_init(self) -> None:
        "Placeholder for setup code for behavior actors."

    async def actor_run(self) -> None:
        behavior_methods = get_behaviors_from_class(self.__class__)
        rpc_methods = get_rpc_methods_from_class(self.__class__)

        async def handle_behavior[*I](
            method: _BehaviorMethod[*I], mailbox: Mailbox[*I]
        ) -> None:
            async for args in mailbox:
                method.call(self, *args)

        async def handle_rpc[*I, O](
            method: _RpcMethod[*I, O], mailbox: RpcMailbox[tuple[*I], O]
        ) -> None:
            async for rpc_message in mailbox:
                result: O = await method.call(self, *rpc_message.inputs)

                # Return result.
                rpc_message.reply_to.tell(result)

        # Receive messages and call corresponding behaviors.
        async with create_task_group() as tg:
            # Call setup function: Note that the setup function might call
            # other actors that respond to our mailbox, so we should consume
            # the mailbox here while the setup function is running.
            tg.start_soon(self.actor_init)

            for name, be_method in behavior_methods.items():
                tg.start_soon(
                    handle_behavior, be_method, self._behavior_mailboxes[name]
                )
            for name, rpc_method in rpc_methods.items():
                tg.start_soon(handle_rpc, rpc_method, self._rpc_mailboxes[name])


def get_behaviors_from_class(
    cls: TypeForm[BehaviorActor], only_rpc: bool = False
) -> dict[str, _BehaviorMethod[Any, Any]]:
    """
    Derive behavior methods for `BehaviorActor` class.
    """

    if isinstance(cls, GenericAlias):
        cls = typing.get_origin(cls)

    # Collect all behavior methods.
    behaviors: dict[str, _BehaviorMethod[Any, Any]] = {}

    for name in dir(cls):
        try:
            value = getattr(cls, name)
        except AttributeError:
            continue
        if isinstance(value, _BehaviorMethod):
            if only_rpc and not value.is_rpc_behavior:
                continue
            behaviors[name] = value

    return behaviors


def get_rpc_methods_from_class(
    cls: TypeForm[BehaviorActor], only_rpc: bool = False
) -> dict[str, _RpcMethod[Any, Any]]:
    if isinstance(cls, GenericAlias):
        cls = typing.get_origin(cls)

    # Collect all behavior methods.
    behaviors: dict[str, _RpcMethod[Any, Any]] = {}

    for name in dir(cls):
        try:
            value = getattr(cls, name)
        except AttributeError:
            continue
        if isinstance(value, _RpcMethod):
            behaviors[name] = value

    return behaviors


if TYPE_CHECKING:
    # from typing import Literal
    # from typemap_extensions import (
    #     Attrs,
    #     GetMemberType,
    #     IsAssignable,
    #     Iter,
    #     Member,
    #     NewProtocol,
    # )
    # type BehaviorRef[A: BehaviorActor] = NewProtocol[
    #     *[
    #         # Take the `behavior_method` from the `_BehaviorMethod` attributes from
    #         # a `BehaviorActor`.
    #         Member[p.name, GetMemberType[p.type, Literal["behavior_signature"]]]
    #         for p in Iter[Attrs[A]]
    #         if IsAssignable[p.type, _BehaviorMethod[Any, Any]]
    #         or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any]]
    #         or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any]]
    #         or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any, Any]]
    #     ],
    #     *[
    #         # Take the `rpc_method` from the `_RpcMethod` attributes from
    #         # a `BehaviorActor`.
    #         Member[p.name, GetMemberType[p.type, Literal["rpc_method"]]]
    #         for p in Iter[Attrs[A]]
    #         if IsAssignable[p.type, _RpcMethod[Any, Any, Any]]
    #         or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any]]
    #         or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any, Any]]
    #         or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any, Any, Any]]
    #     ],
    # ]

    # Simplified type definition for mypy - the plugin will handle the details
    class BehaviorRef[A: BehaviorActor]:
        """
        Type stub for BehaviorRef. The actual attributes are provided by the mypy plugin
        based on the @behavior and @rpc methods of the actor class A.
        """

        def __getattr__(self, name: str) -> Any: ...

else:

    class BehaviorRef[A: BehaviorActor]:
        @classmethod
        @cache
        def __class_getitem__(cls, behavior_actor: type[A]) -> type[BehaviorRef[A]]:
            actor = behavior_actor
            annotations = {}

            for name, be_method in get_behaviors_from_class(actor).items():
                annotations[name] = SimpleRef[*be_method.get_input_types(actor)]

            for name, rpc_method in get_rpc_methods_from_class(actor).items():
                annotations[name] = RpcRef[
                    *rpc_method.get_input_types(actor),
                    rpc_method.get_output_type(actor),
                ]

            fields = {"__module__": cls.__module__, "__annotations__": annotations}

            args = get_args(actor)
            if len(args) > 0:
                name = f"BehaviorRef[{actor.__name__}[{', '.join(map(str, get_args(actor)))}]]"
            else:
                name = f"BehaviorRef[{actor.__name__}[{get_args(actor)}]]"

            return type(name, (Struct,), fields)
