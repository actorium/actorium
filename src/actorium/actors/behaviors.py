import inspect
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from types import GenericAlias
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Literal,
    Self,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from anyio import create_task_group, fail_after
from pydantic import BaseModel, TypeAdapter, model_serializer, model_validator
from typemap_extensions import (
    Attrs,
    GetMemberType,
    IsAssignable,
    Iter,
    Member,
    NewProtocol,
)
from typing_extensions import TypeForm

from actorium.actor import BaseActor, RawMailbox, SerializedMessage
from actorium.system import _get_system
from actorium.types import ActorAddress

from .simple import SimpleRef

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

        self.input_type: TypeForm[tuple[*I]] = tuple[  # type: ignore
            *(func.__annotations__[name] for name in input_param_names)
        ]

        # TODO: this is not accurate! We should test whether the last param is called
        self.is_rpc_behavior = (
            len(input_param_names) > 0 and input_param_names[-1] == "reply_to"
        )

    def call(self, behavior_actor: BehaviorActor, *param: *I) -> None:
        self._func(behavior_actor, *param)

    if TYPE_CHECKING:
        # Define a `behavior_method` for inclusion in the `BehaviorRefMethods`
        # `NewProtocol`.

        @staticmethod
        def behavior_method(*param: *I) -> None:
            raise NotImplementedError

    # TODO: cache!!

    def get_behavior_message_type(
        self, behavior_name: str, orig_class: type
    ) -> type[_BehaviorMessage[Any, Any]]:
        return _BehaviorMessage[  # type: ignore
            Literal[behavior_name],  # type: ignore
            self.get_input_types(orig_class),
        ]

    def get_input_types(self, orig_class: type) -> TypeForm[tuple[Any, ...]]:
        origin = get_origin(orig_class)

        if origin is None:
            if len(orig_class.__type_params__) > 0:
                raise RuntimeError("Type parameters required but not given.")

            return self.input_type

        return _substitute_type(
            self.input_type, origin.__type_params__, get_args(orig_class)
        )


class _RpcMethod[*I, O]:
    def __init__(
        self, func: Callable[[BehaviorActor, *I], Coroutine[Any, Any, O]]
    ) -> None:
        self._func = func

        input_param_names = list(inspect.signature(func).parameters)[1:]

        self.input_type: TypeForm[tuple[*I]] = tuple[  # type: ignore
            *(func.__annotations__[name] for name in input_param_names)
        ]
        self.output_type: TypeForm[O] = get_type_hints(self._func)["return"]

    async def call(self, behavior_actor: BehaviorActor, *param: *I) -> O:
        return await self._func(behavior_actor, *param)

    if TYPE_CHECKING:
        # Define a `behavior_method` for inclusion in the `BehaviorRefMethods`
        # `NewProtocol`.

        @staticmethod
        async def rpc_method(*param: *I, timeout: float | None = None) -> O:
            raise NotImplementedError

    # TODO: cache!!

    def get_rpc_message_type(
        self, behavior_name: str, orig_class: type
    ) -> type[_RpcMessage[Any, Any, Any]]:
        return _RpcMessage[  # type: ignore
            Literal[behavior_name],  # type: ignore
            self.get_input_types(orig_class),
            self.get_output_type(orig_class),
        ]

    def get_input_types(self, orig_class: type) -> TypeForm[tuple[Any, ...]]:
        origin = get_origin(orig_class)

        if origin is None:
            if len(orig_class.__type_params__) > 0:
                raise RuntimeError("Type parameters required but not given.")

            return self.input_type

        return _substitute_type(
            self.input_type, origin.__type_params__, get_args(orig_class)
        )

    def get_output_type(self, orig_class: type) -> TypeForm[O]:
        origin = get_origin(orig_class)

        if origin is None:
            if len(orig_class.__type_params__) > 0:
                raise RuntimeError("Type parameters required but not given.")

            return self.output_type

        return _substitute_type(
            self.output_type, origin.__type_params__, get_args(orig_class)
        )


def behavior[*I](method: Callable[[Any, *I], None]) -> _BehaviorMethod[*I]:
    """
    Decorator for annotating methods of a `BehaviorActor`, exposing them
    publicly as an RPC method of the actor.
    """
    return _BehaviorMethod(method)


def rpc[*I, O](
    method: Callable[[Any, *I], Coroutine[Any, Any, O]],
) -> _RpcMethod[*I, O]:
    """
    Create behavior that can return a response through a `SimpleRef[O]`.
    """
    return _RpcMethod(method)


class _BehaviorMessage[N: str, I: tuple[Any, ...]](BaseModel):
    """
    Message type sent from `BehaviorRef` to `BehaviorActor`.
    """

    # Method name, also used as discriminator.
    behavior_name: N

    # Behavior input, tuple[] of arguments.
    inputs: I


class _RpcMessage[N: str, I: tuple[Any, ...], O](BaseModel):
    """
    Message type sent from `BehaviorRef` to `BehaviorActor`.
    """

    # Method name, also used as discriminator.
    rpc_name: N

    # Behavior input, tuple[] of arguments.
    inputs: I

    # RPC output, actor address where the response is sent to.
    reply_to: SimpleRef[O]


class BehaviorActor(BaseActor):
    """
    Actor implementation that works by defining multiple `@behavior` or `@rpc`
    decorated methods. Those behaviors and RPC methods can be called from the
    `BehaviorRef` proxy, through respectively the `be` (short for "behaviors")
    and `rpc` attribute. The difference between a "behavior" and "rpc" is that
    a "behavior" is fire-and-forget", while for "rpc" we can return a value.

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
        ref.be.say_hello("john")

        # RPC calls need to be called using await.
        result = await ref.rpc.double_it(4)
    """

    _behavior_methods_: ClassVar[Mapping[str, _BehaviorMethod[Any, Any]]]

    def actor_ref(self, actor_address: ActorAddress) -> BehaviorRef[Self]:
        if not TYPE_CHECKING:
            if hasattr(self, "__orig_class__"):
                Self = self.__orig_class__
            else:
                Self = type(self)

        return BehaviorRef[Self](actor_address=actor_address)

    async def actor_init(self, ref: BehaviorRef[Self]) -> None:
        "Placeholder for setup code for behavior actors."

    async def actor_run(
        self, raw_mailbox: RawMailbox, actor_address: ActorAddress
    ) -> None:
        behavior_methods = get_behaviors_from_class(self.__class__)
        rpc_methods = get_rpc_methods_from_class(self.__class__)

        # Construct input type, combining all inputs/outputs from the behaviors
        # that are defined here.
        orig_class = (
            self.__orig_class__ if hasattr(self, "__orig_class__") else self.__class__
        )

        input_type: TypeForm[Any] = Union[  # type:ignore[assignment]
            *[
                method.get_behavior_message_type(name, orig_class)
                for name, method in behavior_methods.items()
            ],
            *[
                method.get_rpc_message_type(name, orig_class)
                for name, method in rpc_methods.items()
            ],
        ]

        # Wrap the `RawMailbox` in a `Mailbox` so that we can consume
        # deserialized messages.
        type_adapter: TypeAdapter[
            _BehaviorMessage[Any, Any] | _RpcMessage[Any, Any, Any]
        ] = TypeAdapter(input_type)

        async def handle_one_msg[*I, O](
            method: _RpcMethod[*I, O],
            msg: _RpcMessage[Any, tuple[*I], O],
        ) -> None:
            output_data = await method.call(self, *msg.inputs)

            # Return result.
            msg.reply_to.tell(output_data)

        # Receive messages and call corresponding behaviors.
        async with create_task_group() as tg:
            # Call setup function: Note that the setup function might call
            # other actors that respond to our mailbox, so we should consume
            # the mailbox here while the setup function is running.
            tg.start_soon(self.actor_init, self.actor_ref(actor_address))

            async for msg in raw_mailbox:
                if isinstance(msg, SerializedMessage):
                    message: _BehaviorMessage[Any, Any] | _RpcMessage[Any, Any, Any] = (
                        type_adapter.validate_json(msg.data)
                    )
                else:
                    message = cast(
                        _BehaviorMessage[Any, Any] | _RpcMessage[Any, Any, Any], msg
                    )

                if isinstance(message, _BehaviorMessage):
                    try:
                        method = behavior_methods[message.behavior_name]
                    except KeyError:
                        print("Behavior not found")
                    else:
                        method.call(self, *message.inputs)

                elif isinstance(message, _RpcMessage):
                    try:
                        rpc_method = rpc_methods[message.rpc_name]
                    except KeyError:
                        print("RPC not found")
                    else:
                        tg.start_soon(handle_one_msg, rpc_method, message)


def get_behaviors_from_class(
    cls: TypeForm[BehaviorActor], only_rpc: bool = False
) -> dict[str, _BehaviorMethod[Any, Any]]:
    """
    When defining a new `BehaviorActor` class, collect all `@behavior`
    decorated methods under the `_behavior_methods_` class attribute for
    runtime introspection.
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


@dataclass(frozen=True)
class BehaviorRef[A: BehaviorActor]:
    """
    Actor reference for any behavior actor.
    """

    actor_address: ActorAddress

    _a: ClassVar[TypeForm[A] | None] = None

    @cache
    @staticmethod
    def __class_getitem__(item: type) -> type:
        class _BehaviorRef(BehaviorRef):  # type: ignore
            _a = item  # Actor class that the behavior is pointing to.

        return _BehaviorRef

    @property
    def be(self) -> BehaviorRefMethods[A]:
        """
        Remote access to all behavior methods of the `BehaviorActor`.
        """
        actor_cls = self._a
        assert actor_cls is not None

        if not TYPE_CHECKING:
            A = actor_cls

        behavior_methods = get_behaviors_from_class(actor_cls)

        return cast(
            BehaviorRefMethods[A],
            _RuntimeBehaviorMethods[A](
                self.actor_address, behavior_methods=behavior_methods
            ),
        )

    @property
    def rpc(self) -> RpcMethods[A]:
        actor_cls = self._a
        assert actor_cls is not None

        if not TYPE_CHECKING:
            A = actor_cls

        rpc_methods = get_rpc_methods_from_class(actor_cls)

        return cast(
            RpcMethods[A],
            _RuntimeRpcMethods[A](self.actor_address, rpc_methods=rpc_methods),
        )

    @model_serializer
    def _serialize(self) -> dict[str, object]:
        return {
            "type_": "behavior-actor-ref",
            "actor_address": {
                "actor_id": self.actor_address.actor_id,
                "system_id": self.actor_address.system_id,
            },
        }

    @model_validator(mode="before")
    @classmethod
    def _deserialize(cls, data: Any) -> Any:
        if isinstance(data, BehaviorRef):
            return {"actor_address": data.actor_address}
        return cls(
            actor_address=ActorAddress(
                actor_id=data["actor_address"]["actor_id"],
                system_id=data["actor_address"]["system_id"],
            )
        )


type BehaviorRefMethods[A: BehaviorActor] = NewProtocol[
    *[
        # Take the `behavior_method` from the `_BehaviorMethod` attributes from
        # a `BehaviorActor`.
        Member[p.name, GetMemberType[p.type, Literal["behavior_method"]]]
        for p in Iter[Attrs[A]]
        if IsAssignable[p.type, _BehaviorMethod[Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any]]
        or IsAssignable[p.type, _BehaviorMethod[Any, Any, Any, Any, Any]]
    ]
]

type RpcMethods[A: BehaviorActor] = NewProtocol[
    *[
        # Take the `rpc_method` from the `_RpcMethod` attributes from
        # a `BehaviorActor`.
        Member[p.name, GetMemberType[p.type, Literal["rpc_method"]]]
        for p in Iter[Attrs[A]]
        if IsAssignable[p.type, _RpcMethod[Any, Any, Any]]
        or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any]]
        or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any, Any]]
        or IsAssignable[p.type, _RpcMethod[Any, Any, Any, Any, Any, Any]]
    ]
]


class _RuntimeBehaviorMethods[A: BehaviorActor]:
    def __init__(
        self,
        actor_address: ActorAddress,
        behavior_methods: Mapping[str, _BehaviorMethod[Any, Any]],
    ) -> None:
        self.actor_address = actor_address
        self.behavior_methods = behavior_methods

    def __dir__(self) -> list[str]:
        return [name for name in self.behavior_methods.keys()]

    def __getattr__(self, name: str) -> Callable[[Any], None]:
        behavior_method = self.behavior_methods[name]

        def call_behavior[*I](*params: *I) -> None:
            orig_actor_cls = get_args(self.__orig_class__)[0]

            msg_type = behavior_method.get_behavior_message_type(name, orig_actor_cls)
            message = msg_type(behavior_name=name, inputs=list(params))

            _get_system().call_actor_soon(
                self.actor_address,
                message=message,
                serialize=lambda msg: SerializedMessage(data=msg.model_dump_json()),
            )

        return call_behavior


class _RuntimeRpcMethods[A: BehaviorActor]:
    def __init__(
        self,
        actor_address: ActorAddress,
        rpc_methods: Mapping[str, _RpcMethod[Any, Any, Any]],
    ) -> None:
        self.actor_address = actor_address
        self.rpc_methods = rpc_methods

    def __dir__(self) -> list[str]:
        return [name for name in self.rpc_methods.keys()]

    def __getattr__(self, name: str) -> Callable[[Any], Coroutine[Any, Any, Any]]:
        from actorium.actors.future import Future

        rpc_method = self.rpc_methods[name]

        async def call_rpc[*I, O](*params: *I, timeout: float | None = None) -> O:
            orig_actor_cls = get_args(self.__orig_class__)[0]
            output_type = rpc_method.get_output_type(orig_actor_cls)
            output_adapter: TypeAdapter[O] = TypeAdapter(output_type)

            if not TYPE_CHECKING:
                O = output_type
            future = Future[O]()

            msg_type = rpc_method.get_rpc_message_type(name, orig_actor_cls)

            message = msg_type(
                rpc_name=name,
                inputs=list(params),
                reply_to=future.actor,
            )
            _get_system().call_actor_soon(
                self.actor_address,
                message=message,
                serialize=lambda msg: SerializedMessage(data=msg.model_dump_json()),
            )

            with fail_after(timeout):
                serialized_return_value = await future.result()

            result = output_adapter.validate_python(serialized_return_value)
            return result

        return call_rpc


def _substitute_type(
    type_definition: TypeForm[Any],
    type_params: tuple[TypeVar, ...],
    args: tuple[type, ...],
) -> TypeForm[Any]:
    if len(type_params) != len(args):
        raise RuntimeError("Type parameters not specified for behavior actor.")

    if isinstance(type_definition, TypeVar):
        # Lookup.
        for t, a in zip(type_params, args):
            if type_definition == t:
                return a
        raise RuntimeError("Type parameter not found.")

    if isinstance(type_definition, GenericAlias):
        cls = get_origin(type_definition)
        return cls[
            *[_substitute_type(a, type_params, args) for a in type_definition.__args__]
        ]

    # TODO: recurse into other container types!

    return type_definition
