"""
Mypy plugin for actorium BehaviorActor typing.

This plugin provides type information for BehaviorRef and its attributes.
"""

from __future__ import annotations

from typing import Any, Callable
from typing import Type as TypingType

from mypy.nodes import ARG_OPT, Decorator, TypeInfo
from mypy.plugin import AttributeContext, Plugin
from mypy.types import AnyType, CallableType, Instance, Type, TypeOfAny, get_proper_type


class ActoriumPlugin(Plugin):
    """Mypy plugin for actorium typing support."""

    def get_function_hook(self, fullname: str) -> Callable[[Any], Type] | None:
        """Hook into function calls"""
        if fullname in ("actorium.spawn", "actorium.system.spawn"):
            return self._spawn_hook
        return None

    def _spawn_hook(self, ctx: Any) -> Type:
        """
        Provide return type for spawn() calls.

        spawn(SomeActor, ...) should return the type returned by SomeActor.actor_ref()
        """
        from mypy.plugin import FunctionContext

        if not isinstance(ctx, FunctionContext):
            return ctx.default_return_type  # type: ignore[no-any-return]

        # Get the first argument (the actor class)
        if not ctx.args or not ctx.args[0]:
            return ctx.default_return_type

        actor_arg = ctx.args[0][0]  # First argument, first element
        actor_type = ctx.api.get_expression_type(actor_arg)
        actor_type = get_proper_type(actor_type)

        # If it's a callable (class), get the return type (the instance type)
        if isinstance(actor_type, CallableType):
            instance_type = get_proper_type(actor_type.ret_type)

            if isinstance(instance_type, Instance):
                # Check if this is a BehaviorActor subclass
                for base in instance_type.type.mro:
                    if base.fullname == "actorium.actors.behaviors.BehaviorActor":
                        # This is a BehaviorActor, so spawn returns BehaviorRef[InstanceType]
                        # Get BehaviorRef TypeInfo from the module
                        try:
                            module = ctx.api.modules.get("actorium.actors.behaviors")  # type: ignore[attr-defined]
                            if module:
                                behavior_ref_sym = module.names.get("BehaviorRef")
                                if behavior_ref_sym and isinstance(
                                    behavior_ref_sym.node, TypeInfo
                                ):
                                    return Instance(
                                        behavior_ref_sym.node, [instance_type]
                                    )
                        except Exception:
                            pass
                        break

        return ctx.default_return_type

    def get_type_analyze_hook(self, fullname: str) -> Callable[[Any], Type] | None:
        """Hook into type analysis for BehaviorRef"""
        if fullname == "actorium.actors.behaviors.BehaviorRef":
            return self._analyze_behavior_ref_type
        return None

    def _analyze_behavior_ref_type(self, ctx: Any) -> Type:
        """
        Analyze BehaviorRef[SomeActor] and create appropriate type.

        This hook intercepts when mypy sees BehaviorRef[X] and we need to tell
        mypy what attributes are available.
        """
        # ctx is an AnalyzeTypeContext
        from mypy.plugin import AnalyzeTypeContext

        if not isinstance(ctx, AnalyzeTypeContext):
            return AnyType(TypeOfAny.from_error)

        # Get the argument to BehaviorRef (the actor class)
        if not ctx.type or not ctx.type.args:
            # BehaviorRef without type argument - shouldn't happen but handle it
            return ctx.api.named_generic_type(  # type: ignore[no-any-return,attr-defined]
                "actorium.actors.behaviors.BehaviorRef", []
            )

        # Analyze the actor type argument
        actor_arg = ctx.api.analyze_type(ctx.type.args[0])
        if actor_arg is None:
            return AnyType(TypeOfAny.from_error)

        actor_type = get_proper_type(actor_arg)

        if not isinstance(actor_type, Instance):
            return AnyType(TypeOfAny.from_error)

        # Create BehaviorRef[ActorType] instance
        behavior_ref_typeinfo = ctx.api.lookup_fully_qualified(  # type: ignore[attr-defined]
            "actorium.actors.behaviors.BehaviorRef"
        ).node

        if not isinstance(behavior_ref_typeinfo, TypeInfo):
            return AnyType(TypeOfAny.from_error)

        # Return an Instance of BehaviorRef parameterized with the actor type
        return Instance(behavior_ref_typeinfo, [actor_type])

    def get_attribute_hook(
        self, fullname: str
    ) -> Callable[[AttributeContext], Type] | None:
        """Hook into attribute accesses"""
        # Skip builtins and obviously unrelated modules for performance
        if (
            fullname.startswith("builtins.")
            or fullname.startswith("typing.")
            or fullname.startswith("_")
            or fullname.startswith("pydantic.v1.")
            or fullname.startswith("actorium.utils.")
        ):
            return None

        # For everything else, check if it's a BehaviorRef
        return self._check_behavior_ref_attribute

    def _check_behavior_ref_attribute(self, ctx: AttributeContext) -> Type:
        """
        Check if we're accessing an attribute on BehaviorRef and provide appropriate typing.
        """
        # Get the type of the object whose attribute is being accessed
        instance_type = get_proper_type(ctx.type)

        if not isinstance(instance_type, Instance):
            return ctx.default_attr_type

        # Check if this is a BehaviorRef type
        if not (
            instance_type.type.fullname == "actorium.actors.behaviors.BehaviorRef"
            or instance_type.type.name == "BehaviorRef"
        ):
            return ctx.default_attr_type

        # This is BehaviorRef, now extract the actor class
        if not instance_type.args:
            return ctx.default_attr_type

        actor_type = get_proper_type(instance_type.args[0])

        # Handle TypeVar resolution
        from mypy.types import TypeVarType

        if isinstance(actor_type, TypeVarType):
            if actor_type.upper_bound:
                actor_type = get_proper_type(actor_type.upper_bound)

        if not isinstance(actor_type, Instance):
            return ctx.default_attr_type

        # Get the attribute name being accessed
        attribute_name = ctx.context.name  # type: ignore[attr-defined]

        # Find if this attribute corresponds to a @behavior or @rpc method
        actor_class_info = actor_type.type

        # Check for @behavior decorated method
        behavior_method = self._find_decorated_method(
            actor_class_info, attribute_name, "behavior"
        )

        if behavior_method is not None:
            # Apply type substitutions if the actor is generic
            if actor_type.args:
                behavior_method = self._apply_type_substitutions(
                    behavior_method, actor_class_info, list(actor_type.args)
                )

            # Return SimpleRef's __call__ signature (non-async, fire-and-forget)
            result = self._create_simple_ref_type(behavior_method, ctx.api)
            if result is not None:
                return result

        # Check for @rpc decorated method
        rpc_method = self._find_decorated_method(
            actor_class_info, attribute_name, "rpc"
        )

        if rpc_method is not None:
            # Apply type substitutions if the actor is generic
            if actor_type.args:
                rpc_method = self._apply_type_substitutions(
                    rpc_method, actor_class_info, list(actor_type.args)
                )

            # Return RpcRef's __call__ signature (async with timeout)
            result = self._create_rpc_ref_type(rpc_method, ctx.api)
            if result is not None:
                return result

        # Not a behavior or rpc method, use default
        return ctx.default_attr_type

    def _find_decorated_method(
        self, actor_class_info: TypeInfo, method_name: str, decorator_name: str
    ) -> CallableType | None:
        """Find a specific method decorated with @behavior or @rpc"""
        # Check the class and all its bases
        for base in [actor_class_info] + actor_class_info.mro[:-1]:
            symbol = base.names.get(method_name)
            if symbol is None or symbol.node is None:
                continue

            if isinstance(symbol.node, Decorator):
                decorator = symbol.node
                if self._has_decorator(decorator, decorator_name):
                    func_def = decorator.func
                    if func_def.type and isinstance(func_def.type, CallableType):
                        return func_def.type

        return None

    def _has_decorator(self, decorator: Decorator, decorator_name: str) -> bool:
        """Check if a Decorator node has a specific decorator applied."""
        for dec_expr in decorator.original_decorators:
            if hasattr(dec_expr, "name") and dec_expr.name == decorator_name:
                return True
            if hasattr(dec_expr, "fullname"):
                if dec_expr.fullname and dec_expr.fullname.endswith(
                    f".{decorator_name}"
                ):
                    return True
        return False

    def _apply_type_substitutions(
        self,
        method_type: CallableType,
        actor_class_info: TypeInfo,
        type_args: list[Type],
    ) -> CallableType:
        """
        Apply type parameter substitutions to a method type.

        For example, if Calculator[T] is specialized as Calculator[int],
        this will substitute T -> int in the method signature.
        """
        # Get the defn for type variables
        if not actor_class_info.defn or not actor_class_info.defn.type_vars:
            return method_type

        type_vars = actor_class_info.defn.type_vars

        if len(type_vars) != len(type_args):
            return method_type

        # Create a substitution mapping
        from mypy.expandtype import expand_type

        # Build the mapping from type variable IDs to their concrete types
        type_map = {tv.id: arg for tv, arg in zip(type_vars, type_args)}

        # Apply the substitutions to the method type
        return expand_type(method_type, type_map)

    def _create_simple_ref_type(
        self, original_type: CallableType, api: Any
    ) -> CallableType | None:
        """
        Create the callable type for SimpleRef.

        SimpleRef[*T] has __call__(*message: *T) -> None
        We strip 'self' and make it return None (fire-and-forget).
        """
        try:
            # Remove 'self' from argument list
            arg_types = list(original_type.arg_types[1:])
            arg_kinds = list(original_type.arg_kinds[1:])
            arg_names = list(original_type.arg_names[1:])

            # Return type is None for behaviors (fire-and-forget)
            from mypy.types import NoneType

            ret_type = NoneType()

            return CallableType(
                arg_types=arg_types,
                arg_kinds=arg_kinds,
                arg_names=arg_names,
                ret_type=ret_type,
                fallback=original_type.fallback,
                name=original_type.name,
            )
        except Exception:
            return None

    def _create_rpc_ref_type(
        self, original_type: CallableType, api: Any
    ) -> CallableType | None:
        """
        Create the callable type for RpcRef.

        RpcRef[*I, O] has async __call__(*inputs: *I, timeout: float | None = None) -> O
        We strip 'self' and add the timeout parameter.
        """
        try:
            # Remove 'self' from argument list
            arg_types = list(original_type.arg_types[1:])
            arg_kinds = list(original_type.arg_kinds[1:])
            arg_names = list(original_type.arg_names[1:])

            # Add optional timeout parameter
            from mypy.types import NoneType, UnionType

            timeout_type: Type
            try:
                float_type = api.named_type("builtins.float")
                timeout_type = UnionType([float_type, NoneType()])
            except Exception:
                timeout_type = AnyType(TypeOfAny.special_form)

            arg_types.append(timeout_type)
            arg_kinds.append(ARG_OPT)
            arg_names.append("timeout")

            # Keep the original return type (the RPC method's return type)
            ret_type = original_type.ret_type

            return CallableType(
                arg_types=arg_types,
                arg_kinds=arg_kinds,
                arg_names=arg_names,
                ret_type=ret_type,
                fallback=original_type.fallback,
                name=original_type.name,
            )
        except Exception:
            return None


def plugin(version: str) -> TypingType[Plugin]:
    """Entry point for mypy plugin."""
    return ActoriumPlugin
