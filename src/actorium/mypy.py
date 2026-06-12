"""
Mypy plugin for actorium BehaviorActor typing.

This plugin provides type information for BehaviorRef and its attributes.
"""

from __future__ import annotations

from typing import Any, Callable
from typing import Type as TypingType

from mypy.nodes import Decorator, FuncDef, MemberExpr, TypeInfo
from mypy.plugin import AttributeContext, Plugin
from mypy.types import AnyType, CallableType, Instance, Type, TypeOfAny, get_proper_type


class ActoriumPlugin(Plugin):
    """Mypy plugin for actorium typing support."""

    def get_function_hook(self, fullname: str) -> Callable[[Any], Type] | None:
        """Hook into function calls"""
        # Hook into spawn function
        if fullname == "actorium.system.spawn":
            return self._spawn_hook
        return None

    def get_method_hook(self, fullname: str) -> Callable[[Any], Type] | None:
        """Hook into method calls"""
        # Hook into any actor_ref method call - we'll check if it's a BehaviorActor in the hook
        if fullname.endswith(".actor_ref"):
            return self._actor_ref_hook
        return None

    def _spawn_hook(self, ctx: Any) -> Type:
        """
        Provide return type for spawn() calls.

        spawn(ActorClass) should return the type that ActorClass.actor_ref() returns.
        """
        from mypy.plugin import FunctionContext

        if not isinstance(ctx, FunctionContext):
            return ctx.default_return_type  # type: ignore[no-any-return]

        # Get the first argument to spawn (the actor factory/class)
        if not ctx.args or not ctx.args[0]:
            return ctx.default_return_type

        arg_expr = ctx.args[0][0]

        # Try to get the TypeInfo of the actor class
        type_info = None
        instance_type = None

        # First, try to get the type from arg_types (works for both generic and non-generic)
        arg_type = get_proper_type(ctx.arg_types[0][0])
        if isinstance(arg_type, CallableType):
            # It's a constructor, get the return type
            ret_type = get_proper_type(arg_type.ret_type)
            if isinstance(ret_type, Instance):
                type_info = ret_type.type
                instance_type = ret_type

        # Fallback: try to get from the expression node
        if not type_info and hasattr(arg_expr, "node"):
            if isinstance(arg_expr.node, TypeInfo):
                type_info = arg_expr.node
                instance_type = Instance(type_info, [])
            elif hasattr(arg_expr.node, "type") and isinstance(
                arg_expr.node.type, TypeInfo
            ):
                type_info = arg_expr.node.type
                instance_type = Instance(type_info, [])

        if not type_info or not instance_type:
            return ctx.default_return_type

        # Check if this actor has a custom actor_ref implementation
        # (look for it in the class itself, not in base classes)
        actor_ref_method = None
        symbol = type_info.names.get("actor_ref")
        if symbol is not None and symbol.node is not None:
            # Handle both decorated and non-decorated methods
            if isinstance(symbol.node, Decorator):
                func_def = symbol.node.func
            elif isinstance(symbol.node, FuncDef):
                func_def = symbol.node
            else:
                func_def = None

            if func_def and func_def.type and isinstance(func_def.type, CallableType):
                actor_ref_method = func_def.type

        # If we found a custom actor_ref, use its return type
        if actor_ref_method is not None:
            # Apply type substitutions if the actor is generic
            if instance_type.args:
                actor_ref_method = self._apply_type_substitutions(
                    actor_ref_method, type_info, list(instance_type.args)
                )

            # Get the return type of actor_ref()
            ret_type = actor_ref_method.ret_type

            # Substitute Self with the actual instance type
            ret_type = self._substitute_self_in_type(ret_type, instance_type, type_info)

            return ret_type

        # Check if this is a BehaviorActor subclass (default behavior)
        is_behavior_actor = any(
            base.fullname == "actorium.actors.behaviors.BehaviorActor"
            for base in type_info.mro
        )

        if not is_behavior_actor:
            return ctx.default_return_type

        # For BehaviorActor without custom actor_ref, return BehaviorRef[instance_type]
        try:
            module = ctx.api.modules.get("actorium.actors.behaviors")  # type: ignore[attr-defined]
            if not module:
                return ctx.default_return_type

            behavior_ref_sym = module.names.get("BehaviorRef")
            if not behavior_ref_sym or not isinstance(behavior_ref_sym.node, TypeInfo):
                return ctx.default_return_type

            # Return BehaviorRef[instance_type]
            result = Instance(behavior_ref_sym.node, [instance_type])
            return result
        except Exception:
            return ctx.default_return_type

    def _substitute_self_in_type(
        self, typ: Type, self_type: Instance, type_info: TypeInfo
    ) -> Type:
        """
        Substitute Self in a type with the actual self type.

        For BehaviorRef[Self], this becomes BehaviorRef[ActualClass].
        """
        from mypy.types import TypeVarType

        typ = get_proper_type(typ)

        if isinstance(typ, TypeVarType) and typ.name == "Self":
            return self_type

        if isinstance(typ, Instance):
            # Recursively substitute in type arguments
            new_args = []
            for arg in typ.args:
                new_args.append(
                    self._substitute_self_in_type(arg, self_type, type_info)
                )
            return Instance(typ.type, new_args)

        return typ

    def _actor_ref_hook(self, ctx: Any) -> Type:
        """
        Provide return type for BehaviorActor.actor_ref() calls.

        actor.actor_ref() should return BehaviorRef[type(actor)]
        """
        from mypy.plugin import MethodContext

        if not isinstance(ctx, MethodContext):
            return ctx.default_return_type  # type: ignore[no-any-return]

        # Get the type of 'self' (the actor instance)
        self_type = get_proper_type(ctx.type)

        if not isinstance(self_type, Instance):
            return ctx.default_return_type

        # Check if this is a BehaviorActor subclass
        is_behavior_actor = False
        for base in self_type.type.mro:
            if base.fullname == "actorium.actors.behaviors.BehaviorActor":
                is_behavior_actor = True
                break

        if not is_behavior_actor:
            return ctx.default_return_type

        # Get BehaviorRef TypeInfo
        try:
            module = ctx.api.modules.get("actorium.actors.behaviors")  # type: ignore[attr-defined]
            if module:
                behavior_ref_sym = module.names.get("BehaviorRef")
                if behavior_ref_sym and isinstance(behavior_ref_sym.node, TypeInfo):
                    # Return BehaviorRef[SelfType]
                    return Instance(behavior_ref_sym.node, [self_type])
        except Exception:
            pass

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
        # ctx.context should be a MemberExpr when accessing attributes
        if not isinstance(ctx.context, MemberExpr):
            return ctx.default_attr_type

        attribute_name = ctx.context.name

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

    def _find_actor_ref_method(self, actor_class_info: TypeInfo) -> CallableType | None:
        """Find the actor_ref method on the actor class"""
        # Check the class and all its bases
        for base in [actor_class_info] + actor_class_info.mro[:-1]:
            symbol = base.names.get("actor_ref")
            if symbol is None or symbol.node is None:
                continue

            # Handle both decorated and non-decorated methods
            if isinstance(symbol.node, Decorator):
                func_def = symbol.node.func
            elif isinstance(symbol.node, FuncDef):
                func_def = symbol.node
            else:
                continue

            if func_def.type and isinstance(func_def.type, CallableType):
                return func_def.type

        return None

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
    ) -> Type | None:
        """
        Create the type for SimpleRef.

        SimpleRef[*T] represents a reference to a behavior method that takes (*T) as arguments.
        """
        try:
            # Remove 'self' from argument list
            arg_types = list(original_type.arg_types[1:])

            # Get SimpleRef TypeInfo
            try:
                module = api.modules.get("actorium.actors.simple")
                if not module:
                    return None
                simple_ref_sym = module.names.get("SimpleRef")
                if simple_ref_sym and isinstance(simple_ref_sym.node, TypeInfo):
                    # Return SimpleRef[*arg_types]
                    return Instance(simple_ref_sym.node, arg_types)
            except Exception:
                pass

            return None
        except Exception:
            return None

    def _create_rpc_ref_type(
        self, original_type: CallableType, api: Any
    ) -> Type | None:
        """
        Create the type for RpcRef.

        RpcRef[*I, O] represents a reference to an RPC method that takes (*I) as inputs
        and returns O.

        Note: The original method is async and returns Coroutine[Any, Any, O], but
        RpcRef's __call__ unwraps this to just O.
        """
        try:
            # Remove 'self' from argument list
            arg_types = list(original_type.arg_types[1:])

            # Get the return type and unwrap Coroutine if needed
            ret_type = original_type.ret_type

            # If it's a coroutine, extract the actual return type
            ret_type = get_proper_type(ret_type)
            if isinstance(ret_type, Instance) and ret_type.type.fullname in (
                "typing.Coroutine",
                "collections.abc.Coroutine",
                "typing.Awaitable",
                "collections.abc.Awaitable",
            ):
                # For Coroutine[T, U, V] or Awaitable[V], the last type arg is the result
                if ret_type.args:
                    ret_type = ret_type.args[-1]

            # Get RpcRef TypeInfo
            try:
                module = api.modules.get("actorium.actors.simple_rpc")
                if not module:
                    return None
                rpc_ref_sym = module.names.get("RpcRef")
                if rpc_ref_sym and isinstance(rpc_ref_sym.node, TypeInfo):
                    # Return RpcRef[*arg_types, ret_type]
                    return Instance(rpc_ref_sym.node, [*arg_types, ret_type])
            except Exception:
                pass

            return None
        except Exception:
            return None


def plugin(version: str) -> TypingType[Plugin]:
    """Entry point for mypy plugin."""
    return ActoriumPlugin
