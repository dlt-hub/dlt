import re
import inspect
from makefun import wraps
from types import ModuleType
from typing import Callable, Dict, Type, Any, Optional, Tuple, TypeVar, overload
from inspect import Signature, Parameter

from dlt.common.typing import AnyType, DictStrAny, StrAny, TFun, AnyFun
from dlt.common.configuration.resolve import resolve_configuration, inject_namespace
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, is_valid_hint, configspec
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.utils import get_callable_name

# [^.^_]+ splits by . or _
_SLEEPING_CAT_SPLIT = re.compile("[^.^_]+")
_LAST_DLT_CONFIG = "_dlt_config"
_ORIGINAL_ARGS = "_dlt_orig_args"
TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)
# keep a registry of all the decorated functions
_FUNC_SPECS: Dict[int, Type[BaseConfiguration]] = {}


def get_fun_spec(f: AnyFun) -> Type[BaseConfiguration]:
    return _FUNC_SPECS.get(id(f))


@overload
def with_config(func: TFun, /, spec: Type[BaseConfiguration] = None, auto_namespace: bool = False, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  TFun:
    ...


@overload
def with_config(func: None = ..., /, spec: Type[BaseConfiguration] = None, auto_namespace: bool = False, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:
    ...


def with_config(func: Optional[AnyFun] = None, /, spec: Type[BaseConfiguration] = None, auto_namespace: bool = False, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:

    namespace_f: Callable[[StrAny], str] = None
    # namespace may be a function from function arguments to namespace
    if callable(namespaces):
        namespace_f = namespaces

    def decorator(f: TFun) -> TFun:
        SPEC: Type[BaseConfiguration] = None
        sig: Signature = inspect.signature(f)
        kwargs_arg = next((p for p in sig.parameters.values() if p.kind == Parameter.VAR_KEYWORD), None)
        spec_arg: Parameter = None
        pipeline_name_arg: Parameter = None
        namespace_context = ConfigNamespacesContext()

        if spec is None:
            SPEC = _spec_from_signature(_get_spec_name_from_f(f), inspect.getmodule(f), sig, only_kw)
        else:
            SPEC = spec

        for p in sig.parameters.values():
            # for all positional parameters that do not have default value, set default
            # if hasattr(SPEC, p.name) and p.default == Parameter.empty:
            #     p._default = None  # type: ignore
            if p.annotation is SPEC:
                # if any argument has type SPEC then us it to take initial value
                spec_arg = p
            if p.name == "pipeline_name" and auto_namespace:
                # if argument has name pipeline_name and auto_namespace is used, use it to generate namespace context
                pipeline_name_arg = p


        @wraps(f, new_sig=sig)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # bind parameters to signature
            bound_args = sig.bind_partial(*args, **kwargs)
            bound_args.apply_defaults()
            # for calls containing resolved spec in the kwargs, we do not need to resolve again
            config: BaseConfiguration = None
            if _LAST_DLT_CONFIG in kwargs:
                config = last_config(**kwargs)
            else:
                # if namespace derivation function was provided then call it
                nonlocal namespaces
                if namespace_f:
                    namespaces = (namespace_f(bound_args.arguments), )
                # namespaces may be a string
                if isinstance(namespaces, str):
                    namespaces = (namespaces,)
                # if one of arguments is spec the use it as initial value
                if spec_arg:
                    config = bound_args.arguments.get(spec_arg.name, None)
                # resolve SPEC, also provide namespace_context with pipeline_name
                if pipeline_name_arg:
                    namespace_context.pipeline_name = bound_args.arguments.get(pipeline_name_arg.name, None)
                with inject_namespace(namespace_context):
                    config = resolve_configuration(config or SPEC(), namespaces=namespaces, explicit_value=bound_args.arguments)
            resolved_params = dict(config)
            # overwrite or add resolved params
            for p in sig.parameters.values():
                if p.name in resolved_params:
                    bound_args.arguments[p.name] = resolved_params.pop(p.name)
                if p.annotation is SPEC:
                    bound_args.arguments[p.name] = config
            # pass all other config parameters into kwargs if present
            if kwargs_arg is not None:
                bound_args.arguments[kwargs_arg.name].update(resolved_params)
                bound_args.arguments[kwargs_arg.name][_LAST_DLT_CONFIG] = config
                bound_args.arguments[kwargs_arg.name][_ORIGINAL_ARGS] = (args, kwargs)
            # call the function with resolved config
            return f(*bound_args.args, **bound_args.kwargs)

        # register the spec for a wrapped function
        _FUNC_SPECS[id(_wrap)] = SPEC

        return _wrap  # type: ignore

    # See if we're being called as @with_config or @with_config().
    if func is None:
        # We're called with parens.
        return decorator

    if not callable(func):
        raise ValueError("First parameter to the with_config must be callable ie. by using it as function decorator")

    # We're called as @with_config without parens.
    return decorator(func)


def last_config(**kwargs: Any) -> BaseConfiguration:
    return kwargs[_LAST_DLT_CONFIG]  # type: ignore


def get_orig_args(**kwargs: Any) -> Tuple[Tuple[Any], DictStrAny]:
    return kwargs[_ORIGINAL_ARGS]  # type: ignore


def _get_spec_name_from_f(f: AnyFun) -> str:
    func_name = get_callable_name(f, "__qualname__").replace("<locals>.", "")  # func qual name contains position in the module, separated by dots

    def _first_up(s: str) -> str:
        return s[0].upper() + s[1:]

    return "".join(map(_first_up, _SLEEPING_CAT_SPLIT.findall(func_name))) + "Configuration"


def _spec_from_signature(name: str, module: ModuleType, sig: Signature, kw_only: bool = False) -> Type[BaseConfiguration]:
    # synthesize configuration from the signature
    fields: Dict[str, Any] = {}
    annotations: Dict[str, Any] = {}

    for p in sig.parameters.values():
        # skip *args and **kwargs, skip typical method params and if kw_only flag is set: accept KEYWORD ONLY args
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL) and p.name not in ["self", "cls"] and \
           (kw_only and p.kind == Parameter.KEYWORD_ONLY or not kw_only):
            field_type = AnyType if p.annotation == Parameter.empty else p.annotation
            if is_valid_hint(field_type):
                field_default = None if p.default == Parameter.empty else p.default
                # try to get type from default
                if field_type is AnyType and field_default:
                    field_type = type(field_default)
                # make type optional if explicit None is provided as default
                if p.default is None:
                    field_type = Optional[field_type]
                # set annotations
                annotations[p.name] = field_type
                # set field with default value
                fields[p.name] = field_default

    # new type goes to the module where sig was declared
    fields["__module__"] = module.__name__
    # set annotations so they are present in __dict__
    fields["__annotations__"] = annotations
    # synthesize type
    T: Type[BaseConfiguration] = type(name, (BaseConfiguration,), fields)
    # add to the module
    setattr(module, name, T)
    SPEC = configspec(init=False)(T)
    # print(f"SYNTHESIZED {SPEC} in {inspect.getmodule(SPEC)} for sig {sig}")
    # import dataclasses
    # print("\n".join(map(str, dataclasses.fields(SPEC))))
    return SPEC
