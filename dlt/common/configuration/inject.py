import re
import inspect
from makefun import wraps
from types import ModuleType
from typing import Callable, List, Dict, Type, Any, Optional, Tuple, overload
from inspect import Signature, Parameter

from dlt.common.typing import StrAny, TFun, AnyFun
from dlt.common.configuration.resolve import make_configuration
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, is_valid_hint, configspec

# [^.^_]+ splits by . or _
_SLEEPING_CAT_SPLIT = re.compile("[^.^_]+")


@overload
def with_config(func: TFun, /, spec: Type[BaseConfiguration] = None, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  TFun:
    ...


@overload
def with_config(func: None = ..., /, spec: Type[BaseConfiguration] = None, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:
    ...


def with_config(func: Optional[AnyFun] = None, /, spec: Type[BaseConfiguration] = None, only_kw: bool = False, namespaces: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:

    namespace_f: Callable[[StrAny], str] = None
    # namespace may be a function from function arguments to namespace
    if callable(namespaces):
        namespace_f = namespaces

    def decorator(f: TFun) -> TFun:
        SPEC: Type[BaseConfiguration] = None
        sig: Signature = inspect.signature(f)
        kwargs_par = next((p for p in sig.parameters.values() if p.kind == Parameter.VAR_KEYWORD), None)

        if spec is None:
            SPEC = _spec_from_signature(_get_spec_name_from_f(f), inspect.getmodule(f), sig, only_kw)
        else:
            SPEC = spec

        # for all positional parameters that do not have default value, set default
        for p in sig.parameters.values():
            if hasattr(SPEC, p.name) and p.default == Parameter.empty:
                p._default = None  # type: ignore

        @wraps(f, new_sig=sig)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # for calls providing all parameters to the func, configuration may not be resolved
            # if len(args) + len(kwargs) == len(sig.parameters):
            #     return f(*args, **kwargs)

            # bind parameters to signature
            bound_args = sig.bind_partial(*args, **kwargs)
            bound_args.apply_defaults()
            # if namespace derivation function was provided then call it
            nonlocal namespaces
            if namespace_f:
                namespaces = (namespace_f(bound_args.arguments), )
            # namespaces may be a string
            if isinstance(namespaces, str):
                namespaces = (namespaces,)
            # resolve SPEC
            config = make_configuration(SPEC(), namespaces=namespaces, initial_value=bound_args.arguments)
            resolved_params = dict(config)
            # overwrite or add resolved params
            for p in sig.parameters.values():
                if p.name in resolved_params:
                    bound_args.arguments[p.name] = resolved_params.pop(p.name)
            # pass all other config parameters into kwargs if present
            if kwargs_par is not None:
                bound_args.arguments[kwargs_par.name].update(resolved_params)
            # call the function with injected config
            return f(*bound_args.args, **bound_args.kwargs)

        return _wrap  # type: ignore

    # See if we're being called as @with_config or @with_config().
    if func is None:
        # We're called with parens.
        return decorator

    if not callable(func):
        raise ValueError("First parameter to the with_config must be callable ie. by using it as function decorator")

    # We're called as @with_config without parens.
    return decorator(func)


def _get_spec_name_from_f(f: AnyFun) -> str:
    func_name = f.__qualname__.replace("<locals>.", "")  # func qual name contains position in the module, separated by dots

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
            field_type = Any if p.annotation == Parameter.empty else p.annotation
            if is_valid_hint(field_type):
                field_default = None if p.default == Parameter.empty else p.default
                # try to get type from default
                if field_type is Any and field_default:
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
