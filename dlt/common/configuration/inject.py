import inspect
from functools import wraps
from typing import Callable, Dict, Type, Any, Optional, Tuple, TypeVar, overload
from inspect import Signature, Parameter

from dlt.common.typing import DictStrAny, StrAny, TFun, AnyFun
from dlt.common.configuration.resolve import resolve_configuration, inject_section
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.configuration.specs.config_namespace_context import ConfigSectionContext
from dlt.common.reflection.spec import spec_from_signature


_LAST_DLT_CONFIG = "_dlt_config"
_ORIGINAL_ARGS = "_dlt_orig_args"
# keep a registry of all the decorated functions
_FUNC_SPECS: Dict[int, Type[BaseConfiguration]] = {}

TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def get_fun_spec(f: AnyFun) -> Type[BaseConfiguration]:
    return _FUNC_SPECS.get(id(f))


@overload
def with_config(func: TFun, /, spec: Type[BaseConfiguration] = None, auto_section: bool = False, only_kw: bool = False, sections: Tuple[str, ...] = ()) ->  TFun:
    ...


@overload
def with_config(func: None = ..., /, spec: Type[BaseConfiguration] = None, auto_section: bool = False, only_kw: bool = False, sections: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:
    ...


def with_config(func: Optional[AnyFun] = None, /, spec: Type[BaseConfiguration] = None, auto_section: bool = False, only_kw: bool = False, sections: Tuple[str, ...] = ()) ->  Callable[[TFun], TFun]:

    section_f: Callable[[StrAny], str] = None
    # section may be a function from function arguments to section
    if callable(sections):
        section_f = sections

    def decorator(f: TFun) -> TFun:
        SPEC: Type[BaseConfiguration] = None
        sig: Signature = inspect.signature(f)
        kwargs_arg = next((p for p in sig.parameters.values() if p.kind == Parameter.VAR_KEYWORD), None)
        spec_arg: Parameter = None
        pipeline_name_arg: Parameter = None
        section_context = ConfigSectionContext()

        if spec is None:
            SPEC = spec_from_signature(f, sig, only_kw)
        else:
            SPEC = spec

        for p in sig.parameters.values():
            # for all positional parameters that do not have default value, set default
            # if hasattr(SPEC, p.name) and p.default == Parameter.empty:
            #     p._default = None  # type: ignore
            if p.annotation is SPEC:
                # if any argument has type SPEC then us it to take initial value
                spec_arg = p
            if p.name == "pipeline_name" and auto_section:
                # if argument has name pipeline_name and auto_section is used, use it to generate section context
                pipeline_name_arg = p
                pipeline_name_arg_default = None if p.default == Parameter.empty else p.default


        @wraps(f)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # bind parameters to signature
            bound_args = sig.bind(*args, **kwargs)
            # for calls containing resolved spec in the kwargs, we do not need to resolve again
            config: BaseConfiguration = None
            if _LAST_DLT_CONFIG in kwargs:
                config = last_config(**kwargs)
            else:
                # if section derivation function was provided then call it
                nonlocal sections
                if section_f:
                    sections = (section_f(bound_args.arguments), )
                # sections may be a string
                if isinstance(sections, str):
                    sections = (sections,)
                # if one of arguments is spec the use it as initial value
                if spec_arg:
                    config = bound_args.arguments.get(spec_arg.name, None)
                # resolve SPEC, also provide section_context with pipeline_name
                if pipeline_name_arg:
                    section_context.pipeline_name = bound_args.arguments.get(pipeline_name_arg.name, pipeline_name_arg_default)
                with inject_section(section_context):
                    config = resolve_configuration(config or SPEC(), sections=sections, explicit_value=bound_args.arguments)
            resolved_params = dict(config)
            bound_args.apply_defaults()
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


def last_config(**kwargs: Any) -> Any:
    return kwargs[_LAST_DLT_CONFIG]


def get_orig_args(**kwargs: Any) -> Tuple[Tuple[Any], DictStrAny]:
    return kwargs[_ORIGINAL_ARGS]  # type: ignore
