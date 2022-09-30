import os
import inspect
import dataclasses
import tomlkit
from inspect import Signature, Parameter
from typing import Any, List, Type
# from makefun import wraps
from functools import wraps

from dlt.common.typing import DictStrAny, StrAny, TAny, TFun
from dlt.common.configuration import BaseConfiguration
from dlt.common.configuration.resolve import NON_EVAL_TYPES, make_configuration, SIMPLE_TYPES

# _POS_PARAMETER_KINDS = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD, Parameter.VAR_POSITIONAL)

def _read_toml(file_name: str) -> StrAny:
    config_file_path = os.path.abspath(os.path.join(".", "experiments/.dlt", file_name))

    if os.path.isfile(config_file_path):
        with open(config_file_path, "r", encoding="utf-8") as f:
            # use whitespace preserving parser
            return tomlkit.load(f)
    else:
        return {}


def get_config_from_toml():
    pass


def get_config(SPEC: Type[TAny], key: str = None, namespace: str = None, initial_value: Any = None, accept_partial: bool = False) -> TAny:
    # TODO: implement key and namespace
    return make_configuration(SPEC(), initial_value=initial_value, accept_partial=accept_partial)


def spec_from_dict():
    pass


def spec_from_signature(name: str, sig: Signature) -> Type[BaseConfiguration]:
    # synthesize configuration from the signature
    fields: List[dataclasses.Field] = []
    for p in sig.parameters.values():
        # skip *args and **kwargs
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL) and p.name not in ["self", "cls"]:
            field_type = Any if p.annotation == Parameter.empty else p.annotation
            if field_type in SIMPLE_TYPES or field_type in NON_EVAL_TYPES or issubclass(field_type, BaseConfiguration):
                field_default = None if p.default == Parameter.empty else dataclasses.field(default=p.default)
                if field_default:
                    # correct the type if Any
                    if field_type is Any:
                        field_type = type(p.default)
                    fields.append((p.name, field_type, field_default))
                else:
                    fields.append((p.name, field_type))
    print(fields)
    SPEC = dataclasses.make_dataclass(name + "_CONFIG", fields, bases=(BaseConfiguration,), init=False)
    print("synthesized")
    print(SPEC)
    # print(SPEC())
    return SPEC


def with_config(func = None, /, spec: Type[BaseConfiguration] = None) ->  TFun:

    def decorator(f: TFun) -> TFun:
        SPEC: Type[BaseConfiguration] = None
        sig: Signature = inspect.signature(f)
        kwargs_par = next((p for p in sig.parameters.values() if p.kind == Parameter.VAR_KEYWORD), None)
        # pos_params = [p.name for p in sig.parameters.values() if p.kind in _POS_PARAMETER_KINDS]
        # kw_params = [p.name for p in sig.parameters.values() if p.kind not in _POS_PARAMETER_KINDS]

        if spec is None:
            SPEC = spec_from_signature(f.__name__, sig)
        else:
            SPEC = spec

        @wraps(f)
        def _wrap(*args: Any, **kwargs: Any) -> Any:
            # for calls providing all parameters to the func, configuration may not be resolved
            # if len(args) + len(kwargs) == len(sig.parameters):
            #     return f(*args, **kwargs)

            # bind parameters to signature
            bound_args = sig.bind_partial(*args, **kwargs)
            bound_args.apply_defaults()
            # resolve SPEC
            config = get_config(SPEC, SPEC, initial_value=bound_args.arguments)
            resolved_params = dict(config)
            print("RESOLVED")
            print(resolved_params)
            # overwrite or add resolved params
            for p in sig.parameters.values():
                if p.name in resolved_params:
                    bound_args.arguments[p.name] = resolved_params.pop(p.name)
            # pass all other config parameters into kwargs if present
            if kwargs_par is not None:
                bound_args.arguments[kwargs_par.name].update(resolved_params)
            # call the function with injected config
            return f(*bound_args.args, **bound_args.kwargs)

        return _wrap

    # See if we're being called as @with_config or @with_config().
    if func is None:
        # We're called with parens.
        return decorator

    if not callable(func):
        raise ValueError("First parameter to the with_config must be callable ie. by using it as function decorator")

    # We're called as @with_config without parens.
    return decorator(func)
