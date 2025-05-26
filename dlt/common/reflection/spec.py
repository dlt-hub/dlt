import re
import typing
import inspect
from typing import Dict, Tuple, Type, Any, Optional, get_args, get_origin
from inspect import Signature, Parameter

from dlt.common.typing import (
    AnyType,
    AnyFun,
    ConfigValueSentinel,
    NoneType,
    TSecretValue,
    Annotated,
    SecretSentinel,
)
from dlt.common.configuration import configspec, is_valid_hint, is_secret_hint
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.utils import get_callable_name

# [^.^_]+ splits by . or _
_SLEEPING_CAT_SPLIT = re.compile("[^.^_]+")


def get_spec_name_from_f(f: AnyFun, kind: str = "Configuration") -> str:
    func_name = get_callable_name(f, "__qualname__").replace(
        "<locals>.", ""
    )  # func qual name contains position in the module, separated by dots

    def _first_up(s: str) -> str:
        return s[0].upper() + s[1:]

    return "".join(map(_first_up, _SLEEPING_CAT_SPLIT.findall(func_name))) + kind


def spec_from_signature(
    f: AnyFun,
    sig: Signature,
    include_defaults: bool = True,
    base: Type[BaseConfiguration] = BaseConfiguration,
    config_defaults: Dict[str, Any] = None,
) -> Tuple[Type[BaseConfiguration], Dict[str, Any]]:
    """Creates a SPEC on base `base1 for a function `f` with signature `sig`.

    All the arguments in `sig` that are valid SPEC hints and have defaults will be part of the SPEC.
    Special default markers for required SPEC fields `dlt.secrets.value` and `dlt.config.value` are sentinel
    string values with a type set to Any during typechecking. The sentinels are defined in dlt.common.typing module.

    The name of a SPEC type is inferred from qualname of `f` and type will refer to `f` module and is unique
    for a module. NOTE: the SPECS are cached in the module by using name as an id.

    Return value is a tuple of SPEC and SPEC fields created from a `sig`.
    """
    name = get_spec_name_from_f(f)
    module = inspect.getmodule(f)
    base_fields = base.get_resolvable_fields()

    # check if spec for that function exists
    spec_id = name  # f"SPEC_{name}_kw_only_{kw_only}"
    if hasattr(module, spec_id):
        MOD_SPEC: Type[BaseConfiguration] = getattr(module, spec_id)
        return MOD_SPEC, MOD_SPEC.get_resolvable_fields()

    # synthesize configuration from the signature
    new_fields: Dict[str, Any] = {}
    sig_base_fields: Dict[str, Any] = {}
    annotations: Dict[str, Any] = {}

    for p in sig.parameters.values():
        # skip *args and **kwargs, skip typical method params
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL) and p.name not in [
            "self",
            "cls",
        ]:
            field_type = AnyType if p.annotation == Parameter.empty else p.annotation
            # keep the base fields if sig not annotated
            if (
                p.name in base_fields
                and field_type is AnyType
                and isinstance(p.default, (NoneType, ConfigValueSentinel))
            ):
                sig_base_fields[p.name] = base_fields[p.name]
                continue
            # only valid hints and parameters with defaults are eligible
            if is_valid_hint(field_type) and p.default != Parameter.empty:
                type_from_literal: AnyType = None
                # make type optional if explicit None is provided as default
                if p.default is None:
                    # optional type
                    field_type = Optional[field_type]
                elif isinstance(p.default, ConfigValueSentinel):
                    # check if the defaults were attributes of the form .config.value or .secrets.value
                    type_from_literal = p.default.default_type
                    if type_from_literal is TSecretValue:
                        # override type with secret value if secrets.value
                        if not is_secret_hint(field_type):
                            if field_type is AnyType:
                                field_type = TSecretValue
                            else:
                                # generate typed SecretValue
                                field_type = Annotated[field_type, SecretSentinel]
                    # remove sentinel from default
                    p = p.replace(default=None)
                elif field_type is AnyType:
                    # try to get type from default
                    field_type = type(p.default)

                if include_defaults or type_from_literal is not None:
                    # set annotations
                    annotations[p.name] = field_type
                    # set field with default value
                    new_fields[p.name] = p.default
                    # print(f"Param {p.name} is {field_type}: {p.default} due to {include_defaults} or {type_from_literal}")
                if config_defaults and p.name in config_defaults:
                    # verify that the type of the value is an instance of the field type
                    check_default_type(field_type, config_defaults, p)

    signature_fields = {**sig_base_fields, **new_fields}

    # new type goes to the module where sig was declared
    new_fields["__module__"] = module.__name__
    # set annotations so they are present in __dict__
    new_fields["__annotations__"] = annotations
    # synthesize type
    T: Type[BaseConfiguration] = type(name, (base,), new_fields)
    if config_defaults:
        T.__config_gen_annotations__ = config_defaults
    SPEC = configspec()(T)
    # add to the module
    setattr(module, spec_id, SPEC)
    return SPEC, signature_fields

def check_default_type(field_type: Any, config_defaults: Dict[str, Any], p: Parameter) -> None:
    # hacky code, needs to be more thorough
    value = config_defaults[p.name]
    origin = get_origin(field_type)
    args = get_args(field_type)
    if origin:
        # For example, origin is list for List[str]
        if not isinstance(value, origin):
            raise TypeError(
                f"Invalid default config: Expected type {field_type} for {p.name}, got {type(value)}"
            )
        # Optionally, check inner types (e.g., all elements are str)
        if origin in (list, tuple) and args:
            if not all(isinstance(item, args[0]) for item in value):
                raise TypeError(
                    f"Invalid default config: Expected elements of type {args[0]} for {p.name}, got {value}"
                )
    else:
        if not isinstance(value, field_type):
            raise TypeError(
                f"Invalid default config: Expected type {field_type} for {p.name}, got {type(value)}"
            )