import re
import inspect
from typing import Dict, List, Type, Any, Optional, NewType
from inspect import Signature, Parameter

from dlt.common.typing import AnyType, AnyFun, TSecretValue
from dlt.common.configuration import configspec, is_valid_hint, is_secret_hint
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration.accessors import DLT_CONFIG_VALUE, DLT_SECRETS_VALUE
from dlt.common.reflection.utils import get_func_def_node, get_literal_defaults
from dlt.common.utils import get_callable_name

# [^.^_]+ splits by . or _
_SLEEPING_CAT_SPLIT = re.compile("[^.^_]+")


def _get_spec_name_from_f(f: AnyFun) -> str:
    func_name = get_callable_name(f, "__qualname__").replace(
        "<locals>.", ""
    )  # func qual name contains position in the module, separated by dots

    def _first_up(s: str) -> str:
        return s[0].upper() + s[1:]

    return "".join(map(_first_up, _SLEEPING_CAT_SPLIT.findall(func_name))) + "Configuration"


def spec_from_signature(
    f: AnyFun, sig: Signature, include_defaults: bool = True
) -> Type[BaseConfiguration]:
    name = _get_spec_name_from_f(f)
    module = inspect.getmodule(f)

    # check if spec for that function exists
    spec_id = name  # f"SPEC_{name}_kw_only_{kw_only}"
    if hasattr(module, spec_id):
        return getattr(module, spec_id)  # type: ignore

    # find all the arguments that have following defaults
    literal_defaults: Dict[str, str] = None

    def dlt_config_literal_to_type(arg_name: str) -> AnyType:
        nonlocal literal_defaults

        if literal_defaults is None:
            try:
                node = get_func_def_node(f)
                literal_defaults = get_literal_defaults(node)
            except Exception:
                # ignore exception during parsing. it is almost impossible to test all cases of function definitions
                literal_defaults = {}

        if arg_name in literal_defaults:
            literal_default = literal_defaults[arg_name]
            if literal_default.endswith(DLT_CONFIG_VALUE):
                return AnyType
            if literal_default.endswith(DLT_SECRETS_VALUE):
                return TSecretValue
        return None

    # synthesize configuration from the signature
    fields: Dict[str, Any] = {}
    annotations: Dict[str, Any] = {}

    for p in sig.parameters.values():
        # skip *args and **kwargs, skip typical method params
        if p.kind not in (Parameter.VAR_KEYWORD, Parameter.VAR_POSITIONAL) and p.name not in [
            "self",
            "cls",
        ]:
            field_type = AnyType if p.annotation == Parameter.empty else p.annotation
            # only valid hints and parameters with defaults are eligible
            if is_valid_hint(field_type) and p.default != Parameter.empty:
                # try to get type from default
                if field_type is AnyType and p.default is not None:
                    field_type = type(p.default)
                # make type optional if explicit None is provided as default
                type_from_literal: AnyType = None
                if p.default is None:
                    # check if the defaults were attributes of the form .config.value or .secrets.value
                    type_from_literal = dlt_config_literal_to_type(p.name)
                    if type_from_literal is None:
                        # optional type
                        field_type = Optional[field_type]
                    elif type_from_literal is TSecretValue:
                        # override type with secret value if secrets.value
                        # print(f"Param {p.name} is REQUIRED: secrets literal")
                        if not is_secret_hint(field_type):
                            if field_type is AnyType:
                                field_type = TSecretValue
                            else:
                                # generate typed SecretValue
                                field_type = NewType("TSecretValue", field_type)  # type: ignore
                    else:
                        # keep type mandatory if config.value
                        # print(f"Param {p.name} is REQUIRED: config literal")
                        pass
                if include_defaults or type_from_literal is not None:
                    # set annotations
                    annotations[p.name] = field_type
                    # set field with default value
                    fields[p.name] = p.default

    if not fields:
        return None

    # new type goes to the module where sig was declared
    fields["__module__"] = module.__name__
    # set annotations so they are present in __dict__
    fields["__annotations__"] = annotations
    # synthesize type
    T: Type[BaseConfiguration] = type(name, (BaseConfiguration,), fields)
    SPEC = configspec()(T)
    # add to the module
    setattr(module, spec_id, SPEC)
    return SPEC
