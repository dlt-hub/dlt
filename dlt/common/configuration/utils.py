import os
import ast
import contextlib
import tomlkit
from typing import (
    Any,
    Dict,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Sequence,
    get_args,
    Literal,
    get_origin,
)
from collections.abc import Mapping as C_Mapping

import yaml

from dlt.common.json import json
from dlt.common.typing import AnyType, DictStrAny, TAny
from dlt.common.data_types import coerce_value, py_type_to_sc_type
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.exceptions import ConfigValueCannotBeCoercedException, LookupTrace
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    is_base_configuration_inner_hint,
)


class ResolvedValueTrace(NamedTuple):
    key: str
    value: Any
    default_value: Any
    hint: AnyType
    sections: Sequence[str]
    provider_name: str
    config: BaseConfiguration


_RESOLVED_TRACES: Dict[str, ResolvedValueTrace] = {}  # stores all the resolved traces


def deserialize_value(key: str, value: Any, hint: Type[TAny]) -> TAny:
    try:
        if hint != Any:
            # if deserializing to base configuration, try parse the value
            if is_base_configuration_inner_hint(hint):
                c = hint()
                if isinstance(value, dict):
                    c.update(value)
                else:
                    try:
                        c.parse_native_representation(value)
                    except (ValueError, NotImplementedError):
                        # maybe try again with json parse
                        with contextlib.suppress(ValueError):
                            c_v = json.loads(value)
                            # only lists and dictionaries count
                            if isinstance(c_v, dict):
                                c.update(c_v)
                            else:
                                raise
                return c  # type: ignore

            literal_values: Tuple[Any, ...] = ()
            if get_origin(hint) is Literal:
                # Literal fields are validated against the literal values
                literal_values = get_args(hint)
                hint_origin = type(literal_values[0])
            else:
                hint_origin = hint

            # coerce value
            hint_dt = py_type_to_sc_type(hint_origin)
            value_dt = py_type_to_sc_type(type(value))

            # eval only if value is string and hint is "complex"
            if value_dt == "text" and hint_dt == "complex":
                if hint_origin is tuple:
                    # use literal eval for tuples
                    value = ast.literal_eval(value)
                else:
                    # use json for sequences and mappings
                    value = json.loads(value)
                # exact types must match
                if not isinstance(value, hint_origin):
                    raise ValueError(value)
            else:
                # for types that are not complex, reuse schema coercion rules
                if value_dt != hint_dt:
                    value = coerce_value(hint_dt, value_dt, value)
                if literal_values and value not in literal_values:
                    raise ConfigValueCannotBeCoercedException(key, value, hint)
        return value  # type: ignore
    except ConfigValueCannotBeCoercedException:
        raise
    except Exception as exc:
        raise ConfigValueCannotBeCoercedException(key, value, hint) from exc


def serialize_value(value: Any) -> str:
    if value is None:
        raise ValueError(value)
    # return literal for tuples
    if isinstance(value, tuple):
        return str(value)
    if isinstance(value, BaseConfiguration):
        try:
            return str(value.to_native_representation())
        except NotImplementedError:
            # no native representation: use dict
            value = dict(value)
    # coerce type to text which will use json for mapping and sequences
    value_dt = py_type_to_sc_type(type(value))
    return coerce_value("text", value_dt, value)  # type: ignore[no-any-return]


def auto_cast(value: str) -> Any:
    """Parse and cast str `value` to bool, int, float and complex (via JSON)

    F[f]alse and T[t]rue strings are cast to bool values
    """
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    with contextlib.suppress(ValueError):
        return coerce_value("bigint", "text", value)
    with contextlib.suppress(ValueError):
        return coerce_value("double", "text", value)
    with contextlib.suppress(ValueError):
        c_v = json.loads(value)
        # only lists and dictionaries count
        if isinstance(c_v, (list, dict)):
            return c_v
    return value


def auto_config_fragment(value: str) -> Optional[DictStrAny]:
    """Tries to parse config fragment assuming toml, yaml and json formats

    Only dicts are considered valid fragments.
    None is returned when not a fragment
    """
    try:
        return tomlkit.parse(value).unwrap()
    except ValueError:
        pass
    with contextlib.suppress(Exception):
        c_v = yaml.safe_load(value)
        if isinstance(c_v, dict):
            return c_v
    with contextlib.suppress(ValueError):
        c_v = json.loads(value)
        # only lists and dictionaries count
        if isinstance(c_v, dict):
            return c_v
    return None


def log_traces(
    config: Optional[BaseConfiguration],
    key: str,
    hint: Type[Any],
    value: Any,
    default_value: Any,
    traces: Sequence[LookupTrace],
) -> None:
    # if logger.is_logging() and logger.log_level() == "DEBUG" and config:
    #     logger.debug(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
    # print(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
    # for tr in traces:
    #     # print(str(tr))
    #     logger.debug(str(tr))
    # store all traces with resolved values
    resolved_trace = next((trace for trace in traces if trace.value is not None), None)
    if resolved_trace is not None:
        path = f'{".".join(resolved_trace.sections)}.{key}'
        _RESOLVED_TRACES[path] = ResolvedValueTrace(
            key,
            resolved_trace.value,
            default_value,
            hint,
            resolved_trace.sections,
            resolved_trace.provider,
            config,
        )


def get_resolved_traces() -> Dict[str, ResolvedValueTrace]:
    return _RESOLVED_TRACES


def add_config_to_env(config: BaseConfiguration, sections: Tuple[str, ...] = ()) -> None:
    """Writes values in configuration back into environment using the naming convention of EnvironProvider. Will descend recursively if embedded BaseConfiguration instances are found"""
    if config.__section__:
        sections += (config.__section__,)
    return add_config_dict_to_env(dict(config), sections, overwrite_keys=True)


def add_config_dict_to_env(
    dict_: Mapping[str, Any],
    sections: Tuple[str, ...] = (),
    overwrite_keys: bool = False,
    destructure_dicts: bool = True,
) -> None:
    """Writes values in dict_ back into environment using the naming convention of EnvironProvider. Applies `sections` if specified. Does not overwrite existing keys by default"""
    for k, v in dict_.items():
        if isinstance(v, BaseConfiguration):
            if not v.__section__:
                embedded_sections = sections + (k,)
            else:
                embedded_sections = sections
            add_config_to_env(v, embedded_sections)
        else:
            env_key = EnvironProvider.get_key_name(k, *sections)
            if env_key not in os.environ or overwrite_keys:
                if v is None:
                    os.environ.pop(env_key, None)
                elif isinstance(v, dict) and destructure_dicts:
                    add_config_dict_to_env(
                        v,
                        sections + (k,),
                        overwrite_keys=overwrite_keys,
                        destructure_dicts=destructure_dicts,
                    )
                else:
                    os.environ[env_key] = serialize_value(v)
