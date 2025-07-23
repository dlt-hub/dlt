import os
import ast
import contextlib
import tomlkit
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Sequence,
    Literal,
)

import yaml

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.provider import EXPLICIT_VALUES_PROVIDER_NAME
from dlt.common.json import json
from dlt.common.typing import AnyType, DictStrAny, TAny, is_any_type, get_args, get_origin
from dlt.common.data_types import coerce_value, py_type_to_sc_type
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.exceptions import ConfigValueCannotBeCoercedException, LookupTrace
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    ContainerInjectableContext,
    is_base_configuration_inner_hint,
    configspec,
)


class ResolvedValueTrace(NamedTuple):
    key: str
    value: Any
    default_value: Any
    hint: AnyType
    sections: Sequence[str]
    provider_name: str
    config: BaseConfiguration

    def is_resolved(self) -> bool:
        # explicit values if present are always resolved
        return self.value is not None or self.provider_name == EXPLICIT_VALUES_PROVIDER_NAME


@configspec
class TraceLogContext(ContainerInjectableContext):
    """Stores log of all config resolver traces, per thread so parallel pipelines may log to it"""

    resolved_traces: List[ResolvedValueTrace] = None
    """Traces with resolved values"""
    all_traces: List[ResolvedValueTrace] = None
    """All logged traces"""

    logging_enabled: bool = True
    """Collect logs by default"""

    def clear(self) -> None:
        self.resolved_traces = []
        self.all_traces = []

    def log(self, trace: ResolvedValueTrace) -> None:
        if not self.logging_enabled:
            return
        if trace.is_resolved():
            self.resolved_traces.append(trace)
        self.all_traces.append(trace)

    def __init__(self) -> None:
        self.clear()

    @staticmethod
    def _get_log_as_dict(traces: List[ResolvedValueTrace]) -> Dict[str, ResolvedValueTrace]:
        """Converts logs into layout path - value dict"""
        return {f'{".".join(t.sections)}.{t.key}': t for t in traces}


def deserialize_value(key: str, value: Any, hint: Type[TAny]) -> TAny:
    try:
        if not is_any_type(hint):
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

            # eval only if value is string and hint is "json"
            if value_dt == "text" and hint_dt == "json":
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
                # for types that are not nested, reuse schema coercion rules
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
    """Parse and cast str `value` to bool, int, float and json

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
    trace_logger = get_resolved_traces()
    for trace in traces:
        trace_logger.log(
            ResolvedValueTrace(
                key,
                trace.value,
                default_value,
                hint,
                trace.sections,
                trace.provider,
                config,
            )
        )


def get_resolved_traces() -> TraceLogContext:
    """Gets trace logging context, per thread, stopped by default"""
    # may create default value
    return Container()[TraceLogContext]


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
                    # skip non-serializable fields
                    with contextlib.suppress(TypeError):
                        os.environ[env_key] = serialize_value(v)
