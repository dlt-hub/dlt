import os
import ast
import contextlib
from typing import Any, Dict, Mapping, NamedTuple, Type, Sequence, get_origin

from dlt.common import json, logger
from dlt.common.typing import AnyType, extract_inner_type, TAny
from dlt.common.schema.utils import coerce_value, py_type_to_sc_type
from dlt.common.configuration import DOT_DLT
from dlt.common.configuration.exceptions import ConfigValueCannotBeCoercedException, LookupTrace
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, get_config_if_union, is_base_configuration_hint


class ResolvedValueTrace(NamedTuple):
    key: str
    value: Any
    default_value: Any
    hint: AnyType
    namespaces: Sequence[str]
    provider_name: str
    config: BaseConfiguration


_RESOLVED_TRACES: Dict[str, ResolvedValueTrace] = {}  # stores all the resolved traces


def deserialize_value(key: str, value: Any, hint: Type[TAny]) -> TAny:
    try:
        if hint != Any:
            # if deserializing to base configuration, try parse the value
            if is_base_configuration_hint(hint):
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

            # coerce value
            hint_dt = py_type_to_sc_type(hint)
            value_dt = py_type_to_sc_type(type(value))

            # eval only if value is string and hint is "complex"
            if value_dt == "text" and hint_dt == "complex":
                if hint is tuple:
                    # use literal eval for tuples
                    value = ast.literal_eval(value)
                else:
                    # use json for sequences and mappings
                    value = json.loads(value)
                # exact types must match
                if not isinstance(value, hint):
                    raise ValueError(value)
            else:
                # for types that are not complex, reuse schema coercion rules
                if value_dt != hint_dt:
                    value = coerce_value(hint_dt, value_dt, value)
        return value  # type: ignore
    except ConfigValueCannotBeCoercedException:
        raise
    except Exception as exc:
        raise ConfigValueCannotBeCoercedException(key, value, hint) from exc


def serialize_value(value: Any) -> Any:
    if value is None:
        raise ValueError(value)
    # return literal for tuples
    if isinstance(value, tuple):
        return str(value)
    if isinstance(value, BaseConfiguration):
        try:
            return value.to_native_representation()
        except NotImplementedError:
            # no native representation: use dict
            value = dict(value)
    # coerce type to text which will use json for mapping and sequences
    value_dt = py_type_to_sc_type(type(value))
    return coerce_value("text", value_dt, value)


def extract_inner_hint(hint: Type[Any]) -> Type[Any]:
    # extract hint from Optional / Literal / NewType hints
    inner_hint = extract_inner_type(hint)
    # get base configuration from union type
    inner_hint = get_config_if_union(inner_hint) or inner_hint
    # extract origin from generic types (ie List[str] -> List)
    return get_origin(inner_hint) or inner_hint


def log_traces(config: BaseConfiguration, key: str, hint: Type[Any], value: Any, default_value: Any, traces: Sequence[LookupTrace]) -> None:
    if logger.is_logging() and logger.log_level() == "DEBUG":
        logger.debug(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
        # print(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
        for tr in traces:
            # print(str(tr))
            logger.debug(str(tr))
    # store all traces with resolved values
    resolved_trace = next((trace for trace in traces if trace.value is not None), None)
    if resolved_trace is not None:
        path = f'{".".join(resolved_trace.namespaces)}.{key}'
        _RESOLVED_TRACES[path] = ResolvedValueTrace(key, resolved_trace.value, default_value, hint, resolved_trace.namespaces, resolved_trace.provider, config)


def get_resolved_traces() -> Mapping[str, ResolvedValueTrace]:
    return _RESOLVED_TRACES


def make_dot_dlt_path(path: str) -> str:
    return os.path.join(DOT_DLT, path)


def current_dot_dlt_path() -> str:
    """Returns current path to the .dlt folder. The path is computed with the relation to the current working directory."""
    # entry_path = main_module_file_path()
    # if entry_path:
    #     path, _ = os.path.split(entry_path)
    # else:
    #     path = None
    return os.path.abspath(os.path.join(".", DOT_DLT))
