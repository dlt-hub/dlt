import ast
import dataclasses
import inspect
import sys
import semver
from collections.abc import Mapping as C_Mapping
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Type, TypeVar, get_origin

from dlt.common import json, logger
from dlt.common.typing import TSecretValue, is_optional_type, extract_inner_type
from dlt.common.schema.utils import coerce_type, py_type_to_sc_type

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, CredentialsConfiguration, configspec
from dlt.common.configuration.inject import Container
from dlt.common.configuration.providers.configuration import ConfigProvidersConfiguration
from dlt.common.configuration.exceptions import (LookupTrace, ConfigEntryMissingException, ConfigurationWrongTypeException, ConfigEnvValueCannotBeCoercedException, ValueNotSecretException)

CHECK_INTEGRITY_F: str = "check_integrity"
TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def make_configuration(config: TConfiguration, initial_value: Any = None, accept_partial: bool = False) -> TConfiguration:
    if not isinstance(config, BaseConfiguration):
        raise ConfigurationWrongTypeException(type(config))

    # parse initial value if possible
    if initial_value is not None:
        try:
            config.from_native_representation(initial_value)
        except (NotImplementedError, ValueError):
            # if parsing failed and initial_values is dict then apply
            # TODO: we may try to parse with json here if str
            if isinstance(initial_value, C_Mapping):
                config.update(initial_value)
            else:
                raise InvalidInitialValue(type(config), type(initial_value))

    try:
        _resolve_config_fields(config, accept_partial)
        _check_configuration_integrity(config)
        # full configuration was resolved
        config.__is_resolved__ = True
    except ConfigEntryMissingException:
        if not accept_partial:
            raise
    _add_module_version(config)

    return config


def deserialize_value(key: str, value: Any, hint: Type[Any]) -> Any:
    try:
        if hint != Any:
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
                    value = coerce_type(hint_dt, value_dt, value)
        return value
    except ConfigEnvValueCannotBeCoercedException:
        raise
    except Exception as exc:
        raise ConfigEnvValueCannotBeCoercedException(key, value, hint) from exc


def serialize_value(value: Any) -> Any:
    if value is None:
        raise ValueError(value)
    # return literal for tuples
    if isinstance(value, tuple):
        return str(value)
    # coerce type to text which will use json for mapping and sequences
    value_dt = py_type_to_sc_type(type(value))
    return coerce_type("text", value_dt, value)


def _add_module_version(config: BaseConfiguration) -> None:
    try:
        v = sys._getframe(1).f_back.f_globals["__version__"]
        semver.VersionInfo.parse(v)
        setattr(config, "_version", v)  # noqa: B010
    except KeyError:
        pass


def _resolve_config_fields(config: BaseConfiguration, accept_partial: bool) -> None:
    fields = config.get_resolvable_fields()
    unresolved_fields: Dict[str, Sequence[LookupTrace]] = {}

    for key, hint in fields.items():
        # get default value
        current_value = getattr(config, key, None)
        # check if hint optional
        is_optional = is_optional_type(hint)
        # accept partial becomes True if type if optional so we do not fail on optional configs that do not resolve fully
        accept_partial = accept_partial or is_optional
        # if actual value is BaseConfiguration, resolve that instance
        if isinstance(current_value, BaseConfiguration):
            current_value = make_configuration(current_value, accept_partial=accept_partial)
        else:
            # resolve key value via active providers
            value, traces = _resolve_single_field(key, hint, config.__namespace__)

            # log trace
            if logger.is_logging() and logger.log_level() == "DEBUG":
                logger.debug(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
                for tr in traces:
                    logger.debug(str(tr))

            # extract hint from Optional / Literal / NewType hints
            hint = extract_inner_type(hint)
            # extract origin from generic types
            hint = get_origin(hint) or hint
            # if hint is BaseConfiguration then resolve it recursively
            if inspect.isclass(hint) and issubclass(hint, BaseConfiguration):
                # create new instance and pass value from the provider as initial
                current_value = make_configuration(hint(), initial_value=value or current_value, accept_partial=accept_partial)
            else:
                if value is not None:
                    current_value = deserialize_value(key, value, hint)
        # collect unresolved fields
        if not is_optional and current_value is None:
            unresolved_fields[key] = traces
        # set resolved value in config
        setattr(config, key, current_value)
    if unresolved_fields:
        raise ConfigEntryMissingException(type(config).__name__, unresolved_fields)


# def _is_config_bounded(config: BaseConfiguration, fields: Mapping[str, type]) -> None:
#     # TODO: here we assume all keys are taken from environ provider, that should change when we introduce more providers
#     # environ.get_key_name(key, config.__namespace__)
#     _unbound_attrs = [
#         key for key in fields if getattr(config, key) is None and not is_optional_type(fields[key])
#     ]

#     if len(_unbound_attrs) > 0:
#         raise ConfigEntryMissingException(_unbound_attrs, config.__namespace__)


def _check_configuration_integrity(config: BaseConfiguration) -> None:
    # python multi-inheritance is cooperative and this would require that all configurations cooperatively
    # call each other check_integrity. this is not at all possible as we do not know which configs in the end will
    # be mixed together.

    # get base classes in order of derivation
    mro = type.mro(type(config))
    for c in mro:
        # check if this class implements check_integrity (skip pure inheritance to not do double work)
        if CHECK_INTEGRITY_F in c.__dict__ and callable(getattr(c, CHECK_INTEGRITY_F)):
            # pass right class instance
            c.__dict__[CHECK_INTEGRITY_F](config)


def _get_resolvable_fields(config: BaseConfiguration) -> Dict[str, type]:
    return {f.name:f.type for f in dataclasses.fields(config) if not f.name.startswith("__")}


@configspec
class ConfigNamespacesConfiguration(BaseConfiguration):
    pipeline_name: Optional[str]
    namespaces: List[str]

    def __init__(self) -> None:
        super().__init__()
        self.namespaces = []


def _resolve_single_field(key: str, hint: Type[Any], namespace: str, *namespaces: str) -> Tuple[Optional[Any], List[LookupTrace]]:
    # get providers from container
    providers = Container()[ConfigProvidersConfiguration].providers
    # get additional namespaces to look in from container
    context_namespaces = Container()[ConfigNamespacesConfiguration].namespaces

    # start looking from the top provider with most specific set of namespaces first
    traces: List[LookupTrace] = []
    value = None
    ns = [*namespaces, *context_namespaces]
    for provider in providers:
        while True:
            # first namespace always present
            _ns_t = (namespace, *ns) if namespace else ns
            value, ns_key = provider.get_value(key, hint, *_ns_t)
            # create trace
            traces.append(LookupTrace(provider.name, _ns_t, ns_key, value))
            # if secret is obtained from non secret provider, we must fail
            if value is not None and not provider.is_secret and (hint is TSecretValue or (inspect.isclass(hint) and issubclass(hint, CredentialsConfiguration))):
                raise ValueNotSecretException(provider.name, ns_key)
            if len(ns) == 0 or value is not None:
                break
            ns.pop()

    return value, traces
