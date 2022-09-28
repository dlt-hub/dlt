import dataclasses
import inspect
import sys
import semver
from typing import Any, Dict, List, Mapping, Type, TypeVar

from dlt.common.typing import is_optional_type, is_literal_type
from dlt.common.configuration import BaseConfiguration
from dlt.common.configuration.providers import environ
from dlt.common.configuration.exceptions import (ConfigEntryMissingException, ConfigurationWrongTypeException, ConfigEnvValueCannotBeCoercedException)

SIMPLE_TYPES: List[Any] = [int, bool, list, dict, tuple, bytes, set, float]
# those types and Optionals of those types should not be passed to eval function
NON_EVAL_TYPES = [str, None, Any]
# allows to coerce (type1 from type2)
ALLOWED_TYPE_COERCIONS = [(float, int), (str, int), (str, float)]
CHECK_INTEGRITY_F: str = "check_integrity"

TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def make_configuration(config: TConfiguration, initial_value: Any = None, accept_partial: bool = False) -> TConfiguration:
    if not isinstance(config, BaseConfiguration):
        raise ConfigurationWrongTypeException(type(config))

    # get fields to resolve as per dataclasses PEP
    fields = _get_resolvable_fields(config)
    # parse initial value if possible
    if initial_value is not None:
        try:
            config.from_native_representation(initial_value)
        except (NotImplementedError, ValueError):
            # if parsing failed and initial_values is dict then apply
            if isinstance(initial_value, Mapping):
                config.update(initial_value)

    _resolve_config_fields(config, fields, accept_partial)
    try:
        _is_config_bounded(config, fields)
        _check_configuration_integrity(config)
        # full configuration was resolved
        config.__is_partial__ = False
    except ConfigEntryMissingException:
        if not accept_partial:
            raise
    _add_module_version(config)

    return config


def _add_module_version(config: TConfiguration) -> None:
    try:
        v = sys._getframe(1).f_back.f_globals["__version__"]
        semver.VersionInfo.parse(v)
        setattr(config, "_version", v)  # noqa: B010
    except KeyError:
        pass


def _resolve_config_fields(config: TConfiguration, fields: Mapping[str, type], accept_partial: bool) -> None:
    for key, hint in fields.items():
        # get default value
        resolved_value = getattr(config, key, None)
        # resolve key value via active providers
        value = environ.get_key(key, hint, config.__namespace__)

        # extract hint from Optional / NewType hints
        hint = _extract_simple_type(hint)
        # if hint is BaseConfiguration then resolve it recursively
        if inspect.isclass(hint) and issubclass(hint, BaseConfiguration):
            if isinstance(resolved_value, BaseConfiguration):
                # if actual value is BaseConfiguration, resolve that instance
                resolved_value = make_configuration(resolved_value, accept_partial=accept_partial)
            else:
                # create new instance and pass value from the provider as initial
                resolved_value = make_configuration(hint(), initial_value=value or resolved_value, accept_partial=accept_partial)
        else:
            if value is not None:
                resolved_value = _coerce_single_value(key, value, hint)
        # set value resolved value
        setattr(config, key, resolved_value)


def _coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    try:
        if hint not in NON_EVAL_TYPES:
            # create primitive types out of strings
            typed_value = eval(value)  # nosec
            # for primitive types check coercion
            if hint in SIMPLE_TYPES and type(typed_value) != hint:
                # allow some exceptions
                coerce_exception = next(
                    (e for e in ALLOWED_TYPE_COERCIONS if e == (hint, type(typed_value))), None)
                if coerce_exception:
                    return hint(typed_value)
                else:
                    raise ConfigEnvValueCannotBeCoercedException(key, typed_value, hint)
            return typed_value
        else:
            return value
    except ConfigEnvValueCannotBeCoercedException:
        raise
    except Exception as exc:
        raise ConfigEnvValueCannotBeCoercedException(key, value, hint) from exc


def _is_config_bounded(config: TConfiguration, fields: Mapping[str, type]) -> None:
    # TODO: here we assume all keys are taken from environ provider, that should change when we introduce more providers
    _unbound_attrs = [
        environ.get_key_name(key, config.__namespace__) for key in fields if getattr(config, key) is None and not is_optional_type(fields[key])
    ]

    if len(_unbound_attrs) > 0:
        raise ConfigEntryMissingException(_unbound_attrs, config.__namespace__)


def _check_configuration_integrity(config: TConfiguration) -> None:
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


def _get_resolvable_fields(config: TConfiguration) -> Dict[str, type]:
    return {f.name:f.type for f in dataclasses.fields(config) if not f.name.startswith("__")}


def _extract_simple_type(hint: Type[Any]) -> Type[Any]:
    # extract optional type and call recursively
    if is_literal_type(hint):
        # assume that all literals are of the same type
        return _extract_simple_type(type(hint.__args__[0]))
    if is_optional_type(hint):
        # todo: use `get_args` in python 3.8
        return _extract_simple_type(hint.__args__[0])
    if not hasattr(hint, "__supertype__"):
        return hint
    # descend into supertypes of NewType
    return _extract_simple_type(hint.__supertype__)
