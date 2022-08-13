import sys
import semver
from typing import Any, Dict, List, Mapping, Type, TypeVar, cast

from dlt.common.typing import StrAny, is_optional_type, is_literal_type
from dlt.common.configuration import BaseConfiguration
from dlt.common.configuration.providers import environ
from dlt.common.configuration.exceptions import (ConfigEntryMissingException,
                                                 ConfigEnvValueCannotBeCoercedException)
from dlt.common.utils import uniq_id

SIMPLE_TYPES: List[Any] = [int, bool, list, dict, tuple, bytes, set, float]
# those types and Optionals of those types should not be passed to eval function
NON_EVAL_TYPES = [str, None, Any]
# allows to coerce (type1 from type2)
ALLOWED_TYPE_COERCIONS = [(float, int), (str, int), (str, float)]
IS_DEVELOPMENT_CONFIG_KEY: str = "IS_DEVELOPMENT_CONFIG"
CHECK_INTEGRITY_F: str = "check_integrity"

TConfiguration = TypeVar("TConfiguration", bound=Type[BaseConfiguration])
# TODO: remove production configuration support
TProductionConfiguration = TypeVar("TProductionConfiguration", bound=Type[BaseConfiguration])


def make_configuration(config: TConfiguration,
                       production_config: TProductionConfiguration,
                       initial_values: StrAny = None,
                       accept_partial: bool = False,
                       skip_subclass_check: bool = False) -> TConfiguration:
    if not skip_subclass_check:
        assert issubclass(production_config, config)

    final_config: TConfiguration = config if _is_development_config() else production_config
    possible_keys_in_config = _get_config_attrs_with_hints(final_config)
    # create dynamic class type to not touch original config variables
    derived_config: TConfiguration = cast(TConfiguration,
                                          type(final_config.__name__ + "_" + uniq_id(), (final_config, ), {})
                                    )
    # apply initial values while preserving hints
    derived_config.apply_dict(initial_values)

    _apply_environ_to_config(derived_config, possible_keys_in_config)
    try:
        _is_config_bounded(derived_config, possible_keys_in_config)
        _check_configuration_integrity(derived_config)
        # full configuration was resolved
        derived_config.__is_partial__ = False
    except ConfigEntryMissingException:
        if not accept_partial:
            raise
    _add_module_version(derived_config)

    return derived_config


def is_direct_descendant(child: Type[Any], base: Type[Any]) -> bool:
    # TODO: there may be faster way to get direct descendant that mro
    # note: at index zero there's child
    return base == type.mro(child)[1]


def _is_development_config() -> bool:
    # get from environment
    is_dev_config: bool = None
    try:
        is_dev_config = _coerce_single_value(IS_DEVELOPMENT_CONFIG_KEY, environ.get_key(IS_DEVELOPMENT_CONFIG_KEY, bool), bool)
    except ConfigEnvValueCannotBeCoercedException as coer_exc:
        # pass for None: this key may not be present
        if coer_exc.env_value is None:
            pass
        else:
            # anything else that cannot corece must raise
            raise
    return True if is_dev_config is None else is_dev_config


def _add_module_version(config: TConfiguration) -> None:
    try:
        v = sys._getframe(1).f_back.f_globals["__version__"]
        semver.VersionInfo.parse(v)
        setattr(config, "_VERSION", v)  # noqa: B010
    except KeyError:
        pass


def _apply_environ_to_config(config: TConfiguration, keys_in_config: Mapping[str, type]) -> None:
    for key, hint in keys_in_config.items():
        value = environ.get_key(key, hint, config.__namespace__)
        if value is not None:
            value_from_environment_variable = _coerce_single_value(key, value, hint)
            # set value
            setattr(config, key, value_from_environment_variable)


def _is_config_bounded(config: TConfiguration, keys_in_config: Mapping[str, type]) -> None:
    # TODO: here we assume all keys are taken from environ provider, that should change when we introduce more providers
    _unbound_attrs = [
        environ.get_key_name(key, config.__namespace__) for key in keys_in_config if getattr(config, key) is None and not is_optional_type(keys_in_config[key])
    ]

    if len(_unbound_attrs) > 0:
        raise ConfigEntryMissingException(_unbound_attrs, config.__namespace__)


def _check_configuration_integrity(config: TConfiguration) -> None:
    # python multi-inheritance is cooperative and this would require that all configurations cooperatively
    # call each other check_integrity. this is not at all possible as we do not know which configs in the end will
    # be mixed together.

    # get base classes in order of derivation
    mro = type.mro(config)
    for c in mro:
        # check if this class implements check_integrity (skip pure inheritance to not do double work)
        if CHECK_INTEGRITY_F in c.__dict__ and callable(getattr(c, CHECK_INTEGRITY_F)):
            # access unbounded __func__ to pass right class type so we check settings of the tip of mro
            c.__dict__[CHECK_INTEGRITY_F].__func__(config)


def _coerce_single_value(key: str, value: str, hint: Type[Any]) -> Any:
    try:
        hint_primitive_type = _extract_simple_type(hint)
        if hint_primitive_type not in NON_EVAL_TYPES:
            # create primitive types out of strings
            typed_value = eval(value)  # nosec
            # for primitive types check coercion
            if hint_primitive_type in SIMPLE_TYPES and type(typed_value) != hint_primitive_type:
                # allow some exceptions
                coerce_exception = next(
                    (e for e in ALLOWED_TYPE_COERCIONS if e == (hint_primitive_type, type(typed_value))), None)
                if coerce_exception:
                    return hint_primitive_type(typed_value)
                else:
                    raise ConfigEnvValueCannotBeCoercedException(key, typed_value, hint)
            return typed_value
        else:
            return value
    except ConfigEnvValueCannotBeCoercedException:
        raise
    except Exception as exc:
        raise ConfigEnvValueCannotBeCoercedException(key, value, hint) from exc


def _get_config_attrs_with_hints(config: TConfiguration) -> Dict[str, type]:
    keys: Dict[str, type] = {}
    mro = type.mro(config)
    for cls in reversed(mro):
        # update in reverse derivation order so derived classes overwrite hints from base classes
        if cls is not object:
            keys.update(
                [(attr, cls.__annotations__.get(attr, None))
                  # if hasattr(config, '__annotations__') and attr in config.__annotations__ else None)
                 for attr in cls.__dict__.keys() if not callable(getattr(cls, attr)) and not attr.startswith("__")
                 ])
    return keys


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
