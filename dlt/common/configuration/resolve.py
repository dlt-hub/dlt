import ast
import inspect
from collections.abc import Mapping as C_Mapping
from typing import Any, Dict, Generator, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, get_origin

from dlt.common import json, logger
from dlt.common.typing import TSecretValue, is_optional_type, extract_inner_type
from dlt.common.schema.utils import coerce_type, py_type_to_sc_type

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, CredentialsConfiguration, ContainerInjectableContext
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.providers.container import ContextProvider
from dlt.common.configuration.exceptions import (LookupTrace, ConfigEntryMissingException, ConfigurationWrongTypeException, ConfigEnvValueCannotBeCoercedException, ValueNotSecretException, InvalidInitialValue)

CHECK_INTEGRITY_F: str = "check_integrity"
TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def resolve_configuration(config: TConfiguration, *, namespaces: Tuple[str, ...] = (), initial_value: Any = None, accept_partial: bool = False) -> TConfiguration:
    if not isinstance(config, BaseConfiguration):
        raise ConfigurationWrongTypeException(type(config))

    return _resolve_configuration(config, namespaces, (), initial_value, accept_partial)


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


def inject_namespace(namespace_context: ConfigNamespacesContext, merge_existing: bool = True) -> Generator[ConfigNamespacesContext, None, None]:
    """Adds `namespace` context to container, making it injectable. Optionally merges the context already in the container with the one provided

    Args:
        namespace_context (ConfigNamespacesContext): Instance providing a pipeline name and namespace context
        merge_existing (bool, optional): Gets `pipeline_name` and `namespaces` from existing context if they are not provided in `namespace` argument. Defaults to True.

    Yields:
        Iterator[ConfigNamespacesContext]: Context manager with current namespace context
    """
    container = Container()
    existing_context = container[ConfigNamespacesContext]

    if merge_existing:
        namespace_context.pipeline_name = namespace_context.pipeline_name or existing_context.pipeline_name
        namespace_context.namespaces = namespace_context.namespaces or existing_context.namespaces

    return container.injectable_context(namespace_context)


def _resolve_configuration(
        config: TConfiguration,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...],
        initial_value: Any,
        accept_partial: bool
    ) -> TConfiguration:
    # do not resolve twice
    if config.is_resolved():
        return config

    config.__exception__ = None
    try:
        # if initial value is a Mapping then apply it
        if isinstance(initial_value, C_Mapping):
            config.update(initial_value)
            # cannot be native initial value
            initial_value = None

        # try to get the native representation of the configuration using the config namespace as a key
        # allows, for example, to store connection string or service.json in their native form in single env variable or under single vault key
        resolved_initial: Any = None
        if config.__namespace__ or embedded_namespaces:
            cf_n, emb_ns = _apply_embedded_namespaces_to_config_namespace(config.__namespace__, embedded_namespaces)
            resolved_initial, traces = _resolve_single_field(cf_n, type(config), None, explicit_namespaces, emb_ns)
            _log_traces(config, cf_n, type(config), resolved_initial, traces)
            # initial values cannot be dictionaries
            if not isinstance(resolved_initial, C_Mapping):
                initial_value = resolved_initial or initial_value
            # if this is injectable context then return it immediately
            if isinstance(resolved_initial, ContainerInjectableContext):
                return resolved_initial  # type: ignore
        try:
            try:
                # use initial value to set config values
                if initial_value:
                    config.from_native_representation(initial_value)
                # if no initial value or initial value was passed via argument, resolve config normally (config always over explicit params)
                if not initial_value or not resolved_initial:
                    raise NotImplementedError()
            except ValueError:
                raise InvalidInitialValue(type(config), type(initial_value))
            except NotImplementedError:
                # if config does not support native form, resolve normally
                _resolve_config_fields(config, explicit_namespaces, embedded_namespaces, accept_partial)

            _check_configuration_integrity(config)
            # full configuration was resolved
            config.__is_resolved__ = True
        except ConfigEntryMissingException as cm_ex:
            if not accept_partial:
                raise
            else:
                # store the ConfigEntryMissingException to have full info on traces of missing fields
                config.__exception__ = cm_ex
    except Exception as ex:
        # store the exception that happened in the resolution process
        config.__exception__ = ex
        raise

    return config


def _apply_embedded_namespaces_to_config_namespace(config_namespace: str, embedded_namespaces: Tuple[str, ...]) -> Tuple[str, Tuple[str, ...]]:
    # for the configurations that have __namespace__ (config_namespace) defined and are embedded in other configurations,
    # the innermost embedded namespace replaces config_namespace
    if embedded_namespaces:
        config_namespace = embedded_namespaces[-1]
        embedded_namespaces = embedded_namespaces[:-1]
    # if config_namespace:
    return config_namespace, embedded_namespaces


def _is_secret_hint(hint: Type[Any]) -> bool:
    return hint is TSecretValue or (inspect.isclass(hint) and issubclass(hint, CredentialsConfiguration))


def _resolve_config_fields(
        config: BaseConfiguration,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...],
        accept_partial: bool
    ) -> None:

    fields = config.get_resolvable_fields()
    unresolved_fields: Dict[str, Sequence[LookupTrace]] = {}

    for key, hint in fields.items():
        # get default value
        current_value = getattr(config, key, None)
        # check if hint optional
        is_optional = is_optional_type(hint)
        # accept partial becomes True if type if optional so we do not fail on optional configs that do not resolve fully
        accept_partial = accept_partial or is_optional

        # if current value is BaseConfiguration, resolve that instance
        if isinstance(current_value, BaseConfiguration):
            # resolve only if not yet resolved otherwise just pass it
            if not current_value.is_resolved():
                # add key as innermost namespace
                current_value = _resolve_configuration(current_value, explicit_namespaces, embedded_namespaces + (key,), None, accept_partial)
        else:
            # extract hint from Optional / Literal / NewType hints
            inner_hint = extract_inner_type(hint)
            # extract origin from generic types
            inner_hint = get_origin(inner_hint) or inner_hint

            # if inner_hint is BaseConfiguration then resolve it recursively
            if inspect.isclass(inner_hint) and issubclass(inner_hint, BaseConfiguration):
                # create new instance and pass value from the provider as initial, add key to namespaces
                current_value = _resolve_configuration(inner_hint(), explicit_namespaces, embedded_namespaces + (key,), current_value, accept_partial)
            else:

                # resolve key value via active providers passing the original hint ie. to preserve TSecretValue
                value, traces = _resolve_single_field(key, hint, config.__namespace__, explicit_namespaces, embedded_namespaces)
                _log_traces(config, key, hint, value, traces)
                # if value is resolved, then deserialize and coerce it
                if value is not None:
                    current_value = deserialize_value(key, value, inner_hint)

        # collect unresolved fields
        if not is_optional and current_value is None:
            unresolved_fields[key] = traces
        # set resolved value in config
        setattr(config, key, current_value)
    if unresolved_fields:
        raise ConfigEntryMissingException(type(config).__name__, unresolved_fields)


def _log_traces(config: BaseConfiguration, key: str, hint: Type[Any], value: Any, traces: Sequence[LookupTrace]) -> None:
    if logger.is_logging() and logger.log_level() == "DEBUG":
        logger.debug(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
        # print(f"Field {key} with type {hint} in {type(config).__name__} {'NOT RESOLVED' if value is None else 'RESOLVED'}")
        for tr in traces:
            # print(str(tr))
            logger.debug(str(tr))


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


def _resolve_single_field(
        key: str,
        hint: Type[Any],
        config_namespace: str,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...]
    ) -> Tuple[Optional[Any], List[LookupTrace]]:
    container = Container()
    # get providers from container
    providers = container[ConfigProvidersContext].providers
    # get additional namespaces to look in from container
    namespaces_context = container[ConfigNamespacesContext]
    config_namespace, embedded_namespaces = _apply_embedded_namespaces_to_config_namespace(config_namespace, embedded_namespaces)

    # start looking from the top provider with most specific set of namespaces first
    traces: List[LookupTrace] = []
    value = None

    def look_namespaces(pipeline_name: str = None) -> Any:
        for provider in providers:
            if provider.supports_namespaces:
                # if explicit namespaces are provided, ignore the injected context
                if explicit_namespaces:
                    ns = list(explicit_namespaces)
                else:
                    ns = list(namespaces_context.namespaces)
                # always extend with embedded namespaces
                ns.extend(embedded_namespaces)
            else:
                # if provider does not support namespaces and pipeline name is set then ignore it
                if pipeline_name:
                    continue
                else:
                    # pass empty namespaces
                    ns = []

            value = None
            while True:
                if (pipeline_name or config_namespace) and provider.supports_namespaces:
                    full_ns = ns.copy()
                    # pipeline, when provided, is the most outer and always present
                    if pipeline_name:
                        full_ns.insert(0, pipeline_name)
                    # config namespace, is always present and innermost
                    if config_namespace:
                        full_ns.append(config_namespace)
                else:
                    full_ns = ns
                value, ns_key = provider.get_value(key, hint, *full_ns)
                # if secret is obtained from non secret provider, we must fail
                cant_hold_it: bool = not provider.supports_secrets and _is_secret_hint(hint)
                if value is not None and cant_hold_it:
                    raise ValueNotSecretException(provider.name, ns_key)

                # create trace, ignore container provider and providers that cant_hold_it
                if provider.name != ContextProvider.NAME and not cant_hold_it:
                    traces.append(LookupTrace(provider.name, full_ns, ns_key, value))

                if value is not None:
                    # value found, ignore other providers
                    return value
                if len(ns) == 0:
                    # check next provider
                    break
                # pop optional namespaces for less precise lookup
                ns.pop()

    # first try with pipeline name as namespace, if present
    if namespaces_context.pipeline_name:
        value = look_namespaces(namespaces_context.pipeline_name)
    # then without it
    if value is None:
        value = look_namespaces()

    return value, traces
