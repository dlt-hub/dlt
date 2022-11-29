from collections.abc import Mapping as C_Mapping
from typing import Any, Dict, ContextManager, List, Optional, Sequence, Tuple, Type, TypeVar

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.typing import AnyType, StrAny, TSecretValue, is_final_type, is_optional_type

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, CredentialsConfiguration, is_secret_hint, is_context_hint, is_base_configuration_hint
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.utils import extract_inner_hint, log_traces, deserialize_value
from dlt.common.configuration.exceptions import (FinalConfigFieldException, LookupTrace, ConfigFieldMissingException, ConfigurationWrongTypeException, ValueNotSecretException, InvalidNativeValue)

TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def resolve_configuration(config: TConfiguration, *, namespaces: Tuple[str, ...] = (), explicit_value: Any = None, accept_partial: bool = False) -> TConfiguration:
    if not isinstance(config, BaseConfiguration):
        raise ConfigurationWrongTypeException(type(config))

    # try to get the native representation of the top level configuration using the config namespace as a key
    # allows, for example, to store connection string or service.json in their native form in single env variable or under single vault key
    if config.__namespace__ and explicit_value is None:
        initial_hint = TSecretValue if isinstance(config, CredentialsConfiguration) else AnyType
        explicit_value, traces = _resolve_single_value(config.__namespace__, initial_hint, AnyType, None, namespaces, ())
        if isinstance(explicit_value, C_Mapping):
            # mappings cannot be used as explicit values, we want to enumerate mappings and request the fields' values one by one
            explicit_value = None
        else:
            log_traces(None, config.__namespace__, type(config), explicit_value, None, traces)

    return _resolve_configuration(config, namespaces, (), explicit_value, accept_partial)


def inject_namespace(namespace_context: ConfigNamespacesContext, merge_existing: bool = True) -> ContextManager[ConfigNamespacesContext]:
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
        explicit_value: Any,
        accept_partial: bool
    ) -> TConfiguration:

    # do not resolve twice
    if config.is_resolved():
        return config

    config.__exception__ = None
    try:
        try:
            # use initial value to resolve the whole configuration. if explicit value is a mapping it will be applied field by field later
            if explicit_value and not isinstance(explicit_value, C_Mapping):
                try:
                    config.parse_native_representation(explicit_value)
                except ValueError as v_err:
                    # provide generic exception
                    raise InvalidNativeValue(type(config), type(explicit_value), embedded_namespaces, v_err)
                except NotImplementedError:
                    pass
                # explicit value was consumed
                explicit_value = None

            # if native representation didn't fully resolve the config, we try to resolve field by field
            if not config.is_resolved():
                _resolve_config_fields(config, explicit_value, explicit_namespaces, embedded_namespaces, accept_partial)

            _call_method_in_mro(config, "on_resolved")
            # full configuration was resolved
            config.__is_resolved__ = True
        except ConfigFieldMissingException as cm_ex:
            # store the ConfigEntryMissingException to have full info on traces of missing fields
            config.__exception__ = cm_ex
            _call_method_in_mro(config, "on_partial")
            # if resolved then do not raise
            if config.is_resolved():
                _call_method_in_mro(config, "on_resolved")
            else:
                if not accept_partial:
                    raise
    except Exception as ex:
        # store the exception that happened in the resolution process
        config.__exception__ = ex
        raise

    return config


def _resolve_config_fields(
        config: BaseConfiguration,
        explicit_values: StrAny,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...],
        accept_partial: bool
    ) -> None:

    fields = config.get_resolvable_fields()
    unresolved_fields: Dict[str, Sequence[LookupTrace]] = {}

    for key, hint in fields.items():
        # get default and explicit values
        default_value = getattr(config, key, None)

        if explicit_values:
            explicit_value = explicit_values.get(key)
        else:
            if is_final_type(hint):
                # for final fields default value is like explicit
                explicit_value = default_value
            else:
                explicit_value = None

        current_value, traces = _resolve_config_field(key, hint, default_value, explicit_value, config, config.__namespace__, explicit_namespaces, embedded_namespaces, accept_partial)
        # check if hint optional
        is_optional = is_optional_type(hint)
        # collect unresolved fields
        if not is_optional and current_value is None:
            unresolved_fields[key] = traces
        # set resolved value in config
        if default_value != current_value:
            if is_final_type(hint):
                raise FinalConfigFieldException(type(config).__name__, key)
            setattr(config, key, current_value)

    if unresolved_fields:
        raise ConfigFieldMissingException(type(config).__name__, unresolved_fields)


def _resolve_config_field(
        key: str,
        hint: Type[Any],
        default_value: Any,
        explicit_value: Any,
        config: BaseConfiguration,
        config_namespace: str,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...],
        accept_partial: bool
    ) -> Tuple[Any, List[LookupTrace]]:

    inner_hint = extract_inner_hint(hint)

    if explicit_value is not None:
        value = explicit_value
        traces: List[LookupTrace] = []
    else:
        # resolve key value via active providers passing the original hint ie. to preserve TSecretValue
        value, traces = _resolve_single_value(key, hint, inner_hint, config_namespace, explicit_namespaces, embedded_namespaces)
        log_traces(config, key, hint, value, default_value, traces)
    # contexts must be resolved as a whole
    if is_context_hint(inner_hint):
        pass
    # if inner_hint is BaseConfiguration then resolve it recursively
    elif is_base_configuration_hint(inner_hint):
        if isinstance(value, BaseConfiguration):
            # if resolved value is instance of configuration (typically returned by context provider)
            embedded_config = value
            value = None
        elif isinstance(default_value, BaseConfiguration):
            # if default value was instance of configuration, use it
            embedded_config = default_value
            default_value = None
        else:
            embedded_config = inner_hint()

        if embedded_config.is_resolved():
            # injected context will be resolved
            value = embedded_config
        else:
            # only config with namespaces may look for initial values
            if embedded_config.__namespace__ and value is None:
                # config namespace becomes the key if the key does not start with, otherwise it keeps its original value
                initial_key, initial_embedded = _apply_embedded_namespaces_to_config_namespace(embedded_config.__namespace__, embedded_namespaces + (key,))
                # it must be a secret value is config is credentials
                initial_hint = TSecretValue if isinstance(embedded_config, CredentialsConfiguration) else AnyType
                value, initial_traces = _resolve_single_value(initial_key, initial_hint, AnyType, None, explicit_namespaces, initial_embedded)
                if isinstance(value, C_Mapping):
                    # mappings are not passed as initials
                    value = None
                else:
                    traces.extend(initial_traces)
                    log_traces(config, initial_key, type(embedded_config), value, default_value, initial_traces)

            # check if hint optional
            is_optional = is_optional_type(hint)
            # accept partial becomes True if type if optional so we do not fail on optional configs that do not resolve fully
            accept_partial = accept_partial or is_optional
            # create new instance and pass value from the provider as initial, add key to namespaces
            value = _resolve_configuration(embedded_config, explicit_namespaces, embedded_namespaces + (key,), default_value if value is None else value, accept_partial)
    else:
        # if value is resolved, then deserialize and coerce it
        if value is not None:
            # do not deserialize explicit values
            if value is not explicit_value:
                value = deserialize_value(key, value, inner_hint)

    return default_value if value is None else value, traces


def _call_method_in_mro(config: BaseConfiguration, method_name: str) -> None:
    # python multi-inheritance is cooperative and this would require that all configurations cooperatively
    # call each other class_method_name. this is not at all possible as we do not know which configs in the end will
    # be mixed together.

    # get base classes in order of derivation
    mro = type.mro(type(config))
    for c in mro:
        # check if this class implements on_resolved (skip pure inheritance to not do double work)
        if method_name in c.__dict__ and callable(getattr(c, method_name)):
            # pass right class instance
            c.__dict__[method_name](config)


def _resolve_single_value(
        key: str,
        hint: Type[Any],
        inner_hint: Type[Any],
        config_namespace: str,
        explicit_namespaces: Tuple[str, ...],
        embedded_namespaces: Tuple[str, ...]
    ) -> Tuple[Optional[Any], List[LookupTrace]]:

    traces: List[LookupTrace] = []
    value = None

    container = Container()
    # get providers from container
    providers_context = container[ConfigProvidersContext]
    # we may be resolving context
    if is_context_hint(inner_hint):
        # resolve context with context provider and do not look further
        value, _ = providers_context.context_provider.get_value(key, inner_hint)
        return value, traces
    if is_base_configuration_hint(inner_hint):
        # cannot resolve configurations directly
        return value, traces

    # resolve a field of the config
    config_namespace, embedded_namespaces = _apply_embedded_namespaces_to_config_namespace(config_namespace, embedded_namespaces)
    providers = providers_context.providers
    # get additional namespaces to look in from container
    namespaces_context = container[ConfigNamespacesContext]

    def look_namespaces(pipeline_name: str = None) -> Any:
        # start looking from the top provider with most specific set of namespaces first
        for provider in providers:
            value, provider_traces = resolve_single_provider_value(
                provider,
                key,
                hint,
                pipeline_name,
                config_namespace,
                # if explicit namespaces are provided, ignore the injected context
                explicit_namespaces or namespaces_context.namespaces,
                embedded_namespaces
            )
            traces.extend(provider_traces)
            if value is not None:
                # value found, ignore other providers
                break

        return value

    # first try with pipeline name as namespace, if present
    if namespaces_context.pipeline_name:
        value = look_namespaces(namespaces_context.pipeline_name)
    # then without it
    if value is None:
        value = look_namespaces()

    return value, traces


def resolve_single_provider_value(
    provider: ConfigProvider,
    key: str,
    hint: Type[Any],
    pipeline_name: str = None,
    config_namespace: str = None,
    explicit_namespaces: Tuple[str, ...] = (),
    embedded_namespaces: Tuple[str, ...] = (),
    # context_namespaces: Tuple[str, ...] = (),
    ) -> Tuple[Optional[Any], List[LookupTrace]]:
    traces: List[LookupTrace] = []

    if provider.supports_namespaces:
        ns = list(explicit_namespaces)
        # always extend with embedded namespaces
        ns.extend(embedded_namespaces)
    else:
        # if provider does not support namespaces and pipeline name is set then ignore it
        if pipeline_name:
            return None, traces
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
        cant_hold_it: bool = not provider.supports_secrets and is_secret_hint(hint)
        if value is not None and cant_hold_it:
            raise ValueNotSecretException(provider.name, ns_key)

        # create trace, ignore providers that cant_hold_it
        if not cant_hold_it:
            traces.append(LookupTrace(provider.name, full_ns, ns_key, value))

        if value is not None:
            # value found, ignore further namespaces
            break
        if len(ns) == 0:
            # namespaces exhausted
            break
        # pop optional namespaces for less precise lookup
        ns.pop()

    return value, traces


def _apply_embedded_namespaces_to_config_namespace(config_namespace: str, embedded_namespaces: Tuple[str, ...]) -> Tuple[str, Tuple[str, ...]]:
    # for the configurations that have __namespace__ (config_namespace) defined and are embedded in other configurations,
    # the innermost embedded namespace replaces config_namespace
    if embedded_namespaces:
        # do not add key to embedded namespaces if it starts with _, those namespaces must be ignored
        if not embedded_namespaces[-1].startswith("_"):
            config_namespace = embedded_namespaces[-1]
        embedded_namespaces = embedded_namespaces[:-1]

    # remove all embedded ns starting with _
    return config_namespace, tuple(ns for ns in embedded_namespaces if not ns.startswith("_"))
