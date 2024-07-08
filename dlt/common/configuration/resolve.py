import itertools
from collections.abc import Mapping as C_Mapping
from typing import Any, Dict, ContextManager, List, Optional, Sequence, Tuple, Type, TypeVar

from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.typing import (
    AnyType,
    ConfigValueSentinel,
    StrAny,
    TSecretValue,
    get_all_types_of_class_in_union,
    is_optional_type,
    is_subclass,
    is_union_type,
)

from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
    is_secret_hint,
    extract_inner_hint,
    is_context_inner_hint,
    is_base_configuration_inner_hint,
    is_valid_hint,
    is_hint_not_resolvable,
)
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.specs.exceptions import NativeValueError
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.container import Container
from dlt.common.configuration.utils import log_traces, deserialize_value
from dlt.common.configuration.exceptions import (
    LookupTrace,
    ConfigFieldMissingException,
    ConfigurationWrongTypeException,
    ValueNotSecretException,
    InvalidNativeValue,
    UnmatchedConfigHintResolversException,
)

TConfiguration = TypeVar("TConfiguration", bound=BaseConfiguration)


def resolve_configuration(
    config: TConfiguration,
    *,
    sections: Tuple[str, ...] = (),
    explicit_value: Any = None,
    accept_partial: bool = False
) -> TConfiguration:
    if not isinstance(config, BaseConfiguration):
        raise ConfigurationWrongTypeException(type(config))

    # try to get the native representation of the top level configuration using the config section as a key
    # allows, for example, to store connection string or service.json in their native form in single env variable or under single vault key
    if config.__section__ and explicit_value is None:
        initial_hint = TSecretValue if isinstance(config, CredentialsConfiguration) else AnyType
        explicit_value, traces = _resolve_single_value(
            config.__section__, initial_hint, AnyType, None, sections, ()
        )
        if isinstance(explicit_value, C_Mapping):
            # mappings cannot be used as explicit values, we want to enumerate mappings and request the fields' values one by one
            explicit_value = None
        else:
            log_traces(None, config.__section__, type(config), explicit_value, None, traces)

    return _resolve_configuration(config, sections, (), explicit_value, accept_partial)


def initialize_credentials(hint: Any, initial_value: Any) -> CredentialsConfiguration:
    """Instantiate credentials of type `hint` with `initial_value`. The initial value must be a native representation (typically string)
    or a dictionary corresponding to credential's fields. In case of union of credentials, the first configuration in the union fully resolved by
    initial value will be instantiated."""
    # use passed credentials as initial value. initial value may resolve credentials
    if is_union_type(hint):
        specs_in_union = get_all_types_of_class_in_union(hint, CredentialsConfiguration)
        assert len(specs_in_union) > 0
        first_credentials: CredentialsConfiguration = None
        for idx, spec in enumerate(specs_in_union):
            try:
                credentials = spec.from_init_value(initial_value)
                if credentials.is_resolved():
                    return credentials
                # keep first credentials in the union to return in case all of the match but not resolve
                first_credentials = first_credentials or credentials
            except (NativeValueError, NotImplementedError):
                # if none of specs in union parsed
                if idx == len(specs_in_union) - 1 and first_credentials is None:
                    raise
        return first_credentials
    else:
        assert is_subclass(hint, CredentialsConfiguration)
        return hint.from_init_value(initial_value)  # type: ignore


def inject_section(
    section_context: ConfigSectionContext, merge_existing: bool = True, lock_context: bool = False
) -> ContextManager[ConfigSectionContext]:
    """Context manager that sets section specified in `section_context` to be used during configuration resolution. Optionally merges the context already in the container with the one provided

    Args:
        section_context (ConfigSectionContext): Instance providing a pipeline name and section context
        merge_existing (bool, optional): Merges existing section context with `section_context` in the arguments by executing `merge_style` function on `section_context`. Defaults to True.
        lock_context (bool, optional): Instruct to threadlock the current thread to prevent race conditions in context injection.

    Default Merge Style:
        Gets `pipeline_name` and `sections` from existing context if they are not provided in `section_context` argument.

    Yields:
        Iterator[ConfigSectionContext]: Context manager with current section context
    """
    container = Container()
    existing_context = container[ConfigSectionContext]

    if merge_existing:
        section_context.merge(existing_context)

    return container.injectable_context(section_context, lock_context=lock_context)


def _maybe_parse_native_value(
    config: TConfiguration, explicit_value: Any, embedded_sections: Tuple[str, ...]
) -> Any:
    # use initial value to resolve the whole configuration. if explicit value is a mapping it will be applied field by field later
    if explicit_value and (
        not isinstance(explicit_value, C_Mapping) or isinstance(explicit_value, BaseConfiguration)
    ):
        try:
            # parse the native value anyway because there are configs with side effects
            config.parse_native_representation(explicit_value)
            default_value = config.__class__()
            # parse native value and convert it into dict, extract the diff and use it as exact value
            # NOTE: as those are the same dataclasses, the set of keys must be the same
            explicit_value = {
                k: v
                for k, v in config.__class__.from_init_value(explicit_value).items()
                if default_value[k] != v
            }
        except ValueError as v_err:
            # provide generic exception
            raise InvalidNativeValue(type(config), type(explicit_value), embedded_sections, v_err)
        except NotImplementedError:
            pass
    return explicit_value


def _resolve_configuration(
    config: TConfiguration,
    explicit_sections: Tuple[str, ...],
    embedded_sections: Tuple[str, ...],
    explicit_value: Any,
    accept_partial: bool,
) -> TConfiguration:
    # do not resolve twice
    if config.is_resolved():
        return config

    config.__exception__ = None
    try:
        try:
            explicit_value = _maybe_parse_native_value(config, explicit_value, embedded_sections)
            # if native representation didn't fully resolve the config, we try to resolve field by field
            if not config.is_resolved():
                _resolve_config_fields(
                    config, explicit_value, explicit_sections, embedded_sections, accept_partial
                )
            # full configuration was resolved
            config.resolve()
        except ConfigFieldMissingException as cm_ex:
            # store the ConfigEntryMissingException to have full info on traces of missing fields
            config.__exception__ = cm_ex
            # may resolve in partial handler
            config.call_method_in_mro("on_partial")
            # if resolved then do not raise
            if not config.is_resolved() and not accept_partial:
                raise
    except Exception as ex:
        # store the exception that happened in the resolution process
        config.__exception__ = ex
        raise

    return config


def _resolve_config_fields(
    config: BaseConfiguration,
    explicit_values: StrAny,
    explicit_sections: Tuple[str, ...],
    embedded_sections: Tuple[str, ...],
    accept_partial: bool,
) -> None:
    fields = config.get_resolvable_fields()
    unresolved_fields: Dict[str, Sequence[LookupTrace]] = {}

    for key, hint in fields.items():
        if key in config.__hint_resolvers__:
            # Type hint for this field is created dynamically
            hint = config.__hint_resolvers__[key](config)
        # get default and explicit values
        default_value = getattr(config, key, None)
        explicit_none = False
        traces: List[LookupTrace] = []

        if explicit_values:
            explicit_value = None
            if key in explicit_values:
                # allow None to be passed in explicit values
                # so we are able to reset defaults like in regular function calls
                explicit_value = explicit_values[key]
                explicit_none = explicit_value is None
                # detect dlt.config and dlt.secrets and force injection
                if isinstance(explicit_value, ConfigValueSentinel):
                    explicit_value = None
        else:
            if is_hint_not_resolvable(hint):
                # for final fields default value is like explicit
                explicit_value = default_value
            else:
                explicit_value = None

        current_value = None
        # explicit none skips resolution
        if not explicit_none:
            # if hint is union of configurations, any of them must be resolved
            specs_in_union: List[Type[BaseConfiguration]] = []
            if is_union_type(hint):
                # if union contains a type of explicit value which is not a valid hint, return it as current value
                if (
                    explicit_value
                    and not is_valid_hint(type(explicit_value))
                    and get_all_types_of_class_in_union(
                        hint, type(explicit_value), with_superclass=True
                    )
                ):
                    current_value, traces = explicit_value, []
                else:
                    specs_in_union = get_all_types_of_class_in_union(hint, BaseConfiguration)
            if not current_value:
                if len(specs_in_union) > 1:
                    for idx, alt_spec in enumerate(specs_in_union):
                        # return first resolved config from an union
                        try:
                            current_value, traces = _resolve_config_field(
                                key,
                                alt_spec,
                                default_value,
                                explicit_value,
                                config,
                                config.__section__,
                                explicit_sections,
                                embedded_sections,
                                accept_partial,
                            )
                            break
                        except ConfigFieldMissingException as cfm_ex:
                            # add traces from unresolved union spec
                            # TODO: we should group traces per hint - currently user will see all options tried without the key info
                            traces.extend(list(itertools.chain(*cfm_ex.traces.values())))
                        except InvalidNativeValue:
                            # if none of specs in union parsed
                            if idx == len(specs_in_union) - 1:
                                raise
                else:
                    current_value, traces = _resolve_config_field(
                        key,
                        hint,
                        default_value,
                        explicit_value,
                        config,
                        config.__section__,
                        explicit_sections,
                        embedded_sections,
                        accept_partial,
                    )
        else:
            # set the trace for explicit none
            traces = [LookupTrace("ExplicitValues", None, key, None)]

        # check if hint optional
        is_optional = is_optional_type(hint)
        # collect unresolved fields
        if not is_optional and current_value is None:
            unresolved_fields[key] = traces
        # set resolved value in config
        if default_value != current_value:
            if not is_hint_not_resolvable(hint):
                # ignore final types
                setattr(config, key, current_value)

    # Check for dynamic hint resolvers which have no corresponding fields
    unmatched_hint_resolvers: List[str] = []
    for field_name in config.__hint_resolvers__:
        if field_name not in fields:
            unmatched_hint_resolvers.append(field_name)

    if unmatched_hint_resolvers:
        raise UnmatchedConfigHintResolversException(type(config).__name__, unmatched_hint_resolvers)

    if unresolved_fields:
        raise ConfigFieldMissingException(type(config).__name__, unresolved_fields)


def _resolve_config_field(
    key: str,
    hint: Type[Any],
    default_value: Any,
    explicit_value: Any,
    config: BaseConfiguration,
    config_sections: str,
    explicit_sections: Tuple[str, ...],
    embedded_sections: Tuple[str, ...],
    accept_partial: bool,
) -> Tuple[Any, List[LookupTrace]]:
    inner_hint = extract_inner_hint(hint, preserve_literal=True)

    if explicit_value is not None:
        value = explicit_value
        traces: List[LookupTrace] = []
    else:
        # resolve key value via active providers passing the original hint ie. to preserve TSecretValue
        value, traces = _resolve_single_value(
            key, hint, inner_hint, config_sections, explicit_sections, embedded_sections
        )
        log_traces(config, key, hint, value, default_value, traces)
    # contexts must be resolved as a whole
    if is_context_inner_hint(inner_hint):
        pass
    # if inner_hint is BaseConfiguration then resolve it recursively
    elif is_base_configuration_inner_hint(inner_hint):
        if isinstance(default_value, BaseConfiguration):
            # if default value was instance of configuration, use it as embedded initial
            embedded_config = default_value
            default_value = None
        elif isinstance(value, BaseConfiguration):
            # if resolved value is instance of configuration (typically returned by context provider)
            embedded_config = value
            default_value = None
            value = None
        else:
            embedded_config = inner_hint()

        if embedded_config.is_resolved():
            # print(f"{embedded_config} IS RESOLVED with VALUE {value}")
            # injected context will be resolved
            if value is not None:
                from_native_explicit = _maybe_parse_native_value(
                    embedded_config, value, embedded_sections + (key,)
                )
                if from_native_explicit is not value:
                    embedded_config.update(from_native_explicit)
            value = embedded_config
        else:
            # only config with sections may look for initial values
            if embedded_config.__section__ and value is None:
                # config section becomes the key if the key does not start with, otherwise it keeps its original value
                initial_key, initial_embedded = _apply_embedded_sections_to_config_sections(
                    embedded_config.__section__, embedded_sections + (key,)
                )
                # it must be a secret value is config is credentials
                initial_hint = (
                    TSecretValue
                    if isinstance(embedded_config, CredentialsConfiguration)
                    else AnyType
                )
                value, initial_traces = _resolve_single_value(
                    initial_key, initial_hint, AnyType, None, explicit_sections, initial_embedded
                )
                if isinstance(value, C_Mapping):
                    # mappings are not passed as initials
                    value = None
                else:
                    traces.extend(initial_traces)
                    log_traces(
                        config,
                        initial_key,
                        type(embedded_config),
                        value,
                        default_value,
                        initial_traces,
                    )

            # check if hint optional
            is_optional = is_optional_type(hint)
            # accept partial becomes True if type if optional so we do not fail on optional configs that do not resolve fully
            accept_partial = accept_partial or is_optional
            # create new instance and pass value from the provider as initial, add key to sections
            value = _resolve_configuration(
                embedded_config,
                explicit_sections,
                embedded_sections + (key,),
                default_value if value is None else value,
                accept_partial,
            )
            if value.is_partial() and is_optional:
                # do not return partially resolved optional embeds
                value = None
    else:
        # if value is resolved, then deserialize and coerce it
        if value is not None:
            # do not deserialize explicit values
            if value is not explicit_value:
                value = deserialize_value(key, value, inner_hint)

    return default_value if value is None else value, traces


def _resolve_single_value(
    key: str,
    hint: Type[Any],
    inner_hint: Type[Any],
    config_section: str,
    explicit_sections: Tuple[str, ...],
    embedded_sections: Tuple[str, ...],
) -> Tuple[Optional[Any], List[LookupTrace]]:
    traces: List[LookupTrace] = []
    value = None

    container = Container()
    # get providers from container
    providers_context = container[ConfigProvidersContext]
    # we may be resolving context
    if is_context_inner_hint(inner_hint):
        # resolve context with context provider and do not look further
        value, _ = providers_context.context_provider.get_value(key, inner_hint, None)
        return value, traces
    if is_base_configuration_inner_hint(inner_hint):
        # cannot resolve configurations directly
        return value, traces

    # resolve a field of the config
    config_section, embedded_sections = _apply_embedded_sections_to_config_sections(
        config_section, embedded_sections
    )
    providers = providers_context.providers
    # get additional sections to look in from container
    sections_context = container[ConfigSectionContext]

    def look_sections(pipeline_name: str = None) -> Any:
        # start looking from the top provider with most specific set of sections first
        value: Any = None
        for provider in providers:
            if provider.is_empty:
                # do not query empty provider so they are not added to the trace
                continue

            value, provider_traces = resolve_single_provider_value(
                provider,
                key,
                hint,
                pipeline_name,
                config_section,
                # if explicit sections are provided, ignore the injected context
                explicit_sections or sections_context.sections,
                embedded_sections,
            )
            traces.extend(provider_traces)
            if value is not None:
                # value found, ignore other providers
                break

        return value

    # first try with pipeline name as section, if present
    if sections_context.pipeline_name:
        value = look_sections(sections_context.pipeline_name)
    # then without it
    if value is None:
        value = look_sections()

    return value, traces


def resolve_single_provider_value(
    provider: ConfigProvider,
    key: str,
    hint: Type[Any],
    pipeline_name: str = None,
    config_section: str = None,
    explicit_sections: Tuple[str, ...] = (),
    embedded_sections: Tuple[str, ...] = (),
) -> Tuple[Optional[Any], List[LookupTrace]]:
    traces: List[LookupTrace] = []

    if provider.supports_sections:
        ns = list(explicit_sections)
        # always extend with embedded sections
        ns.extend(embedded_sections)
    else:
        # if provider does not support sections and pipeline name is set then ignore it
        if pipeline_name:
            return None, traces
        else:
            # pass empty sections
            ns = []

    value = None
    while True:
        if config_section and provider.supports_sections:
            full_ns = ns.copy()
            # config section, is always present and innermost
            if config_section:
                full_ns.append(config_section)
        else:
            full_ns = ns
        value, ns_key = provider.get_value(key, hint, pipeline_name, *full_ns)
        # if secret is obtained from non secret provider, we must fail
        cant_hold_it: bool = not provider.supports_secrets and is_secret_hint(hint)
        if value is not None and cant_hold_it:
            raise ValueNotSecretException(provider.name, ns_key)

        # create trace, ignore providers that cant_hold_it
        if not cant_hold_it:
            traces.append(LookupTrace(provider.name, full_ns, ns_key, value))

        if value is not None:
            # value found, ignore further sections
            break
        if len(ns) == 0:
            # sections exhausted
            break
        # pop optional sections for less precise lookup
        ns.pop()

    return value, traces


def _apply_embedded_sections_to_config_sections(
    config_section: str, embedded_sections: Tuple[str, ...]
) -> Tuple[str, Tuple[str, ...]]:
    # for the configurations that have __section__ (config_section) defined and are embedded in other configurations,
    # the innermost embedded section replaces config_section
    if embedded_sections:
        # do not add key to embedded sections if it starts with _, those sections must be ignored
        if not embedded_sections[-1].startswith("_"):
            config_section = embedded_sections[-1]
        embedded_sections = embedded_sections[:-1]

    # remove all embedded ns starting with _
    return config_section, tuple(ns for ns in embedded_sections if not ns.startswith("_"))
