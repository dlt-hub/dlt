import os
from typing import Any, Dict, Mapping, Type, Tuple, NamedTuple, Sequence

from dlt.common.exceptions import DltException, TerminalException
from dlt.common.utils import main_module_file_path


class LookupTrace(NamedTuple):
    provider: str
    sections: Sequence[str]
    key: str
    value: Any


class ConfigurationException(DltException, TerminalException):
    pass


class ConfigurationValueError(ConfigurationException, ValueError):
    pass


class ContainerException(DltException):
    """base exception for all exceptions related to injectable container"""


class ConfigProviderException(ConfigurationException):
    def __init__(self, provider_name: str, *args: Any) -> None:
        self.provider_name = provider_name
        super().__init__(*args)


class ConfigurationWrongTypeException(ConfigurationException):
    def __init__(self, _typ: type) -> None:
        super().__init__(
            f"Invalid configuration instance type `{_typ}`. Configuration instances must derive"
            " from BaseConfiguration and must be decorated with @configspec."
        )


class ConfigFieldMissingException(KeyError, ConfigurationException):
    """raises when not all required config fields are present"""

    def __init__(self, spec_name: str, traces: Mapping[str, Sequence[LookupTrace]]) -> None:
        self.traces = traces
        self.spec_name = spec_name
        self.fields = list(traces.keys())
        super().__init__(spec_name)

    def __str__(self) -> str:
        msg = f"Missing fields in configuration: {str(self.fields)} {self.spec_name}\n"
        for f, field_traces in self.traces.items():
            msg += (
                f"\tfor field `{f}` the following (config providers, keys) were tried in order:\n"
            )
            for tr in field_traces:
                msg += f"\t\t({tr.provider}, {tr.key})\n"

        from dlt.common.configuration.container import Container
        from dlt.common.configuration.specs import PluggableRunContext

        # print locations for config providers
        msg += "\n"
        providers = Container()[PluggableRunContext].providers
        for provider in providers.providers:
            if provider.locations:
                locations = "\n".join([f"\t- {os.path.abspath(loc)}" for loc in provider.locations])
                msg += f"Provider `{provider.name}` loaded values from locations:\n{locations}\n"

            if provider.is_empty:
                msg += (
                    f"WARNING: provider `{provider.name}` is empty. Locations (i.e., files) are"
                    " missing or empty.\n"
                )
        # get pipeline context warning
        msg += get_run_context_warning(main_module_file_path())
        msg += "Learn more: https://dlthub.com/docs/general-usage/credentials/\n"
        return msg

    def attrs(self) -> Dict[str, Any]:
        attrs_ = super().attrs()
        if "traces" in attrs_:
            for _, traces in self.traces.items():
                for idx, trace in enumerate(traces):
                    # drop all values as they may contain secrets
                    traces[idx] = trace._replace(value=None)  # type: ignore[index]
        return attrs_


def get_run_context_warning(main_module_path: str) -> str:
    """Generates additional warnings when config resolution failed. There are two typical reasons
    (except genuine missing configurations):
    * `main_module_path` differs from current run_dir so `dlt` does not find related .dlt folder with settings
    * command line was used but pipeline script hardcodes config and secrets
    """
    from dlt.common.configuration.container import Container
    from dlt.common.runtime import run_context
    from dlt.common.pipeline import SupportsPipeline, PipelineContext

    msg = "\n"
    settings_moved_msg = (
        "If you keep `.dlt` folder with secret files in the same directory as your pipeline script"
        " but run your script or a dlt cli command from some other folder,"
        " secrets/configs will not be found.\n"
    )

    # find active pipeline
    context = Container()[PipelineContext]
    active_pipeline: SupportsPipeline = None
    # it is not always present
    if context.is_active():
        active_pipeline = context.pipeline()

    # use abs path to settings
    active_context = run_context.active()
    active_settings = os.path.abspath(active_context.settings_dir)

    if active_pipeline and active_pipeline.last_run_context:
        # warn if settings are now somewhere else than when pipeline had last successful run
        last_run_settings = active_pipeline.last_run_context["settings_dir"]
        if last_run_settings != active_settings:
            msg += (
                f"WARNING: Active pipeline `{active_pipeline.pipeline_name}` used"
                f" `{last_run_settings}` directory to read config and secrets for the last"
                f" successful run. Different directory `{active_settings}` is used now which may be"
                " the reason for configuration not resolving.\n"
                + settings_moved_msg
            )

        else:
            # locations didn't change
            pass
    else:
        # there's no active pipeline or it does not contain run context from successful run
        if main_module_path and main_module_path.endswith(".py"):
            pipeline_script_dir = os.path.abspath(os.path.dirname(main_module_path))
            run_dir = os.path.abspath(run_context.active().run_dir)
            if pipeline_script_dir != run_dir:
                msg += (
                    "WARNING: Your run dir (%s) is different from directory of your"
                    " pipeline script (%s).\n" % (run_dir, pipeline_script_dir)
                ) + settings_moved_msg
        else:
            # if no module path (ie. interactive) or it was not a script (ie. `dlt` cli`)
            pass

    if main_module_path and main_module_path.endswith("dlt"):
        msg += (
            "\nWARNING: When accessing data in the pipeline from the command line `dlt` will not"
            " execute user code and will just attach to the existing pipeline working dir. If you"
            " hardcoded your credentials or passed them explicitly ie. with `dlt.secrets` they"
            " won't be visible. Use environment variables, config files or other providers with"
            " config injection in order to use cli commands that access data.\n"
        )

    return msg


class UnmatchedConfigHintResolversException(ConfigurationException):
    """Raised when using `@resolve_type` on a field that doesn't exist in the spec"""

    def __init__(self, spec_name: str, field_names: Sequence[str]) -> None:
        self.field_names = field_names
        self.spec_name = spec_name
        example = f">>> class {spec_name}(BaseConfiguration)\n" + "\n".join(
            f">>>    {name}: Any" for name in field_names
        )
        msg = (
            f"The config spec `{spec_name}` has dynamic type resolvers for fields: `{field_names}`"
            " but these fields are not defined in the spec.\nWhen using @resolve_type() decorator,"
            f" Add the fields with 'Any' or another common type hint, example:\n\n{example}"
        )
        super().__init__(msg)


class FinalConfigFieldException(ConfigurationException):
    """rises when field was annotated as final ie Final[str] and the value is modified by config provider"""

    def __init__(self, spec_name: str, field: str) -> None:
        super().__init__(
            f"Field `{field}` in spec `{spec_name}` is final but is being changed by a config"
            " provider"
        )


class ConfigValueCannotBeCoercedException(ConfigurationValueError):
    """raises when value returned by config provider cannot be coerced to hinted type"""

    def __init__(self, field_name: str, field_value: Any, hint: type) -> None:
        self.field_name = field_name
        self.field_value = field_value
        self.hint = hint
        super().__init__(
            f"Configured value for field `{field_name}` cannot be coerced into type `{str(hint)}`"
        )


# class ConfigIntegrityException(ConfigurationException):
#     """thrown when value from ENV cannot be coerced to hinted type"""

#     def __init__(self, attr_name: str, env_value: str, info: Union[type, str]) -> None:
#         self.attr_name = attr_name
#         self.env_value = env_value
#         self.info = info
#         super().__init__('integrity error for field %s. %s.' % (attr_name, info))


class ConfigFileNotFoundException(ConfigurationException):
    """thrown when configuration file cannot be found in config folder"""

    def __init__(self, path: str) -> None:
        super().__init__(f"Missing config file in `{path}`")


class ConfigFieldMissingTypeHintException(ConfigurationException):
    """thrown when configuration specification does not have type hint"""

    def __init__(self, field_name: str, spec: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(
            f"Field `{field_name}` on configspec `{spec}` does not provide required type hint"
        )


class ConfigFieldTypeHintNotSupported(ConfigurationException):
    """thrown when configuration specification uses not supported type in hint"""

    def __init__(self, field_name: str, spec: Type[Any], typ_: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(
            f"Field `{field_name}` on configspec `{spec}` has hint with unsupported type `{typ_}`"
        )


class ValueNotSecretException(ConfigurationException):
    def __init__(self, provider_name: str, key: str) -> None:
        self.provider_name = provider_name
        self.key = key
        super().__init__(
            f"Provider `{provider_name}` cannot hold secret values but key `{key}` with secret"
            " value is present"
        )


class InvalidNativeValue(ConfigurationException):
    def __init__(
        self,
        spec: Type[Any],
        native_value_type: Type[Any],
        embedded_sections: Tuple[str, ...],
        inner_exception: Exception,
    ) -> None:
        self.spec = spec
        self.native_value_type = native_value_type
        self.embedded_sections = embedded_sections
        self.inner_exception = inner_exception
        inner_msg = f" {self.inner_exception}" if inner_exception is not ValueError else ""
        super().__init__(
            f"`{spec.__name__}` cannot parse the configuration value provided. The value is of type"
            f" `{native_value_type.__name__}` and comes from the sections `{embedded_sections}`"
            f" Value may be a secret and is not shown. Details: {inner_msg}"
        )


class ContainerInjectableContextMangled(ContainerException):
    def __init__(self, spec: Type[Any], existing_config: Any, expected_config: Any) -> None:
        self.spec = spec
        self.existing_config = existing_config
        self.expected_config = expected_config
        super().__init__(
            f"When restoring context `{spec.__name__}`, instance `{expected_config}` was expected,"
            f" instead instance `{existing_config}` was found."
        )


class ContextDefaultCannotBeCreated(ContainerException, KeyError):
    def __init__(self, spec: Type[Any]) -> None:
        self.spec = spec
        super().__init__(f"Container cannot create the default value of context `{spec.__name__}`.")


class DuplicateConfigProviderException(ConfigProviderException):
    def __init__(self, provider_name: str) -> None:
        super().__init__(
            provider_name,
            f"Provider with name `{provider_name}` already present in `ConfigProvidersContext`",
        )
