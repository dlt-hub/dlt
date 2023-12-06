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

    pass


class ConfigProviderException(ConfigurationException):
    """base exceptions for all exceptions raised by config providers"""

    pass


class ConfigurationWrongTypeException(ConfigurationException):
    def __init__(self, _typ: type) -> None:
        super().__init__(
            f"Invalid configuration instance type {_typ}. Configuration instances must derive from"
            " BaseConfiguration."
        )


class ConfigFieldMissingException(KeyError, ConfigurationException):
    """raises when not all required config fields are present"""

    def __init__(self, spec_name: str, traces: Mapping[str, Sequence[LookupTrace]]) -> None:
        self.traces = traces
        self.spec_name = spec_name
        self.fields = list(traces.keys())
        super().__init__(spec_name)

    def __str__(self) -> str:
        msg = (
            f"Following fields are missing: {str(self.fields)} in configuration with spec"
            f" {self.spec_name}\n"
        )
        for f, field_traces in self.traces.items():
            msg += f'\tfor field "{f}" config providers and keys were tried in following order:\n'
            for tr in field_traces:
                msg += f"\t\tIn {tr.provider} key {tr.key} was not found.\n"
        # check if entry point is run with path. this is common problem so warn the user
        main_path = main_module_file_path()
        if main_path:
            main_dir = os.path.dirname(main_path)
            abs_main_dir = os.path.abspath(main_dir)
            if abs_main_dir != os.getcwd():
                # directory was specified
                msg += (
                    "WARNING: dlt looks for .dlt folder in your current working directory and your"
                    " cwd (%s) is different from directory of your pipeline script (%s).\n"
                    % (os.getcwd(), abs_main_dir)
                )
                msg += (
                    "If you keep your secret files in the same folder as your pipeline script but"
                    " run your script from some other folder, secrets/configs will not be found\n"
                )
        msg += (
            "Please refer to https://dlthub.com/docs/general-usage/credentials for more"
            " information\n"
        )
        return msg

    def attrs(self) -> Dict[str, Any]:
        attrs_ = super().attrs()
        if "traces" in attrs_:
            for _, traces in self.traces.items():
                for idx, trace in enumerate(traces):
                    # drop all values as they may contain secrets
                    traces[idx] = trace._replace(value=None)  # type: ignore[index]
        return attrs_


class UnmatchedConfigHintResolversException(ConfigurationException):
    """Raised when using `@resolve_type` on a field that doesn't exist in the spec"""

    def __init__(self, spec_name: str, field_names: Sequence[str]) -> None:
        self.field_names = field_names
        self.spec_name = spec_name
        example = f">>> class {spec_name}(BaseConfiguration)\n" + "\n".join(
            f">>>    {name}: Any" for name in field_names
        )
        msg = (
            f"The config spec {spec_name} has dynamic type resolvers for fields: {field_names} but"
            " these fields are not defined in the spec.\nWhen using @resolve_type() decorator, Add"
            f" the fields with 'Any' or another common type hint, example:\n\n{example}"
        )
        super().__init__(msg)


class FinalConfigFieldException(ConfigurationException):
    """rises when field was annotated as final ie Final[str] and the value is modified by config provider"""

    def __init__(self, spec_name: str, field: str) -> None:
        super().__init__(
            f"Field {field} in spec {spec_name} is final but is being changed by a config provider"
        )


class ConfigValueCannotBeCoercedException(ConfigurationValueError):
    """raises when value returned by config provider cannot be coerced to hinted type"""

    def __init__(self, field_name: str, field_value: Any, hint: type) -> None:
        self.field_name = field_name
        self.field_value = field_value
        self.hint = hint
        super().__init__(
            "Configured value for field %s cannot be coerced into type %s" % (field_name, str(hint))
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
        super().__init__(f"Missing config file in {path}")


class ConfigFieldMissingTypeHintException(ConfigurationException):
    """thrown when configuration specification does not have type hint"""

    def __init__(self, field_name: str, spec: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(
            f"Field {field_name} on configspec {spec} does not provide required type hint"
        )


class ConfigFieldTypeHintNotSupported(ConfigurationException):
    """thrown when configuration specification uses not supported type in hint"""

    def __init__(self, field_name: str, spec: Type[Any], typ_: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(
            f"Field {field_name} on configspec {spec} has hint with unsupported type {typ_}"
        )


class ValueNotSecretException(ConfigurationException):
    def __init__(self, provider_name: str, key: str) -> None:
        self.provider_name = provider_name
        self.key = key
        super().__init__(
            f"Provider {provider_name} cannot hold secret values but key {key} with secret value is"
            " present"
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
            f"{spec.__name__} cannot parse the configuration value provided. The value is of type"
            f" {native_value_type.__name__} and comes from the"
            f" {embedded_sections} section(s).{inner_msg}"
        )


class ContainerInjectableContextMangled(ContainerException):
    def __init__(self, spec: Type[Any], existing_config: Any, expected_config: Any) -> None:
        self.spec = spec
        self.existing_config = existing_config
        self.expected_config = expected_config
        super().__init__(
            f"When restoring context {spec.__name__}, instance {expected_config} was expected,"
            f" instead instance {existing_config} was found."
        )


class ContextDefaultCannotBeCreated(ContainerException, KeyError):
    def __init__(self, spec: Type[Any]) -> None:
        self.spec = spec
        super().__init__(f"Container cannot create the default value of context {spec.__name__}.")


class DuplicateConfigProviderException(ConfigProviderException):
    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        super().__init__(
            f"Provider with name {provider_name} already present in ConfigProvidersContext"
        )
