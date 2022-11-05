from typing import Any, Mapping, Type, Union, NamedTuple, Sequence

from dlt.common.exceptions import DltException


class LookupTrace(NamedTuple):
    provider: str
    namespaces: Sequence[str]
    key: str
    value: Any


class ConfigurationException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)



class ContainerException(ConfigurationException):
    """base exception for all exceptions related to injectable container"""
    pass


class ConfigProviderException(ConfigurationException):
    """base exceptions for all exceptions raised by config providers"""
    pass


class ConfigurationWrongTypeException(ConfigurationException):
    def __init__(self, _typ: type) -> None:
        super().__init__(f"Invalid configuration instance type {_typ}. Configuration instances must derive from BaseConfiguration.")


class ConfigFieldMissingException(ConfigurationException):
    """thrown when not all required config fields are present"""

    def __init__(self, spec_name: str, traces: Mapping[str, Sequence[LookupTrace]]) -> None:
        self.traces = traces
        self.spec_name = spec_name
        self.fields = list(traces.keys())

        msg = f"Following fields are missing: {str(self.fields)} in configuration with spec {spec_name}\n"
        for f, field_traces in traces.items():
            msg += f'\tfor field "{f}" config providers and keys were tried in following order:\n'
            for tr in field_traces:
                msg += f'\t\tIn {tr.provider} key {tr.key} was not found.\n'
        super().__init__(msg)


class ConfigValueCannotBeCoercedException(ConfigurationException):
    """thrown when value returned by config provider cannot be coerced to hinted type"""

    def __init__(self, field_name: str, field_value: Any, hint: type) -> None:
        self.field_name = field_name
        self.field_value = field_value
        self.hint = hint
        super().__init__('env value %s cannot be coerced into type %s in attr %s' % (field_value, str(hint), field_name))


class ConfigIntegrityException(ConfigurationException):
    """thrown when value from ENV cannot be coerced to hinted type"""

    def __init__(self, attr_name: str, env_value: str, info: Union[type, str]) -> None:
        self.attr_name = attr_name
        self.env_value = env_value
        self.info = info
        super().__init__('integrity error for attr %s with value %s. %s.' % (attr_name, env_value, info))


class ConfigFileNotFoundException(ConfigurationException):
    """thrown when configuration file cannot be found in config folder"""

    def __init__(self, path: str) -> None:
        super().__init__(f"Missing config file in {path}")


class ConfigFieldMissingTypeHintException(ConfigurationException):
    """thrown when configuration specification does not have type hint"""

    def __init__(self, field_name: str, spec: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(f"Field {field_name} on configspec {spec} does not provide required type hint")


class ConfigFieldTypeHintNotSupported(ConfigurationException):
    """thrown when configuration specification uses not supported type in hint"""

    def __init__(self, field_name: str, spec: Type[Any], typ_: Type[Any]) -> None:
        self.field_name = field_name
        self.typ_ = spec
        super().__init__(f"Field {field_name} on configspec {spec} has hint with unsupported type {typ_}")


class ValueNotSecretException(ConfigurationException):
    def __init__(self, provider_name: str, key: str) -> None:
        self.provider_name = provider_name
        self.key = key
        super().__init__(f"Provider {provider_name} cannot hold secret values but key {key} with secret value is present")


class InvalidInitialValue(ConfigurationException):
    def __init__(self, spec: Type[Any], initial_value_type: Type[Any]) -> None:
        self.spec = spec
        self.initial_value_type = initial_value_type
        super().__init__(f"Initial value of type {initial_value_type} is not valid for {spec.__name__}")


class ContainerInjectableContextMangled(ContainerException):
    def __init__(self, spec: Type[Any], existing_config: Any, expected_config: Any) -> None:
        self.spec = spec
        self.existing_config = existing_config
        self.expected_config = expected_config
        super().__init__(f"When restoring context {spec.__name__}, instance {expected_config} was expected, instead instance {existing_config} was found.")


class ContextDefaultCannotBeCreated(ContainerException, KeyError):
    def __init__(self, spec: Type[Any]) -> None:
        self.spec = spec
        super().__init__(f"Container cannot create the default value of context {spec.__name__}.")


class DuplicateConfigProviderException(ConfigProviderException):
    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        super().__init__(f"Provider with name {provider_name} already present in ConfigProvidersContext")
