import abc
from typing import Any, Tuple, Type, Optional

from dlt.common.configuration.exceptions import ConfigurationException


class ConfigProvider(abc.ABC):

    @abc.abstractmethod
    def get_value(self, key: str, hint: Type[Any], pipeline_name: str, *sections: str) -> Tuple[Optional[Any], str]:
        pass

    def set_value(self, key: str, value: Any, pipeline_name: str, *sections: str) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def supports_secrets(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def supports_sections(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @property
    def is_empty(self) -> bool:
        return False

    @property
    def is_writable(self) -> bool:
        return False


def get_key_name(key: str, separator: str, /, *sections: str) -> str:
    if sections:
        sections = filter(lambda x: bool(x), sections)  # type: ignore
        env_key = separator.join((*sections, key))
    else:
        env_key = key
    return env_key


class ConfigProviderException(ConfigurationException):
    def __init__(self, provider_name: str, *args: Any) -> None:
        self.provider_name = provider_name
        super().__init__(*args)
