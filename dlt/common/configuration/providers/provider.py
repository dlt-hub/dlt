import abc
from typing import Any, Tuple, Type, Optional

from dlt.common.configuration.exceptions import ConfigurationException



class ConfigProvider(abc.ABC):

    @abc.abstractmethod
    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        pass

    @property
    @abc.abstractmethod
    def supports_secrets(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def supports_namespaces(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass


class ConfigProviderException(ConfigurationException):
    def __init__(self, provider_name: str, *args: Any) -> None:
        self.provider_name = provider_name
        super().__init__(*args)
