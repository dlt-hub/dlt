import abc
from typing import Any, Tuple, Type, Optional



class Provider(abc.ABC):

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
