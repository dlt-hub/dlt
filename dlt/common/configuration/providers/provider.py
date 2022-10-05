import abc
from typing import Any, Tuple, Type, Optional



class Provider(abc.ABC):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        pass

    @property
    @abc.abstractmethod
    def is_secret(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass
