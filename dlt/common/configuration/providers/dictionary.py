from contextlib import contextmanager
from typing import ClassVar, Iterator

from dlt.common.typing import DictStrAny

from .provider import get_key_name
from .toml import BaseDocProvider


class DictionaryProvider(BaseDocProvider):
    NAME: ClassVar[str] = "Dictionary Provider"

    def __init__(self) -> None:
        super().__init__({})

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, "__", *sections)

    @property
    def name(self) -> str:
        return self.NAME

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_sections(self) -> bool:
        return True

    @contextmanager
    def values(self, v: DictStrAny) -> Iterator[None]:
        p_values = self._config_doc
        self._config_doc = v
        yield
        self._config_doc = p_values
