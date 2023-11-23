from contextlib import contextmanager
from typing import Any, ClassVar, Iterator, Optional, Type, Tuple

from dlt.common.typing import StrAny

from .provider import ConfigProvider, get_key_name


class DictionaryProvider(ConfigProvider):
    NAME: ClassVar[str] = "Dictionary Provider"

    def __init__(self) -> None:
        self._values: StrAny = {}

    @property
    def name(self) -> str:
        return self.NAME

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        full_path = sections + (key,)
        if pipeline_name:
            full_path = (pipeline_name,) + full_path
        full_key = get_key_name(key, "__", pipeline_name, *sections)
        node = self._values
        try:
            for k in full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            return node, full_key
        except KeyError:
            return None, full_key

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_sections(self) -> bool:
        return True

    @contextmanager
    def values(self, v: StrAny) -> Iterator[None]:
        p_values = self._values
        self._values = v
        yield
        self._values = p_values
