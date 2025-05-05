import abc
from typing import Any, Sequence, Tuple, Type, Optional

from dlt.common.configuration.exceptions import ConfigurationException


class ConfigProvider(abc.ABC):
    @abc.abstractmethod
    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        """Looks for a value under `key` in section(s) `sections` and tries to coerce the
        value to type `hint`. A pipeline context (top level section) will be added if
        `pipeline_name` was specified.
        """

    def set_value(self, key: str, value: Any, pipeline_name: str, *sections: str) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def supports_secrets(self) -> bool:
        """If true, provider is allowed to store secret. Configuration resolution fails if
        a secret value is discovered in a config provider that does not support secrets.
        """

    @property
    @abc.abstractmethod
    def supports_sections(self) -> bool:
        """If true, config resolution will query this provider for all allowed section combinations
        otherwise values are queried only by field name.
        """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Human readable name of config provider"""

    @property
    def is_empty(self) -> bool:
        """Tells if config provider holds any values"""
        return False

    @property
    def is_writable(self) -> bool:
        """Tells if `set_value` may be used"""
        return False

    @property
    def locations(self) -> Sequence[str]:
        """Returns a list of locations where secrets are stored, human readable"""
        return []


def get_key_name(key: str, separator: str, /, *sections: str) -> str:
    if sections:
        sections = filter(lambda x: bool(x), sections)  # type: ignore
        env_key = separator.join((*sections, key))
    else:
        env_key = key
    return env_key
