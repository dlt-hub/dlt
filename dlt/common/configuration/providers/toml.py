import os
import tomlkit
from tomlkit.items import Item as TOMLItem
from tomlkit.container import Container as TOMLContainer
from typing import Any, Optional, Tuple, Type, Union

from dlt.common.configuration import DOT_DLT
from dlt.common.configuration.utils import current_dot_dlt_path

from .provider import ConfigProvider, ConfigProviderException

CONFIG_TOML = "config.toml"
SECRETS_TOML = "secrets.toml"


class TomlProvider(ConfigProvider):

    def __init__(self, file_name: str, project_dir: str = None) -> None:
        self._file_name = file_name
        self._toml_path = os.path.join(project_dir or current_dot_dlt_path(), file_name)
        try:
            self._toml = self._read_toml(self._toml_path)
        except Exception as ex:
            raise TomlProviderReadException(self.name, file_name, self._toml_path, str(ex))

    @staticmethod
    def get_key_name(key: str, *namespaces: str) -> str:
        # env key is always upper case
        if namespaces:
            namespaces = filter(lambda x: bool(x), namespaces)  # type: ignore
            env_key = ".".join((*namespaces, key))
        else:
            env_key = key
        return env_key

    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        full_path = namespaces + (key,)
        full_key = self.get_key_name(key, *namespaces)
        node: Union[TOMLContainer, TOMLItem] = self._toml
        try:
            for k in  full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            return node, full_key
        except KeyError:
            return None, full_key

    @property
    def supports_namespaces(self) -> bool:
        return True

    def _write_toml(self) -> None:
        with open(self._toml_path, "w", encoding="utf-8") as f:
            tomlkit.dump(self._toml, f)

    @staticmethod
    def _read_toml(toml_path: str) -> tomlkit.TOMLDocument:
        if os.path.isfile(toml_path):
            with open(toml_path, "r", encoding="utf-8") as f:
                # use whitespace preserving parser
                return tomlkit.load(f)
        else:
            return tomlkit.document()


class ConfigTomlProvider(TomlProvider):

    def __init__(self, project_dir: str = None) -> None:
        super().__init__(CONFIG_TOML, project_dir)

    @property
    def name(self) -> str:
        return CONFIG_TOML

    @property
    def supports_secrets(self) -> bool:
        return False



class SecretsTomlProvider(TomlProvider):

    def __init__(self, project_dir: str = None) -> None:
        super().__init__(SECRETS_TOML, project_dir)

    @property
    def name(self) -> str:
        return SECRETS_TOML

    @property
    def supports_secrets(self) -> bool:
        return True


class TomlProviderReadException(ConfigProviderException):
    def __init__(self, provider_name: str, file_name: str, full_path: str, toml_exception: str) -> None:
        self.file_name = file_name
        self.full_path = full_path
        msg = f"A problem encountered when loading {provider_name} from {full_path}:\n"
        msg += toml_exception
        super().__init__(provider_name, msg)
