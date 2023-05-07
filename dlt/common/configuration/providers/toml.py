import os
import tomlkit
from tomlkit.items import Item as TOMLItem
from tomlkit.container import Container as TOMLContainer
from typing import Any, Optional, Tuple, Type, Union

from dlt.common.configuration.paths import get_dlt_project_dir, get_dlt_home_dir
from dlt.common.utils import update_dict_nested

from .provider import ConfigProvider, ConfigProviderException, get_key_name

CONFIG_TOML = "config.toml"
SECRETS_TOML = "secrets.toml"

class BaseTomlProvider(ConfigProvider):
    def __init__(self, toml_document: TOMLContainer) -> None:
        self._toml = toml_document

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, ".", *sections)

    def get_value(self, key: str, hint: Type[Any], pipeline_name: str, *sections: str) -> Tuple[Optional[Any], str]:
        full_path = sections + (key,)
        if pipeline_name:
            full_path = (pipeline_name, ) + full_path
        full_key = self.get_key_name(key, pipeline_name, *sections)
        node: Union[TOMLContainer, TOMLItem] = self._toml
        try:
            for k in full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            rv = node.unwrap() if isinstance(node, (TOMLContainer, TOMLItem)) else node
            return rv, full_key
        except KeyError:
            return None, full_key

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(self._toml.body) == 0


class TomlFileProvider(BaseTomlProvider):
    def __init__(self, file_name: str, project_dir: str = None, add_global_config: bool = False) -> None:
        """Creates config provider from a `toml` file

        The provider loads the `toml` file with specified name and from specified folder. If `add_global_config` flags is specified,
        it will look for `file_name` in `dlt` home dir. The "project" (`project_dir`) values overwrite the "global" values.

        If none of the files exist, an empty provider is created.

        Args:
            file_name (str): The name of `toml` file to load
            project_dir (str, optional): The location of `file_name`. If not specified, defaults to $cwd/.dlt
            add_global_config (bool, optional): Looks for `file_name` in `dlt` home directory which in most cases is $HOME/.dlt

        Raises:
            TomlProviderReadException: File could not be read, most probably `toml` parsing error
        """
        toml_document = self._read_toml_file(file_name, project_dir, add_global_config)
        super().__init__(toml_document)

    def _read_toml_file(self, file_name: str, project_dir: str = None, add_global_config: bool = False) -> tomlkit.TOMLDocument:
        self._file_name = file_name
        self._toml_path = os.path.join(project_dir or get_dlt_project_dir(), file_name)
        self._add_global_config = add_global_config
        try:
            project_toml = self._read_toml(self._toml_path)
            if add_global_config:
                global_toml = self._read_toml(os.path.join(self.global_config_path(), file_name))
                project_toml = update_dict_nested(global_toml, project_toml)
            return project_toml
        except Exception as ex:
            raise TomlProviderReadException(self.name, file_name, self._toml_path, str(ex))

    @staticmethod
    def global_config_path() -> str:
        return get_dlt_home_dir()

    def write_toml(self) -> None:
        assert not self._add_global_config, "Will not write configs when `add_global_config` flag was set"
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


class ConfigTomlProvider(TomlFileProvider):

    def __init__(self, project_dir: str = None, add_global_config: bool = False) -> None:
        super().__init__(CONFIG_TOML, project_dir=project_dir, add_global_config=add_global_config)

    @property
    def name(self) -> str:
        return CONFIG_TOML

    @property
    def supports_secrets(self) -> bool:
        return False



class SecretsTomlProvider(TomlFileProvider):

    def __init__(self, project_dir: str = None, add_global_config: bool = False) -> None:
        super().__init__(SECRETS_TOML, project_dir=project_dir, add_global_config=add_global_config)

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
