import os
import tomlkit
import tomlkit.items
from typing import Any, Optional

from dlt.common.utils import update_dict_nested

from .provider import ConfigProviderException
from .doc import BaseDocProvider, CustomLoaderDocProvider

CONFIG_TOML = "config.toml"
SECRETS_TOML = "secrets.toml"


class StringTomlProvider(BaseDocProvider):
    def __init__(self, toml_string: str) -> None:
        super().__init__(StringTomlProvider.loads(toml_string).unwrap())

    # def update(self, toml_string: str) -> None:
    #     self._config_doc = StringTomlProvider.loads(toml_string).unwrap()

    def dumps(self) -> str:
        return tomlkit.dumps(self._config_doc)

    @staticmethod
    def loads(toml_string: str) -> tomlkit.TOMLDocument:
        return tomlkit.parse(toml_string)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "memory"


class SettingsTomlProvider(CustomLoaderDocProvider):
    _config_toml: tomlkit.TOMLDocument
    """Holds tomlkit document with config values that is in sync with _config_doc"""

    def __init__(
        self,
        name: str,
        supports_secrets: bool,
        file_name: str,
        settings_dir: str,
        global_dir: str = None,
    ) -> None:
        """Creates config provider from a `toml` file

        The provider loads the `toml` file with specified name and from specified folder. If `global_dir` is specified,
        it will additionally look for `file_name` in `dlt` global dir (home dir by default) and merge the content.
        The "settings" (`settings_dir`) values overwrite the "global" values.

        If none of the files exist, an empty provider is created.

        Args:
            name(str): name of the provider when registering in context
            supports_secrets(bool): allows to store secret values in this provider
            file_name (str): The name of `toml` file to load
            settings_dir (str, optional): The location of `file_name`. If not specified, defaults to $cwd/.dlt
            global_dir (bool, optional): Looks for `file_name` in global_dir (defaults to `dlt` home directory which in most cases is $HOME/.dlt)

        Raises:
            TomlProviderReadException: File could not be read, most probably `toml` parsing error
        """
        self._toml_path = os.path.join(settings_dir, file_name)
        self._global_dir = os.path.join(global_dir, file_name) if global_dir else None
        self._config_toml = self._read_toml_files(
            name, file_name, self._toml_path, self._global_dir
        )

        super().__init__(
            name,
            self._config_toml.unwrap,
            supports_secrets,
        )

    def write_toml(self) -> None:
        assert not self._global_dir, "Will not write configs when `global_dir` was set"
        with open(self._toml_path, "w", encoding="utf-8") as f:
            tomlkit.dump(self._config_toml, f)

    def set_value(self, key: str, value: Any, pipeline_name: Optional[str], *sections: str) -> None:
        # write both into tomlkit and dict representations
        try:
            self._set_value(self._config_toml, key, value, pipeline_name, *sections)
        except tomlkit.items._ConvertError:
            pass
        if hasattr(value, "unwrap"):
            value = value.unwrap()
        super().set_value(key, value, pipeline_name, *sections)

    @property
    def is_empty(self) -> bool:
        return len(self._config_toml.body) == 0 and super().is_empty

    def set_fragment(
        self, key: Optional[str], value_or_fragment: str, pipeline_name: str, *sections: str
    ) -> None:
        # write both into tomlkit and dict representations
        try:
            self._config_toml = self._set_fragment(
                self._config_toml, key, value_or_fragment, pipeline_name, *sections
            )
        except tomlkit.items._ConvertError:
            pass
        super().set_fragment(key, value_or_fragment, pipeline_name, *sections)

    def to_toml(self) -> str:
        return tomlkit.dumps(self._config_toml)

    @staticmethod
    def _read_toml_files(
        name: str, file_name: str, toml_path: str, global_path: str
    ) -> tomlkit.TOMLDocument:
        try:
            project_toml = SettingsTomlProvider._read_toml(toml_path)
            if global_path:
                global_toml = SettingsTomlProvider._read_toml(global_path)
                project_toml = update_dict_nested(global_toml, project_toml)
            return project_toml
        except Exception as ex:
            raise TomlProviderReadException(name, file_name, toml_path, str(ex))

    @staticmethod
    def _read_toml(toml_path: str) -> tomlkit.TOMLDocument:
        if os.path.isfile(toml_path):
            with open(toml_path, "r", encoding="utf-8") as f:
                # use whitespace preserving parser
                return tomlkit.load(f)
        else:
            return tomlkit.document()


class ConfigTomlProvider(SettingsTomlProvider):
    def __init__(self, settings_dir: str, global_dir: str = None) -> None:
        super().__init__(
            CONFIG_TOML,
            False,
            CONFIG_TOML,
            settings_dir=settings_dir,
            global_dir=global_dir,
        )

    @property
    def is_writable(self) -> bool:
        return True


class SecretsTomlProvider(SettingsTomlProvider):
    def __init__(self, settings_dir: str, global_dir: str = None) -> None:
        super().__init__(
            SECRETS_TOML,
            True,
            SECRETS_TOML,
            settings_dir=settings_dir,
            global_dir=global_dir,
        )

    @property
    def is_writable(self) -> bool:
        return True


class TomlProviderReadException(ConfigProviderException):
    def __init__(
        self, provider_name: str, file_name: str, full_path: str, toml_exception: str
    ) -> None:
        self.file_name = file_name
        self.full_path = full_path
        msg = f"A problem encountered when loading {provider_name} from {full_path}:\n"
        msg += toml_exception
        super().__init__(provider_name, msg)
