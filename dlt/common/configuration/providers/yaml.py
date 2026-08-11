import os
import yaml
from typing import Any, Dict, Optional, List

from dlt.common.utils import update_dict_nested

from .doc import BaseDocProvider
from .settings import SettingsDocProvider, SettingsProviderReadException, TValueOrigins

CONFIG_YAML = "config.yaml"
SECRETS_YAML = "secrets.yaml"


class StringYamlProvider(BaseDocProvider):
    def __init__(self, yaml_string: str) -> None:
        super().__init__(StringYamlProvider.loads(yaml_string))

    def dumps(self) -> str:
        return yaml.safe_dump(self._config_doc, sort_keys=False, default_flow_style=False)

    @staticmethod
    def loads(yaml_string: str) -> Dict[str, Any]:
        return yaml.safe_load(yaml_string) or {}

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "memory"


class SettingsYamlProvider(SettingsDocProvider):
    def __init__(
        self,
        name: str,
        supports_secrets: bool,
        file_name: str,
        resolvable_dirs: List[str],
        global_dir: str = None,
    ) -> None:
        """Creates config provider from a `yaml` file

        The provider loads the `yaml` file with specified name and from specified folder. If `global_dir` is specified,
        it will additionally look for `file_name` in `dlt` global dir (home dir by default) and merge the content.
        The "settings" (`settings_dir`) values overwrite the "global" values.

        If none of the files exist, an empty provider is created.

        Args:
            name(str): name of the provider when registering in context
            supports_secrets(bool): allows to store secret values in this provider
            file_name (str): The name of `yaml` file to load
            resolvable_dirs (List[str]): A list of directories to resolve the file from.
                              Files will be merged into each other in the order the directories are specified. Provider is writeable if only one dir specified.
            global_dir (str, optional): Which of the `resolvable_dirs` is the `dlt` global dir.

        Raises:
            YamlProviderReadException: File could not be read, most probably `yaml` parsing error
        """
        # set supports_secrets early, we need this flag to read config
        self._supports_secrets = supports_secrets
        # read yaml file from local
        self._yaml_paths = self._resolve_yaml_paths(
            file_name, [d for d in resolvable_dirs if d is not None]
        )
        # read yaml files and set present locations
        self._present_locations: List[str] = []
        self._global_dir = global_dir
        self._value_origins = TValueOrigins()
        config_doc = self._read_yaml_files(name, file_name, self._yaml_paths)

        super().__init__(
            name,
            lambda: config_doc,
            supports_secrets,
            self._yaml_paths,
        )

    def _resolve_yaml_paths(self, file_name: str, resolvable_dirs: List[str]) -> List[str]:
        return [os.path.join(d, file_name) for d in resolvable_dirs]

    def write_yaml(self) -> None:
        assert (
            len(self._yaml_paths) == 1
        ), "Will not write configs when more than one yaml path was resolved. Found paths: " + str(
            self._yaml_paths
        )
        with open(self._yaml_paths[0], "w", encoding="utf-8") as f:
            yaml.safe_dump(self._config_doc, f, sort_keys=False, default_flow_style=False)

    def _read_yaml_file(self, yaml_path: str) -> Dict[str, Any]:
        if os.path.isfile(yaml_path):
            with open(yaml_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        else:
            return None

    def _read_yaml_files(
        self, name: str, file_name: str, yaml_paths: List[str]
    ) -> Dict[str, Any]:
        """Merge all yaml files into one"""
        try:
            result_doc: Optional[Dict[str, Any]] = None
            for path in yaml_paths:
                if (loaded_doc := self._read_yaml_file(path)) is not None:
                    origins = self._doc_origins(loaded_doc, self._path_origin(path))
                    if result_doc is None:
                        result_doc, self._value_origins = loaded_doc, origins
                    else:
                        # files are merged highest precedence first, so accumulated values win
                        result_doc = update_dict_nested(loaded_doc, result_doc)
                        self._value_origins = update_dict_nested(origins, self._value_origins)
                    # store as present location
                    self._present_locations.append(path)

            if result_doc is None:
                result_doc = {}

            return result_doc
        except Exception as ex:
            raise YamlProviderReadException(name, file_name, yaml_paths, str(ex))


class ConfigYamlProvider(SettingsYamlProvider):
    def __init__(self, settings_dir: str, global_dir: str = None) -> None:
        super().__init__(CONFIG_YAML, False, CONFIG_YAML, [settings_dir, global_dir], global_dir)

    @property
    def is_writable(self) -> bool:
        return True


class SecretsYamlProvider(SettingsYamlProvider):
    def __init__(self, settings_dir: str, global_dir: str = None) -> None:
        super().__init__(SECRETS_YAML, True, SECRETS_YAML, [settings_dir, global_dir], global_dir)

    @property
    def is_writable(self) -> bool:
        return True


class YamlProviderReadException(SettingsProviderReadException):
    pass
