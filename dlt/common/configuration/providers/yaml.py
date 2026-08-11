import os
import yaml
from contextlib import contextmanager
from pathlib import PurePath
from typing import Any, Dict, Iterator, Mapping, Optional, List, Sequence, Tuple

from dlt.common.configuration.utils import auto_config_fragment
from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.utils import update_dict_nested

from .doc import BaseDocProvider, CustomLoaderDocProvider
from .toml import GLOBAL_ORIGIN_PREFIX, TValueOrigins

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


class SettingsYamlProvider(CustomLoaderDocProvider):
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

    def get_value_location(self, key: str, pipeline_name: Optional[str], *sections: str) -> str:
        """Get location (file name) of a value from the origins doc built when files were loaded.
        Values from it are located with a `global:` prefix so that files sharing a name are told apart.
        """
        node = self._origins_node(self.get_key_path(key, pipeline_name, *sections))
        if node is None:
            return ""
        return node.origin if isinstance(node, TValueOrigins) else node

    def write_yaml(self) -> None:
        assert (
            len(self._yaml_paths) == 1
        ), "Will not write configs when more than one yaml path was resolved. Found paths: " + str(
            self._yaml_paths
        )
        with open(self._yaml_paths[0], "w", encoding="utf-8") as f:
            yaml.safe_dump(self._config_doc, f, sort_keys=False, default_flow_style=False)

    def set_value(self, key: str, value: Any, pipeline_name: Optional[str], *sections: str) -> None:
        self._drop_origin(self.get_key_path(key, pipeline_name, *sections))
        super().set_value(key, value, pipeline_name, *sections)

    @contextmanager
    def preserve(self) -> Iterator[None]:
        saved_origins = self._value_origins.clone()
        with super().preserve():
            try:
                yield
            finally:
                self._value_origins = saved_origins

    @property
    def present_locations(self) -> List[str]:
        return self._present_locations

    def set_fragment(
        self, key: Optional[str], value_or_fragment: str, pipeline_name: str, *sections: str
    ) -> None:
        if (fragment := auto_config_fragment(value_or_fragment)) is not None:
            if key is None:
                self._value_origins = TValueOrigins()
            else:
                self._drop_origins(fragment)
        else:
            self._drop_origin(self.get_key_path(key, pipeline_name, *sections))
        super().set_fragment(key, value_or_fragment, pipeline_name, *sections)

    def _is_in_global_dir(self, path: str) -> bool:
        """Tells if `path` sits in the global dir. On Windows paths compare case insensitively."""
        if not self._global_dir:
            return False
        # PurePath folds case and separators on Windows but keeps ".." verbatim, abspath resolves it
        return PurePath(os.path.abspath(path)).is_relative_to(os.path.abspath(self._global_dir))

    def _path_origin(self, path: str) -> str:
        """Names the file `path`, marking the global dir so files sharing a name are told apart."""
        if self._is_in_global_dir(path):
            return GLOBAL_ORIGIN_PREFIX + os.path.basename(path)
        return os.path.basename(path)

    @staticmethod
    def _doc_origins(doc: Mapping[str, Any], origin: str) -> TValueOrigins:
        """Mirrors `doc` shape replacing each value with `origin`, tables carry `origin` as well."""
        origins = TValueOrigins()
        origins.origin = origin
        for k, v in doc.items():
            origins[k] = (
                SettingsYamlProvider._doc_origins(v, origin) if isinstance(v, dict) else origin
            )
        return origins

    def _origins_node(self, full_path: Sequence[str]) -> Any:
        """Returns origins doc node under `full_path` or None if not known."""
        node: Any = self._value_origins
        for k in full_path:
            if not isinstance(node, dict) or k not in node:
                return None
            node = node[k]
        return node

    def _drop_origin(self, full_path: Sequence[str]) -> None:
        """Forgets origin of a value under `full_path`, dropping whole subtree for tables."""
        if not full_path:
            return
        parent = self._origins_node(full_path[:-1])
        if isinstance(parent, dict):
            parent.pop(full_path[-1], None)

    def _drop_origins(self, doc: Mapping[str, Any], path: Tuple[str, ...] = ()) -> None:
        """Forgets origins of all values present in `doc`, a fragment merged from the root."""
        if not isinstance(doc, dict):
            return
        for k, v in doc.items():
            sub_path = path + (k,)
            # a fragment table merges key by key, but it replaces a value that was not a table
            if isinstance(v, dict) and isinstance(self._origins_node(sub_path), dict):
                self._drop_origins(v, sub_path)
            else:
                self._drop_origin(sub_path)

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


class YamlProviderReadException(ConfigProviderException):
    def __init__(
        self, provider_name: str, file_name: str, full_paths: List[str], yaml_exception: str
    ) -> None:
        self.file_name = file_name
        self.full_paths = full_paths
        msg = f"A problem encountered when loading {provider_name} from paths {full_paths}:\n"
        msg += yaml_exception
        super().__init__(provider_name, msg)
