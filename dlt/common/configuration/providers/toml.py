import os
import tomlkit
import yaml
import functools
from tomlkit.items import Item as TOMLItem
from tomlkit.container import Container as TOMLContainer
from typing import Any, Callable, Dict, Optional, Tuple, Type

from dlt.common.configuration.paths import get_dlt_settings_dir, get_dlt_data_dir
from dlt.common.configuration.utils import auto_cast, auto_config_fragment
from dlt.common.utils import update_dict_nested

from .provider import ConfigProvider, ConfigProviderException, get_key_name

CONFIG_TOML = "config.toml"
SECRETS_TOML = "secrets.toml"


class BaseDocProvider(ConfigProvider):
    def __init__(self, config_doc: Dict[str, Any]) -> None:
        self._config_doc = config_doc

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, ".", *sections)

    def get_value(
        self, key: str, hint: Type[Any], pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        full_path = sections + (key,)
        if pipeline_name:
            full_path = (pipeline_name,) + full_path
        full_key = self.get_key_name(key, pipeline_name, *sections)
        node = self._config_doc
        try:
            for k in full_path:
                if not isinstance(node, dict):
                    raise KeyError(k)
                node = node[k]
            return node, full_key
        except KeyError:
            return None, full_key

    def set_value(self, key: str, value: Any, pipeline_name: Optional[str], *sections: str) -> None:
        """Sets `value` under `key` in `sections` and optionally for `pipeline_name`

        If key already has value of type dict and value to set is also of type dict, the new value
        is merged with old value.
        """
        if pipeline_name:
            sections = (pipeline_name,) + sections
        if key is None:
            raise ValueError("dlt_secrets_toml must contain toml document")

        master: Dict[str, Any]
        # descend from root, create tables if necessary
        master = self._config_doc
        for k in sections:
            if not isinstance(master, dict):
                raise KeyError(k)
            if k not in master:
                master[k] = {}
            master = master[k]
        if isinstance(value, dict):
            # remove none values, TODO: we need recursive None removal
            value = {k: v for k, v in value.items() if v is not None}
            # if target is also dict then merge recursively
            if isinstance(master.get(key), dict):
                update_dict_nested(master[key], value)
                return
        master[key] = value

    def set_fragment(
        self, key: Optional[str], value_or_fragment: str, pipeline_name: str, *sections: str
    ) -> None:
        """Tries to interpret `value_or_fragment` as a fragment of toml, yaml or json string and replace/merge into config doc.

        If `key` is not provided, fragment is considered a full document and will replace internal config doc. Otherwise
        fragment is merged with config doc from the root element and not from the element under `key`!

        For simple values it falls back to `set_value` method.
        """
        fragment = auto_config_fragment(value_or_fragment)
        if fragment is not None:
            # always update the top document
            if key is None:
                self._config_doc = fragment
            else:
                # TODO: verify that value contains only the elements under key
                update_dict_nested(self._config_doc, fragment)
        else:
            # set value using auto_cast
            self.set_value(key, auto_cast(value_or_fragment), pipeline_name, *sections)

    def to_toml(self) -> str:
        return tomlkit.dumps(self._config_doc)

    def to_yaml(self) -> str:
        return yaml.dump(
            self._config_doc, allow_unicode=True, default_flow_style=False, sort_keys=False
        )

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(self._config_doc) == 0


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


class CustomLoaderDocProvider(BaseDocProvider):
    def __init__(
        self, name: str, loader: Callable[[], Dict[str, Any]], supports_secrets: bool = True
    ) -> None:
        """Provider that calls `loader` function to get a Python dict with config/secret values to be queried.
        The `loader` function typically loads a string (ie. from file), parses it (ie. as toml or yaml), does additional
        processing and returns a Python dict to be queried.

        Instance of CustomLoaderDocProvider must be registered for the returned dict to be used to resolve config values.
        >>> import dlt
        >>> dlt.config.register_provider(provider)

        Args:
            name(str): name of the provider that will be visible ie. in exceptions
            loader(Callable[[], Dict[str, Any]]): user-supplied function that will load the document with config/secret values
            supports_secrets(bool): allows to store secret values in this provider

        """
        self._name = name
        self._supports_secrets = supports_secrets
        super().__init__(loader())

    @property
    def name(self) -> str:
        return self._name

    @property
    def supports_secrets(self) -> bool:
        return self._supports_secrets

    @property
    def is_writable(self) -> bool:
        return True


class ProjectDocProvider(CustomLoaderDocProvider):
    def __init__(
        self,
        name: str,
        supports_secrets: bool,
        file_name: str,
        project_dir: str = None,
        add_global_config: bool = False,
    ) -> None:
        """Creates config provider from a `toml` file

        The provider loads the `toml` file with specified name and from specified folder. If `add_global_config` flags is specified,
        it will look for `file_name` in `dlt` home dir. The "project" (`project_dir`) values overwrite the "global" values.

        If none of the files exist, an empty provider is created.

        Args:
            name(str): name of the provider when registering in context
            supports_secrets(bool): allows to store secret values in this provider
            file_name (str): The name of `toml` file to load
            project_dir (str, optional): The location of `file_name`. If not specified, defaults to $cwd/.dlt
            add_global_config (bool, optional): Looks for `file_name` in `dlt` home directory which in most cases is $HOME/.dlt

        Raises:
            TomlProviderReadException: File could not be read, most probably `toml` parsing error
        """
        self._toml_path = os.path.join(project_dir or get_dlt_settings_dir(), file_name)
        self._add_global_config = add_global_config

        super().__init__(
            name,
            functools.partial(
                self._read_toml_files, name, file_name, self._toml_path, add_global_config
            ),
            supports_secrets,
        )

    @staticmethod
    def global_config_path() -> str:
        return get_dlt_data_dir()

    def write_toml(self) -> None:
        assert (
            not self._add_global_config
        ), "Will not write configs when `add_global_config` flag was set"
        with open(self._toml_path, "w", encoding="utf-8") as f:
            tomlkit.dump(self._config_doc, f)

    @staticmethod
    def _read_toml_files(
        name: str, file_name: str, toml_path: str, add_global_config: bool
    ) -> Dict[str, Any]:
        try:
            project_toml = ProjectDocProvider._read_toml(toml_path).unwrap()
            if add_global_config:
                global_toml = ProjectDocProvider._read_toml(
                    os.path.join(ProjectDocProvider.global_config_path(), file_name)
                ).unwrap()
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


class ConfigTomlProvider(ProjectDocProvider):
    def __init__(self, project_dir: str = None, add_global_config: bool = False) -> None:
        super().__init__(
            CONFIG_TOML,
            False,
            CONFIG_TOML,
            project_dir=project_dir,
            add_global_config=add_global_config,
        )

    @property
    def is_writable(self) -> bool:
        return True


class SecretsTomlProvider(ProjectDocProvider):
    def __init__(self, project_dir: str = None, add_global_config: bool = False) -> None:
        super().__init__(
            SECRETS_TOML,
            True,
            SECRETS_TOML,
            project_dir=project_dir,
            add_global_config=add_global_config,
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
