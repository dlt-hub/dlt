import os
import abc
import tomlkit
import contextlib
from tomlkit.items import Item as TOMLItem
from tomlkit.container import Container as TOMLContainer
from typing import Any, Dict, Optional, Tuple, Type, Union

from dlt.common import pendulum
from dlt.common.configuration.paths import get_dlt_settings_dir, get_dlt_data_dir
from dlt.common.configuration.utils import auto_cast
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import is_secret_hint
from dlt.common.utils import update_dict_nested

from dlt.common.typing import AnyType

from .provider import ConfigProvider, ConfigProviderException, get_key_name

CONFIG_TOML = "config.toml"
SECRETS_TOML = "secrets.toml"
SECRETS_TOML_KEY = "dlt_secrets_toml"


class BaseTomlProvider(ConfigProvider):
    def __init__(self, toml_document: TOMLContainer) -> None:
        self._toml = toml_document

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

    def set_value(self, key: str, value: Any, pipeline_name: str, *sections: str) -> None:
        if pipeline_name:
            sections = (pipeline_name,) + sections

        if isinstance(value, TOMLContainer):
            if key is None:
                self._toml = value
            else:
                # always update the top document
                # TODO: verify that value contains only the elements under key
                update_dict_nested(self._toml, value)
        else:
            if key is None:
                raise ValueError("dlt_secrets_toml must contain toml document")

            master: TOMLContainer
            # descend from root, create tables if necessary
            master = self._toml
            for k in sections:
                if not isinstance(master, dict):
                    raise KeyError(k)
                if k not in master:
                    master[k] = tomlkit.table()
                master = master[k]  # type: ignore
            if isinstance(value, dict):
                # remove none values, TODO: we need recursive None removal
                value = {k: v for k, v in value.items() if v is not None}
                # if target is also dict then merge recursively
                if isinstance(master.get(key), dict):
                    update_dict_nested(master[key], value)  # type: ignore
                    return
            master[key] = value

    @property
    def supports_sections(self) -> bool:
        return True

    @property
    def is_empty(self) -> bool:
        return len(self._toml.body) == 0


class StringTomlProvider(BaseTomlProvider):
    def __init__(self, toml_string: str) -> None:
        super().__init__(StringTomlProvider.loads(toml_string))

    def update(self, toml_string: str) -> None:
        self._toml = self.loads(toml_string)

    def dumps(self) -> str:
        return tomlkit.dumps(self._toml)

    @staticmethod
    def loads(toml_string: str) -> tomlkit.TOMLDocument:
        return tomlkit.parse(toml_string)

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "memory"


class VaultTomlProvider(BaseTomlProvider):
    """A toml-backed Vault abstract config provider.

    This provider allows implementation of providers that store secrets in external vaults: like Hashicorp, Google Secrets or Airflow Metadata.
    The basic working principle is obtain config and secrets values from Vault keys and reconstitute a `secrets.toml` like document that is then used
    as a cache.

    The implemented must provide `_look_vault` method that returns a value from external vault from external key.

    To reduce number of calls to external vaults the provider is searching for a known configuration fragments which should be toml documents and merging
    them with the
    - only keys with secret type hint (CredentialsConfiguration, TSecretValue) will be looked up by default.
    - provider gathers `toml` document fragments that contain source and destination credentials in path specified below
    - single values will not be retrieved, only toml fragments by default

    """

    def __init__(self, only_secrets: bool, only_toml_fragments: bool) -> None:
        """Initializes the toml backed Vault provider by loading a toml fragment from `dlt_secrets_toml` key and using it as initial configuration.

        _extended_summary_

        Args:
            only_secrets (bool): Only looks for secret values (CredentialsConfiguration, TSecretValue) by returning None (not found)
            only_toml_fragments (bool): Only load the known toml fragments and ignore any other lookups by returning None (not found)
        """
        self.only_secrets = only_secrets
        self.only_toml_fragments = only_toml_fragments
        self._vault_lookups: Dict[str, pendulum.DateTime] = {}

        super().__init__(tomlkit.document())
        self._update_from_vault(SECRETS_TOML_KEY, None, AnyType, None, ())

    def get_value(
        self, key: str, hint: type, pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        full_key = self.get_key_name(key, pipeline_name, *sections)

        value, _ = super().get_value(key, hint, pipeline_name, *sections)
        if value is None:
            # only secrets hints are handled
            if self.only_secrets and not is_secret_hint(hint) and hint is not AnyType:
                return None, full_key

            if pipeline_name:
                # loads dlt_secrets_toml for particular pipeline
                lookup_fk = self.get_key_name(SECRETS_TOML_KEY, pipeline_name)
                self._update_from_vault(lookup_fk, "", AnyType, pipeline_name, ())

            # generate auxiliary paths to get from vault
            for known_section in [known_sections.SOURCES, known_sections.DESTINATION]:

                def _look_at_idx(idx: int, full_path: Tuple[str, ...], pipeline_name: str) -> None:
                    lookup_key = full_path[idx]
                    lookup_sections = full_path[:idx]
                    lookup_fk = self.get_key_name(lookup_key, *lookup_sections)
                    self._update_from_vault(
                        lookup_fk, lookup_key, AnyType, pipeline_name, lookup_sections
                    )

                def _lookup_paths(pipeline_name_: str, known_section_: str) -> None:
                    with contextlib.suppress(ValueError):
                        full_path = sections + (key,)
                        if pipeline_name_:
                            full_path = (pipeline_name_,) + full_path
                        idx = full_path.index(known_section_)
                        _look_at_idx(idx, full_path, pipeline_name_)
                        # if there's element after index then also try it (destination name / source name)
                        if len(full_path) - 1 > idx:
                            _look_at_idx(idx + 1, full_path, pipeline_name_)

                # first query the shortest paths so the longer paths can override it
                _lookup_paths(None, known_section)  # check sources and sources.<source_name>
                if pipeline_name:
                    _lookup_paths(
                        pipeline_name, known_section
                    )  # check <pipeline_name>.sources and <pipeline_name>.sources.<source_name>

        value, _ = super().get_value(key, hint, pipeline_name, *sections)
        # skip checking the exact path if we check only toml fragments
        if value is None and not self.only_toml_fragments:
            # look for key in the vault and update the toml document
            self._update_from_vault(full_key, key, hint, pipeline_name, sections)
            value, _ = super().get_value(key, hint, pipeline_name, *sections)

        # if value:
        #     print(f"GSM got value for {key} {pipeline_name}-{sections}")
        # else:
        #     print(f"GSM FAILED value for {key} {pipeline_name}-{sections}")
        return value, full_key

    @property
    def supports_secrets(self) -> bool:
        return True

    @abc.abstractmethod
    def _look_vault(self, full_key: str, hint: type) -> str:
        pass

    def _update_from_vault(
        self, full_key: str, key: str, hint: type, pipeline_name: str, sections: Tuple[str, ...]
    ) -> None:
        if full_key in self._vault_lookups:
            return
        # print(f"tries '{key}' {pipeline_name} | {sections} at '{full_key}'")
        secret = self._look_vault(full_key, hint)
        self._vault_lookups[full_key] = pendulum.now()
        if secret is not None:
            self.set_value(key, auto_cast(secret), pipeline_name, *sections)

    @property
    def is_empty(self) -> bool:
        return False


class TomlFileProvider(BaseTomlProvider):
    def __init__(
        self, file_name: str, project_dir: str = None, add_global_config: bool = False
    ) -> None:
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

    def _read_toml_file(
        self, file_name: str, project_dir: str = None, add_global_config: bool = False
    ) -> tomlkit.TOMLDocument:
        self._file_name = file_name
        self._toml_path = os.path.join(project_dir or get_dlt_settings_dir(), file_name)
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
        return get_dlt_data_dir()

    def write_toml(self) -> None:
        assert (
            not self._add_global_config
        ), "Will not write configs when `add_global_config` flag was set"
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

    @property
    def is_writable(self) -> bool:
        return True


class SecretsTomlProvider(TomlFileProvider):
    def __init__(self, project_dir: str = None, add_global_config: bool = False) -> None:
        super().__init__(SECRETS_TOML, project_dir=project_dir, add_global_config=add_global_config)

    @property
    def name(self) -> str:
        return SECRETS_TOML

    @property
    def supports_secrets(self) -> bool:
        return True

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
