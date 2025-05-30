import abc
import contextlib
from typing import Any, Dict, Optional, Set, Tuple

from dlt.common import logger
from dlt.common.typing import AnyType
from dlt.common.pendulum import pendulum
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import is_secret_hint

from .doc import BaseDocProvider

SECRETS_TOML_KEY = "dlt_secrets_toml"


class VaultDocProvider(BaseDocProvider):
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

    def __init__(
        self, only_secrets: bool, only_toml_fragments: bool, list_secrets: bool = False
    ) -> None:
        """Initializes the toml backed Vault provider by loading a toml fragment from `dlt_secrets_toml` key and using it as initial configuration.

        Args:
            only_secrets (bool): Only looks for secret values (CredentialsConfiguration, TSecretValue) by returning None (not found)
            only_toml_fragments (bool): Only load the known toml fragments and ignore any other lookups by returning None (not found)
            list_secrets (bool): When True, lists all available secrets once on first access to avoid unnecessary lookups
        """
        self.only_secrets = only_secrets
        self.only_toml_fragments = only_toml_fragments
        self.list_secrets = list_secrets
        self._vault_lookups: Dict[str, Any] = {}
        self._available_keys: Optional[Set[str]] = None
        if list_secrets and (only_toml_fragments or only_secrets):
            logger.warning(
                "Listing secrets is enabled so available keys are known upfront but one or both"
                f" only_toml_fragments ({only_toml_fragments}) only_secrets ({only_secrets}) are"
                " enabled too  so lookups of certain keys may be still skipped. Disable both to"
                " access all keys in vault."
            )
        super().__init__({})

    def get_value(
        self, key: str, hint: type, pipeline_name: str, *sections: str
    ) -> Tuple[Optional[Any], str]:
        # global settings must be updated first
        self._update_from_vault(SECRETS_TOML_KEY, None, AnyType, None, ())
        # then regular keys
        full_key = self.get_key_name(key, pipeline_name, *sections)
        value, _ = super().get_value(key, hint, pipeline_name, *sections)
        if value is None:
            if self.only_secrets and not is_secret_hint(hint):
                pass
            else:
                # only secrets hints are handled fully
                self._load_fragments(key, pipeline_name, *sections)
                value, _ = super().get_value(key, hint, pipeline_name, *sections)
                # skip checking the exact path if we check only toml fragments
                if value is None and not self.only_toml_fragments:
                    # look for key in the vault and update the toml document
                    self._update_from_vault(full_key, key, hint, pipeline_name, sections)
                    value, _ = super().get_value(key, hint, pipeline_name, *sections)

        return value, full_key

    @property
    def supports_secrets(self) -> bool:
        return True

    def clear_lookup_cache(self) -> None:
        self._vault_lookups.clear()

    def _load_fragments(self, key: str, pipeline_name: str, *sections: str) -> None:
        """Load known toml fragments from the vault
        * pipeline scoped SECRETS TOML KEY
        * destination and sources key
        * destination.<name> and sources.<name> key
        * the above scoped to pipeline
        """
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

    @abc.abstractmethod
    def _look_vault(self, full_key: str, hint: type) -> str:
        """Looks for `full_key` in the vault, may use `hint` to detect if expected value is a secret"""
        pass

    @abc.abstractmethod
    def _list_vault(self) -> Set[str]:
        """Lists keys in the vault in order to skip lookups for non existing keys

        Returns:
            Set[str]: A set of available key names in the vault
        """
        pass

    def _update_from_vault(
        self, full_key: str, key: str, hint: type, pipeline_name: str, sections: Tuple[str, ...]
    ) -> None:
        if full_key in self._vault_lookups:
            return

        # list the vault on first access if enabled
        if self.list_secrets and self._available_keys is None:
            self._available_keys = self._list_vault()

        # skip lookup if we know the key doesn't exist
        if (
            self.list_secrets
            and self._available_keys is not None
            and full_key not in self._available_keys
        ):
            self._vault_lookups[full_key] = pendulum.now()
            return

        # print(f"tries '{key}' {pipeline_name} | {sections} at '{full_key}'")
        logger.debug(f"Vault provider {self.name} will make a request for {full_key}")
        secret = self._look_vault(full_key, hint)
        self._vault_lookups[full_key] = pendulum.now()
        if secret is not None:
            self.set_fragment(key, secret, pipeline_name, *sections)

    @property
    def is_empty(self) -> bool:
        return False
