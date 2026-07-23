import threading
from typing import Any, Optional, Sequence, Set

from dlt.common import logger
from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.exceptions import MissingDependencyException
from dlt import version

from .vault import VaultDocProvider, normalize_key
from .provider import get_key_name

SECRET_NAME_SEPARATOR = "-"
_AZURE_KEY_VAULT_EXTRA = f"{version.DLT_PKG_NAME}[azure_key_vault]"


class AzureKeyVaultProvider(VaultDocProvider):
    def __init__(
        self,
        vault_url: str,
        credential: Any = None,
        only_secrets: bool = True,
        only_toml_fragments: bool = True,
        list_secrets: bool = False,
    ) -> None:
        """Access secrets stored in Azure Key Vault.

        Args:
            vault_url (str): The URL of the Azure Key Vault, e.g. `https://myvault.vault.azure.net/`.
            credential (Any): An `azure-identity` credential instance. When `None`,
                `DefaultAzureCredential` is created automatically.
            only_secrets (bool): When True, only keys with secret hint types will be looked up.
            only_toml_fragments (bool): When True, only load known TOML fragments and ignore other lookups.
            list_secrets (bool): When True, list all secrets upfront to optimize vault access by
                avoiding lookups for non-existent secrets.
        """
        self.vault_url = vault_url
        self._credential = credential
        self._client: Any = None
        self._client_lock = threading.Lock()
        super().__init__(only_secrets, only_toml_fragments, list_secrets)

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        """Joins normalized key components with `-`.

        Azure Key Vault secret names allow alphanumerics and hyphens (1-127 chars).
        """
        normalized_sections = [normalize_key(section).replace("_", "-") for section in sections if section]
        return get_key_name(normalize_key(key).replace("_", "-"), SECRET_NAME_SEPARATOR, *normalized_sections)

    @property
    def name(self) -> str:
        return "Azure Key Vault"

    @property
    def locations(self) -> Sequence[str]:
        return [self.vault_url]

    def _get_credential(self) -> Any:
        if self._credential is not None:
            return self._credential
        try:
            from azure.identity import DefaultAzureCredential
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "AzureKeyVaultProvider",
                [_AZURE_KEY_VAULT_EXTRA],
                "We need azure-identity for authentication with Azure Key Vault",
            )
        self._credential = DefaultAzureCredential()
        return self._credential

    def _get_client(self) -> Any:
        with self._client_lock:
            if self._client is None:
                try:
                    from azure.keyvault.secrets import SecretClient  # type: ignore[import-untyped]
                except ModuleNotFoundError:
                    raise MissingDependencyException(
                        "AzureKeyVaultProvider",
                        [_AZURE_KEY_VAULT_EXTRA],
                        "We need azure-keyvault-secrets to access Azure Key Vault",
                    )
                self._client = SecretClient(
                    vault_url=self.vault_url, credential=self._get_credential()
                )
            return self._client

    def _look_vault(self, full_key: str, hint: type) -> Optional[str]:
        client = self._get_client()

        from azure.core.exceptions import (
            HttpResponseError,
            ResourceNotFoundError,
            ServiceRequestError,
        )

        try:
            secret = client.get_secret(full_key)
            return secret.value  # type: ignore[no-any-return]
        except ResourceNotFoundError:
            return None
        except HttpResponseError as error:
            if error.status_code == 403:
                logger.warning(
                    f"Access denied when reading secret {full_key} from Azure Key Vault"
                    f" ({self.vault_url}): {error.message}"
                )
                return None
            raise
        except ServiceRequestError as error:
            logger.warning(
                f"Unable to connect to Azure Key Vault ({self.vault_url}): {error.message}"
            )
            raise

    def _list_vault(self) -> Set[str]:
        client = self._get_client()

        from azure.core.exceptions import HttpResponseError, ServiceRequestError

        available_keys: Set[str] = set()
        try:
            for secret_properties in client.list_properties_of_secrets():
                if secret_properties.name and secret_properties.enabled:
                    available_keys.add(secret_properties.name)
        except HttpResponseError as error:
            if error.status_code == 403:
                raise ConfigProviderException(
                    self.name,
                    f"Cannot list secrets: access denied for Azure Key Vault ({self.vault_url})."
                    " Secret listing is required when list_secrets=True to optimize vault"
                    " access by skipping lookups for non-existent secrets."
                    f" Error: {error.message}",
                )
            raise ConfigProviderException(
                self.name,
                f"Failed to list secrets in Azure Key Vault ({self.vault_url})."
                " Secret listing is required when list_secrets=True to optimize vault"
                f" access by skipping lookups for non-existent secrets. Error: {error.message}",
            )
        except ServiceRequestError as error:
            logger.warning(
                f"Unable to connect to Azure Key Vault ({self.vault_url}): {error.message}"
            )
            raise
        logger.info(f"Listed {len(available_keys)} secrets from Azure Key Vault ({self.vault_url})")
        return available_keys
