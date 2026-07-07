import threading
from typing import Any, Optional, Sequence, Set

from dlt.common import logger
from dlt.common.configuration.specs import AzureKeyVaultCredentials
from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.exceptions import MissingDependencyException
from dlt import version

from .vault import VaultDocProvider, normalize_key
from .provider import get_key_name

SECRET_NAME_SEPARATOR = "-"


class AzureKeyVaultProvider(VaultDocProvider):
    def __init__(
        self,
        credentials: AzureKeyVaultCredentials,
        only_secrets: bool = True,
        only_toml_fragments: bool = True,
        list_secrets: bool = False,
    ) -> None:
        """Initialize an Azure Key Vault Provider to access secrets stored in Azure Key Vault

        Args:
            credentials: Azure credentials with the Key Vault URL to access secrets
            only_secrets: When True, only keys with secret hint types will be looked up
            only_toml_fragments: When True, only load known TOML fragments and ignore other lookups
            list_secrets: When True, list all secrets upfront to optimize vault access by
                          avoiding lookups for non-existent secrets. Requires the
                          `Key Vault Secrets Officer` or `list` permission on secrets.
        """
        self.credentials = credentials
        self._client: Any = None
        self._client_lock = threading.Lock()
        super().__init__(only_secrets, only_toml_fragments, list_secrets)

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        """Makes key name for the secret by joining normalized components with `-`

        Azure Key Vault secret names may contain only alphanumeric characters and dashes, so
        punctuation is removed from name components and underscores are replaced with dashes.
        """
        normalized_sections = [normalize_key(section) for section in sections if section]
        key_name = get_key_name(normalize_key(key), SECRET_NAME_SEPARATOR, *normalized_sections)
        return key_name.replace("_", "-")

    @property
    def name(self) -> str:
        return "Azure Key Vault"

    @property
    def locations(self) -> Sequence[str]:
        if self.credentials and self.credentials.azure_key_vault_url:
            return [self.credentials.azure_key_vault_url]
        else:
            return super().locations

    def _get_client(self) -> Any:
        """Creates an Azure Key Vault secret client on first use, client creation is not thread-safe"""
        with self._client_lock:
            if self._client is None:
                try:
                    from azure.keyvault.secrets import SecretClient
                except ModuleNotFoundError:
                    raise MissingDependencyException(
                        "AzureKeyVaultProvider",
                        [f"{version.DLT_PKG_NAME}[az_secrets]"],
                        "We need azure-keyvault-secrets to create the client for Azure Key Vault.",
                    )
                self._client = SecretClient(
                    vault_url=self.credentials.azure_key_vault_url,
                    credential=self.credentials.to_native_credentials(),
                )
            return self._client

    def _look_vault(self, full_key: str, hint: type) -> Optional[str]:
        client = self._get_client()

        from azure.core.exceptions import (
            ClientAuthenticationError,
            HttpResponseError,
            ResourceNotFoundError,
        )

        try:
            return client.get_secret(full_key).value  # type: ignore[no-any-return]
        except ResourceNotFoundError:
            return None
        except ClientAuthenticationError as error:
            logger.warning(
                f"dlt could not authenticate to Azure Key Vault to read {full_key}: {error.message}"
            )
            return None
        except HttpResponseError as error:
            if error.status_code == 403:
                logger.warning(
                    f"dlt does not have `get` permission for {full_key} in Azure Key Vault:"
                    f" {error.message}"
                )
                return None
            raise

    def _list_vault(self) -> Set[str]:
        """Lists secret names in the Key Vault so lookups for non-existent secrets are skipped"""
        client = self._get_client()

        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError

        available_keys: Set[str] = set()
        try:
            for secret in client.list_properties_of_secrets():
                if secret.name:
                    available_keys.add(secret.name)
        except (ClientAuthenticationError, HttpResponseError) as error:
            raise ConfigProviderException(
                self.name,
                "Cannot list secrets: dlt does not have `list` permission on Azure Key Vault"
                " secrets. Secret listing is required when list_secrets=True to optimize vault"
                f" access by skipping lookups for non-existent secrets. Error: {error.message}",
            )
        logger.info(f"Listed {len(available_keys)} secrets from Azure Key Vault")
        return available_keys
