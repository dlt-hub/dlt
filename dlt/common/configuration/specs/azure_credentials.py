from typing import Optional, Dict, Any

from dlt.common import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
from dlt.common.configuration.specs.exceptions import InvalidBoto3Session
from dlt import version

import fsspec


@configspec
class AzureCredentialsWithoutDefaults(CredentialsConfiguration):
    """Credentials for azure blob storage, compatible with adlfs"""

    azure_storage_account_name: str = None
    azure_storage_account_key: Optional[TSecretStrValue] = None
    azure_storage_sas_token: TSecretStrValue = None
    azure_sas_token_permissions: str = "racwdl"
    """Permissions to use when generating a SAS token. Ignored when sas token is provided directly"""

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed as kwargs to adlfs"""
        return dict(
            account_name=self.azure_storage_account_name,
            account_key=self.azure_storage_account_key,
            sas_token=self.azure_storage_sas_token,
        )

    def create_sas_token(self) -> None:
        from azure.storage.blob import generate_account_sas, ResourceTypes

        self.azure_storage_sas_token = generate_account_sas(  # type: ignore[assignment]
            account_name=self.azure_storage_account_name,
            account_key=self.azure_storage_account_key,
            resource_types=ResourceTypes(container=True, object=True),
            permission=self.azure_sas_token_permissions,
            expiry=pendulum.now().add(days=1),
        )

    def on_partial(self) -> None:
        # sas token can be generated from account key
        if self.azure_storage_account_key and not self.azure_storage_sas_token:
            self.create_sas_token()
        if not self.is_partial():
            self.resolve()


@configspec
class AzureCredentials(AzureCredentialsWithoutDefaults, CredentialsWithDefault):
    def on_partial(self) -> None:
        from azure.identity import DefaultAzureCredential

        if not self.azure_storage_account_key and not self.azure_storage_sas_token:
            self._set_default_credentials(DefaultAzureCredential())
            if self.azure_storage_account_name:
                self.resolve()
        else:
            super().on_partial()

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        base_kwargs = super().to_adlfs_credentials()
        if self.has_default_credentials():
            base_kwargs["anon"] = False
        return base_kwargs
