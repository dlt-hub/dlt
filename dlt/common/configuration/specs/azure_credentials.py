from typing import Optional, Dict, Any, Union

from dlt.common.pendulum import pendulum
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)


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

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        # https://docs.rs/object_store/latest/object_store/azure
        creds = self.to_adlfs_credentials()
        if creds["sas_token"] is None:
            creds.pop("sas_token")
        if creds["account_key"] is None:
            creds.pop("account_key")
        return creds

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
class AzureServicePrincipalCredentialsWithoutDefaults(CredentialsConfiguration):
    azure_storage_account_name: str = None
    azure_tenant_id: str = None
    azure_client_id: str = None
    azure_client_secret: TSecretStrValue = None

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        return dict(
            account_name=self.azure_storage_account_name,
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret,
        )

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        # https://docs.rs/object_store/latest/object_store/azure
        return self.to_adlfs_credentials()


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


@configspec
class AzureServicePrincipalCredentials(
    AzureServicePrincipalCredentialsWithoutDefaults, CredentialsWithDefault
):
    def on_partial(self) -> None:
        from azure.identity import DefaultAzureCredential

        self._set_default_credentials(DefaultAzureCredential())
        if self.azure_storage_account_name:
            self.resolve()

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        base_kwargs = super().to_adlfs_credentials()
        if self.has_default_credentials():
            base_kwargs["anon"] = False
        return base_kwargs


AnyAzureCredentials = Union[
    # Credentials without defaults come first because union types are attempted in order
    # and explicit config should supersede system defaults
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentials,
    AzureServicePrincipalCredentials,
]
