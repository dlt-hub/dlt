from typing import Optional, Dict, Any, Union

from dlt.common.pendulum import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials, WithPyicebergConfig
from dlt import version
from dlt.common.utils import without_none

_AZURE_STORAGE_EXTRA = f"{version.DLT_PKG_NAME}[az]"


@configspec
class AzureCredentialsBase(CredentialsConfiguration, WithObjectStoreRsCredentials):
    azure_storage_account_name: str = None
    azure_account_host: Optional[str] = None
    """Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net"""

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        pass

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        # https://docs.rs/object_store/latest/object_store/azure
        creds: Dict[str, Any] = without_none(self.to_adlfs_credentials())  # type: ignore[assignment]
        # only string options accepted
        creds.pop("anon", None)
        return creds


@configspec
class AzureCredentialsWithoutDefaults(AzureCredentialsBase, WithPyicebergConfig):
    """Credentials for Azure Blob Storage, compatible with adlfs"""

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
            account_host=self.azure_account_host,
        )

    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        return {
            "adlfs.account-name": self.azure_storage_account_name,
            "adlfs.account-key": self.azure_storage_account_key,
            "adlfs.sas-token": self.azure_storage_sas_token,
        }

    def create_sas_token(self) -> None:
        try:
            from azure.storage.blob import generate_account_sas, ResourceTypes
        except ModuleNotFoundError:
            raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])

        self.azure_storage_sas_token = generate_account_sas(
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
class AzureServicePrincipalCredentialsWithoutDefaults(AzureCredentialsBase, WithPyicebergConfig):
    azure_tenant_id: str = None
    azure_client_id: str = None
    azure_client_secret: TSecretStrValue = None

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        return dict(
            account_name=self.azure_storage_account_name,
            account_host=self.azure_account_host,
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret,
        )

    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        return {
            "adlfs.account-name": self.azure_storage_account_name,
            "adlfs.tenant-id": self.azure_tenant_id,
            "adlfs.client-id": self.azure_client_id,
            "adlfs.client-secret": self.azure_client_secret,
        }


@configspec
class AzureCredentials(AzureCredentialsWithoutDefaults, CredentialsWithDefault):
    def on_partial(self) -> None:
        try:
            from azure.identity import DefaultAzureCredential
        except ModuleNotFoundError:
            raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])

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
        try:
            from azure.identity import DefaultAzureCredential
        except ModuleNotFoundError:
            raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])

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
