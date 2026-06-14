import os
from copy import copy
from typing import Optional, Dict, Any, Union

from dlt.common.pendulum import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue, Self
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
from dlt.common.configuration.specs.exceptions import (
    InvalidAzureCredential,
    UnsupportedAuthenticationMethodException,
)
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials, WithPyicebergConfig
from dlt import version
from dlt.common.utils import without_none

_AZURE_STORAGE_EXTRA = f"{version.DLT_PKG_NAME}[az]"
_AZURE_STORAGE_SCOPE = "https://storage.azure.com/.default"


def _object_store_will_refresh_default() -> bool:
    """True for configurations that object store crate can refresh with current
    dlt consumers (lance, delta).
    * AKS workload identity or an env client-secret service principal.
    """
    try:
        from azure.identity._constants import EnvironmentVariables
    except ImportError:
        return False
    # reuses azure-identity's own env var groupings
    # cert / username-password are excluded (no object_store provider); managed identity is excluded
    # (not detectable from env without probing IMDS).
    return all(os.environ.get(v) for v in EnvironmentVariables.WORKLOAD_IDENTITY_VARS) or all(
        os.environ.get(v) for v in EnvironmentVariables.CLIENT_SECRET_VARS
    )


@configspec
class AzureCredentialsBase(CredentialsConfiguration, WithObjectStoreRsCredentials):
    azure_storage_account_name: str = None
    azure_account_host: Optional[str] = None
    """Alternative host when accessing blob storage endpoint ie. my_account.dfs.core.windows.net"""

    def is_external_session(self) -> bool:
        """Tells if default credentials are an azure-identity credential passed by the user"""
        return getattr(self, "_external_session", False)

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        pass

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        # https://docs.rs/object_store/latest/object_store/azure
        creds: Dict[str, Any] = without_none(self.to_adlfs_credentials())  # type: ignore[assignment]
        # object_store accepts only string options - the live credential and adlfs hint are dropped
        creds.pop("anon", None)
        creds.pop("credential", None)

        if isinstance(self, CredentialsWithDefault) and self.has_default_credentials():
            if self.is_external_session():
                # object_store cannot resolve a user-passed credential, so freeze a bearer token.
                # NOTE: relies on the consumer merging AZURE_* env into the passed options - verified
                # for both delta-rs (AzureConfigHelper) and lance (with_env_azure, unconditional)
                creds["azure_storage_token"] = (
                    self.default_credentials().get_token(_AZURE_STORAGE_SCOPE).token
                )

        return creds


class _AzureExternalSession:
    """Mixin enabling azure credentials with defaults to accept a user-passed azure-identity
    credential (e.g. `DefaultAzureCredential`) as an always-frozen external session."""

    def parse_native_representation(self, native_value: Any) -> None:
        """Imports an azure-identity credential exposing `get_token`"""
        if not hasattr(native_value, "get_token"):
            raise InvalidAzureCredential(self.__class__, native_value)
        self._set_default_credentials(native_value)  # type: ignore[attr-defined]
        self._external_session = True
        self.__is_resolved__ = True

    @classmethod
    def from_credential(cls, credential: Any) -> Self:
        self = cls()
        self.parse_native_representation(credential)
        return self

    def to_adlfs_credentials(self) -> Dict[str, Any]:
        base_kwargs: Dict[str, Any] = super().to_adlfs_credentials()  # type: ignore[misc]
        if self.is_external_session():  # type: ignore[attr-defined]
            # pass the user's credential object - adlfs uses and refreshes it in-process
            base_kwargs["credential"] = self.default_credentials()  # type: ignore[attr-defined]
        elif self.has_default_credentials():  # type: ignore[attr-defined]
            # adlfs resolves and refreshes via its own default chain
            base_kwargs["anon"] = False
        return base_kwargs

    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        if self.is_external_session():  # type: ignore[attr-defined]
            raise UnsupportedAuthenticationMethodException(
                "An external azure session cannot be used with pyiceberg on Azure. Configure a"
                " static account key, SAS token or service principal, or use default credentials"
                " with AZURE_STORAGE_ANON=false so adlfs resolves the same credential chain itself."
            )
        if self.has_default_credentials():  # type: ignore[attr-defined]
            # hand over: pass only the account name so adlfs (used by pyiceberg for ADLS) resolves
            # and refreshes through its own DefaultAzureCredential. pyiceberg forwards no `anon` flag
            # to adlfs, so this requires AZURE_STORAGE_ANON=false in the environment - otherwise
            # adlfs defaults to anonymous access and the read fails.
            return {"adls.account-name": self.azure_storage_account_name}  # type: ignore[attr-defined]
        config: Dict[str, Any] = super().to_pyiceberg_fileio_config()  # type: ignore[misc]
        return config


@configspec
class AzureCredentialsWithoutDefaults(AzureCredentialsBase, WithPyicebergConfig):
    """Credentials for Azure Blob Storage, compatible with adlfs"""

    azure_storage_account_key: Optional[TSecretStrValue] = None
    azure_storage_sas_token: TSecretStrValue = None
    azure_sas_token_permissions: str = "racwdl"
    """Permissions to use when generating a SAS token. Ignored when sas token is provided directly"""
    azure_sas_token_expiration_hours: float = 24.0
    """Lifetime in hours of the account SAS token minted from an account key. The minted SAS does not auto-refresh."""

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
            "adls.account-name": self.azure_storage_account_name,
            "adls.account-key": self.azure_storage_account_key,
            "adls.sas-token": self.azure_storage_sas_token,
        }

    @classmethod
    def from_pyiceberg_fileio_config(cls, file_io: Dict[str, Any]) -> Self:
        # we'll modify file_io so make a copy
        file_io = copy(file_io)
        # convert signed uri to credentials
        for key, value in list(file_io.items()):
            if key.startswith("adls.sas-token."):
                if "adls.account-name" not in file_io:
                    file_io["adls.account-name"] = key.split(".")[2]
                if "adls.sas-token" not in file_io:
                    file_io["adls.sas-token"] = value  # key value is a sas token
        credentials: Self = cls()
        credentials.azure_account_host = file_io.get("adls.connection-string")
        credentials.azure_storage_account_key = file_io.get("adls.account-key")
        credentials.azure_storage_account_name = file_io.get("adls.account-name")
        credentials.azure_storage_sas_token = file_io.get("adls.sas-token")
        # if not credentials.is_partial():
        #     credentials.resolve()
        return credentials

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
            expiry=pendulum.now().add(seconds=int(self.azure_sas_token_expiration_hours * 3600)),
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

    def to_pyiceberg_fileio_config(self) -> Dict[str, str]:
        return {
            "adls.account-name": self.azure_storage_account_name,
            "adls.tenant-id": self.azure_tenant_id,
            "adls.client-id": self.azure_client_id,
            "adls.client-secret": self.azure_client_secret,
        }

    @classmethod
    def from_pyiceberg_fileio_config(cls, file_io: Dict[str, Any]) -> Self:
        credentials: Self = cls()
        credentials.azure_tenant_id = file_io.get("adls.tenant-id")
        credentials.azure_client_id = file_io.get("adls.client-id")
        credentials.azure_storage_account_name = file_io.get("adls.account-name")
        credentials.azure_client_secret = file_io.get("adls.client-secret")
        # if not credentials.is_partial():
        #     credentials.resolve()
        return credentials


@configspec
class AzureCredentials(
    _AzureExternalSession, AzureCredentialsWithoutDefaults, CredentialsWithDefault
):
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


@configspec
class AzureServicePrincipalCredentials(
    _AzureExternalSession, AzureServicePrincipalCredentialsWithoutDefaults, CredentialsWithDefault
):
    def on_partial(self) -> None:
        try:
            from azure.identity import DefaultAzureCredential
        except ModuleNotFoundError:
            raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])

        self._set_default_credentials(DefaultAzureCredential())
        if self.azure_storage_account_name:
            self.resolve()


AnyAzureCredentials = Union[
    # Credentials without defaults come first because union types are attempted in order
    # and explicit config should supersede system defaults
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentials,
    AzureServicePrincipalCredentials,
]
