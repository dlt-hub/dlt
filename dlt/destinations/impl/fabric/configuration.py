"""Configuration for Fabric Warehouse destination"""

import base64
import threading
import time
from typing import Optional, Final, ClassVar, Dict, Any, List, Literal, NamedTuple

from dlt.common import json
from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import AzureServicePrincipalCredentials
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from dlt.destinations.impl.mssql.configuration import SupportsGetToken, build_odbc_dsn
from dlt import version

_FABRIC_EXTRA = f"{version.DLT_PKG_NAME}[fabric]"

TFabricAuthMethod = Literal["credential", "access_token", "service_principal", "default"]
SERVICE_PRINCIPAL_FIELDS = ("azure_tenant_id", "azure_client_id", "azure_client_secret")


class FabricAccessToken(NamedTuple):
    """Entra ID access token in the shape of `azure.core.credentials.AccessToken`."""

    token: str
    expires_on: Optional[int]
    """POSIX timestamp, `None` if not known."""


def _get_token_expiration(token: str) -> Optional[int]:
    """Reads the `exp` claim of a JWT, returns `None` if `token` is not a JWT."""
    try:
        payload = token.split(".")[1]
        claims = json.loadb(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return int(claims["exp"])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


def _get_notebookutils_credentials() -> Any:
    """Returns `notebookutils.credentials`, `None` outside of a Microsoft Fabric notebook."""
    try:
        import notebookutils  # type: ignore[import-not-found,unused-ignore]
    except ModuleNotFoundError as ex:
        # raise if `notebookutils` is available but fails on its own imports
        if ex.name != "notebookutils":
            raise
        return None
    credentials = getattr(notebookutils, "credentials", None)
    return credentials if callable(getattr(credentials, "getToken", None)) else None


class FabricNotebookCredential(SupportsGetToken):
    """Authenticates as the identity a Microsoft Fabric notebook runs with.

    Has the `get_token` method of azure-identity credentials. Tokens come from NotebookUtils and
    are reused until shortly before they expire.
    """

    REFRESH_BEFORE_EXPIRATION: ClassVar[int] = 300
    """Seconds before expiration at which a token is replaced."""

    # shared by all instances: an instance holds only tokens, so it can be copied and pickled
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self) -> None:
        if not self.is_available():
            raise ConfigurationValueError(
                "`FabricNotebookCredential` needs `notebookutils.credentials.getToken`, which is"
                " available only in a Microsoft Fabric notebook."
            )
        self._tokens: Dict[str, FabricAccessToken] = {}

    @staticmethod
    def is_available() -> bool:
        """Tells if the code runs in a Microsoft Fabric notebook."""
        return _get_notebookutils_credentials() is not None

    def get_token(self, *scopes: str, **kwargs: Any) -> FabricAccessToken:
        """Returns a token of the notebook identity for the first of `scopes`."""
        # NotebookUtils expects the resource without the `.default` scope
        audience = scopes[0].removesuffix(".default")
        with self._lock:
            token = self._tokens.get(audience)
            if (
                token is None
                or token.expires_on is None
                or token.expires_on - self.REFRESH_BEFORE_EXPIRATION < time.time()
            ):
                raw_token = _get_notebookutils_credentials().getToken(audience)
                token = FabricAccessToken(raw_token, _get_token_expiration(raw_token))
                self._tokens[audience] = token
        return token


class _StaticTokenCredential(SupportsGetToken):
    def __init__(self, token: str) -> None:
        self._token = FabricAccessToken(token, _get_token_expiration(token))

    def get_token(self, *scopes: str, **kwargs: Any) -> FabricAccessToken:
        return self._token


@configspec(init=False)
class FabricCredentials(AzureServicePrincipalCredentials):
    """Credentials for Microsoft Fabric Warehouse with Entra ID authentication.

    The authentication method follows from what is configured, in this order:
    an azure-identity credential passed as native value, `access_token`, Service Principal
    fields, the notebook identity in a Microsoft Fabric notebook, `DefaultAzureCredential`.
    """

    drivername: str = "mssql+pyodbc"
    """SQLAlchemy driver name for SQL Server/Fabric."""

    host: str = None
    """Fabric Warehouse host (e.g., abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com)"""

    port: int = 1433
    """Database port (default: 1433)"""

    database: str = None
    """Fabric Warehouse database name"""

    connect_timeout: int = 15
    """Connection timeout in seconds (default: 15)"""

    access_token: Optional[TSecretStrValue] = None
    """Entra ID access token for the warehouse, takes precedence over the Service Principal."""

    # optional, so only `host` and `database` are reported as missing with other methods
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[TSecretStrValue] = None

    # Override to make optional - not needed for Fabric Warehouse credentials (only for staging)
    azure_storage_account_name: Optional[str] = None
    """Not used for Fabric Warehouse credentials (only staging credentials need this)"""

    def parse_native_representation(self, native_value: Any) -> None:
        """Imports an azure-identity credential exposing `get_token`."""
        # credentials created with `from_credential` hand over their azure-identity credential
        if isinstance(native_value, FabricCredentials) and native_value.is_external_session():
            native_value = native_value.default_credentials()
        super().parse_native_representation(native_value)
        # `host` and `database` are not part of the credential and still must be resolved
        self.__is_resolved__ = False

    def _get_auth_method(self) -> TFabricAuthMethod:
        """Returns the authentication method, the first one that is configured: azure-identity
        credential, `access_token`, Service Principal. Without any of them the notebook identity
        or `DefaultAzureCredential` is used.
        """
        # not stored when resolved: `copy` drops private attributes
        if self.is_external_session():
            return "credential"
        # an empty value is configured, ie. an env var set from a secret that is not available
        if self.access_token is not None:
            return "access_token"
        if any(getattr(self, name) is not None for name in SERVICE_PRINCIPAL_FIELDS):
            return "service_principal"
        return "default"

    def on_resolved(self) -> None:
        """Checks the authentication method that is configured.

        Raises:
            ConfigurationValueError: If the method is configured only in part.
        """
        method = self._get_auth_method()
        if method == "access_token" and not self.access_token:
            raise ConfigurationValueError(
                "`access_token` in Fabric credentials is empty. Set a token or remove"
                " `access_token` to use another authentication method."
            )
        if method == "service_principal":
            missing = [name for name in SERVICE_PRINCIPAL_FIELDS if not getattr(self, name)]
            if missing:
                raise ConfigurationValueError(
                    f"Service Principal in Fabric credentials is incomplete, missing: {missing}."
                    " Set all of `azure_tenant_id`, `azure_client_id` and `azure_client_secret`,"
                    " or none of them to use another authentication method."
                )
        if method == "default":
            # `DefaultAzureCredential` cannot sign in from a Fabric notebook
            if FabricNotebookCredential.is_available():
                self._set_default_credentials(FabricNotebookCredential())
            else:
                try:
                    from azure.identity import DefaultAzureCredential
                except ModuleNotFoundError:
                    raise MissingDependencyException(self.__class__.__name__, [_FABRIC_EXTRA])
                self._set_default_credentials(DefaultAzureCredential())

    def get_token_provider(self) -> Optional[SupportsGetToken]:
        """Returns the object with `get_token` that authenticates the connection.

        Returns:
            Optional[SupportsGetToken]: A token provider, `None` if the Service Principal in the
                connection string authenticates.
        """
        method = self._get_auth_method()
        if method == "access_token":
            return _StaticTokenCredential(str(self.access_token))
        if method == "service_principal":
            return None
        provider: SupportsGetToken = self.default_credentials()
        return provider

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build connection string parameters with Fabric-specific settings."""
        # only keywords accepted by `mssql-python`, which rejects ie. `DRIVER` and `LongAsMax`
        params = {
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        }

        # the driver refuses `AUTHENTICATION` together with a token provider
        if self._get_auth_method() == "service_principal":
            params["AUTHENTICATION"] = "ActiveDirectoryServicePrincipal"
            params["UID"] = f"{self.azure_client_id}@{self.azure_tenant_id}"
            params["PWD"] = str(self.azure_client_secret)

        return params

    def to_odbc_dsn(self) -> str:
        """Build ODBC style connection string for `mssql-python`."""
        return build_odbc_dsn(self.get_odbc_dsn_dict())

    def to_native_credentials(self) -> Optional[Any]:
        """Return credentials in a format suitable for the native driver/library."""
        return self.get_odbc_dsn_dict()


@configspec
class FabricClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    """Configuration for Fabric Warehouse destination with staging and collation support.

    Uses FabricCredentials for Entra ID authentication.
    Supports OneLake/Lakehouse or Azure Blob Storage staging with COPY INTO for efficient data loading.

    Example usage with OneLake/Lakehouse staging (recommended):
        fabric(
            credentials={
                "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "azure_tenant_id": "your-tenant-id",
                "azure_client_id": "your-client-id",
                "azure_client_secret": "your-client-secret",
            },
            staging=filesystem(
                # IMPORTANT: Must use workspace GUID and lakehouse GUID (not names)
                # Format: abfss://<workspace_guid>@onelake.dfs.fabric.microsoft.com/<lakehouse_guid>/Files
                bucket_url="abfss://12345678-1234-1234-1234-123456789012@onelake.dfs.fabric.microsoft.com/87654321-4321-4321-4321-210987654321/Files",
                # IMPORTANT: Must specify Service Principal credentials (same as warehouse)
                credentials={
                    "azure_storage_account_name": "onelake",
                    "azure_account_host": "onelake.blob.fabric.microsoft.com",
                    "azure_tenant_id": "your-tenant-id",
                    "azure_client_id": "your-client-id",
                    "azure_client_secret": "your-client-secret",
                },
            ),
            collation="Latin1_General_100_BIN2_UTF8",
        )

    Note: The bucket_url must use GUIDs for both workspace and lakehouse, not their display names.
    You can find these GUIDs in the Fabric portal workspace/lakehouse URLs.

    Example usage with Azure Blob Storage staging:
        fabric(
            credentials={
                "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "azure_tenant_id": "your-tenant-id",
                "azure_client_id": "your-client-id",
                "azure_client_secret": "your-client-secret",
            },
            staging=filesystem(
                bucket_url="az://your-container",
                credentials={
                    "azure_storage_account_name": "your-account-name",
                    "azure_storage_account_key": "your-account-key",
                },
            ),
            collation="Latin1_General_100_BIN2_UTF8",
        )
    """

    destination_type: Final[str] = "fabric"  # type: ignore[misc]
    credentials: FabricCredentials = None

    collation: Optional[str] = "Latin1_General_100_BIN2_UTF8"
    """Database collation for varchar columns, UTF-8 in both supported variants:
    `Latin1_General_100_BIN2_UTF8` (case-sensitive) and
    `Latin1_General_100_CI_AS_KS_WS_SC_UTF8` (case-insensitive).
    """

    # Set to False by default because PRIMARY KEY and UNIQUE constraints
    # are NOT ENFORCED in Fabric and can lead to inaccurate results
    create_indexes: bool = False
    """Whether `primary_key` and `unique` column hints are applied."""

    has_case_sensitive_identifiers: bool = True
    """Whether identifiers (table/column names) are case-sensitive. Depends on database collation."""

    __config_gen_annotations__: ClassVar[List[str]] = ["create_indexes"]

    def data_location(self) -> str:
        """Returns host:port."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        port = self.credentials.port or 1433
        return f"{self.credentials.host}:{port}"

    def fingerprint(self) -> str:
        """Returns a fingerprint of the physical Fabric location."""
        # a fingerprint can say "cannot compute", where a location raises instead
        try:
            return digest128(self.data_location())
        except ConfigurationValueError:
            return ""


__all__ = ["FabricCredentials", "FabricClientConfiguration", "FabricNotebookCredential"]
