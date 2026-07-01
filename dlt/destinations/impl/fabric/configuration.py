"""Configuration for Fabric Warehouse destination - extends Synapse configuration with COPY INTO support"""

from typing import Optional, Final, ClassVar, Dict, Any, List
from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import AzureServicePrincipalCredentials
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from dlt.destinations.impl.mssql.configuration import (
    apply_authentication_to_dsn,
    build_token_attrs_before,
    setup_token_credential,
    validate_authentication,
)

# Fabric Warehouse only supports Entra ID authentication, so it defaults to Service Principal
# (the shared MsSql auth machinery additionally enables azure-identity token methods).
_DEFAULT_AUTHENTICATION = "ActiveDirectoryServicePrincipal"


@configspec(init=False)
class FabricCredentials(AzureServicePrincipalCredentials):
    """Credentials for Microsoft Fabric Warehouse.

    Supports several Entra ID authentication methods, selected through `authentication`:

    * **Driver-native** (the ODBC driver authenticates): `ActiveDirectoryServicePrincipal`
      (default), `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`,
      `ActiveDirectoryInteractive`, `ActiveDirectoryMsi`.
    * **azure-identity** (dlt acquires an access token and injects it, works cross-platform):
      `ActiveDirectoryDefault` (alias `default`, uses `DefaultAzureCredential`),
      `ActiveDirectoryDeviceCode` (uses `DeviceCodeCredential`).

    When `authentication` is left at its default but no Service Principal secret is configured,
    dlt falls back to `ActiveDirectoryDefault` and injects its token.

    Inherits from AzureServicePrincipalCredentials for the Service Principal fields.
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

    authentication: str = _DEFAULT_AUTHENTICATION
    """Authentication method. Driver-native: `ActiveDirectoryServicePrincipal` (default),
    `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`, `ActiveDirectoryInteractive`,
    `ActiveDirectoryMsi`. azure-identity (token injected by dlt): `ActiveDirectoryDefault`
    (alias `default`), `ActiveDirectoryDeviceCode`."""

    username: str | None = None
    """User principal name, used with `ActiveDirectoryPassword` authentication."""

    password: TSecretStrValue | None = None
    """Password, used with `ActiveDirectoryPassword` authentication."""

    # Override to make optional - not needed for Fabric Warehouse credentials (only for staging)
    azure_storage_account_name: Optional[str] = None
    """Not used for Fabric Warehouse credentials (only staging credentials need this)"""

    def on_partial(self) -> None:
        """Set up token-based credentials and resolve once host and database are known.

        Token-based methods (and the default Service Principal method without a secret) get an
        azure-identity credential whose token is injected into the connection. Driver-native
        methods need no token and resolve as-is. Auth logic is shared with `MsSqlCredentials`.
        """
        setup_token_credential(self)
        # Resolve if we have the warehouse connection details (not the storage account name)
        if self.host and self.database:
            self.resolve()

    def on_resolved(self) -> None:
        """Validate the configured authentication method."""
        validate_authentication(self)

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build ODBC DSN dictionary with Fabric-specific settings."""
        params: dict[str, Any] = {
            "DRIVER": "{ODBC Driver 18 for SQL Server}",
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "LongAsMax": "yes",  # Required for UTF-8 collation support
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        }
        apply_authentication_to_dsn(self, params)
        return params

    def to_odbc_attrs_before(self) -> dict[int, bytes] | None:
        """Return pyodbc `attrs_before` with an Entra ID access token, or None for driver-native auth."""
        return build_token_attrs_before(self)

    def to_odbc_dsn(self) -> str:
        """Build ODBC connection string for pyodbc."""
        params = self.get_odbc_dsn_dict()
        return ";".join(f"{k}={v}" for k, v in params.items())

    def to_native_credentials(self) -> Optional[Any]:
        """Return credentials in a format suitable for the native driver/library."""
        return self.get_odbc_dsn_dict()


@configspec
class FabricClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    """Configuration for Fabric Warehouse destination with staging and collation support.

    Uses FabricCredentials for Service Principal authentication.
    Supports OneLake/Lakehouse or Azure Blob Storage staging with COPY INTO for efficient data loading.

    Example usage with OneLake/Lakehouse staging (recommended):
        fabric(
            credentials={
                "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "tenant_id": "your-tenant-id",
                "client_id": "your-client-id",
                "client_secret": "your-client-secret",
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
                "tenant_id": "your-tenant-id",
                "client_id": "your-client-id",
                "client_secret": "your-client-secret",
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
    credentials: Optional[FabricCredentials] = None

    collation: Optional[str] = "Latin1_General_100_BIN2_UTF8"
    """Database collation to use for text columns.

    Note: Fabric Warehouse does not support table indexing. Storage is automatically managed by the system.
    """

    # Set to False by default because PRIMARY KEY and UNIQUE constraints
    # are NOT ENFORCED in Fabric and can lead to inaccurate results
    create_indexes: bool = False
    """Whether `primary_key` and `unique` column hints are applied."""

    has_case_sensitive_identifiers: bool = True
    """Whether identifiers (table/column names) are case-sensitive. Depends on database collation."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "default_table_index_type",
        "create_indexes",
    ]

    """Database collation for varchar columns. Fabric supports:
    - Latin1_General_100_BIN2_UTF8 (default, case-sensitive)
    - Latin1_General_100_CI_AS_KS_WS_SC_UTF8 (case-insensitive)

    Both have UTF-8 encoding. LongAsMax=yes is automatically configured.
    """

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


__all__ = ["FabricCredentials", "FabricClientConfiguration"]
