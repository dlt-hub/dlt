"""Configuration for Fabric Warehouse destination - extends Synapse configuration with COPY INTO support"""

from typing import Optional, Final, ClassVar, Dict, Any, List
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import AzureServicePrincipalCredentials
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt import version

_AZURE_STORAGE_EXTRA = f"{version.DLT_PKG_NAME}[az]"


@configspec(init=False)
class FabricCredentials(AzureServicePrincipalCredentials):
    """Credentials for Microsoft Fabric Warehouse with Service Principal authentication.

    Fabric Warehouse requires Azure AD Service Principal authentication.
    Inherits from AzureServicePrincipalCredentials for Service Principal fields and
    automatic fallback to DefaultAzureCredential.
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

    # Override to make optional - not needed for Fabric Warehouse credentials (only for staging)
    azure_storage_account_name: Optional[str] = None
    """Not used for Fabric Warehouse credentials (only staging credentials need this)"""

    def on_partial(self) -> None:
        """Enable fallback to DefaultAzureCredential if explicit credentials not provided."""
        try:
            from azure.identity import DefaultAzureCredential
        except ModuleNotFoundError:
            raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])

        # If no explicit Service Principal credentials, use default credentials
        if not self.azure_client_id or not self.azure_client_secret or not self.azure_tenant_id:
            self._set_default_credentials(DefaultAzureCredential())
            # Resolve if we have warehouse connection details (not storage account name)
            if self.host and self.database:
                self.resolve()

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build ODBC DSN dictionary with Fabric-specific settings."""
        params = {
            "DRIVER": "{ODBC Driver 18 for SQL Server}",
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "AUTHENTICATION": "ActiveDirectoryServicePrincipal",
            "LongAsMax": "yes",  # Required for UTF-8 collation support
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        }

        # Add Service Principal credentials if provided
        if self.azure_client_id and self.azure_tenant_id and self.azure_client_secret:
            params["UID"] = f"{self.azure_client_id}@{self.azure_tenant_id}"
            params["PWD"] = str(self.azure_client_secret)

        return params

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


__all__ = ["FabricCredentials", "FabricClientConfiguration"]
