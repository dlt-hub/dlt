"""Configuration for Fabric Warehouse destination - extends Synapse configuration with COPY INTO support"""

import dataclasses
from typing import Optional, Final, ClassVar, Dict, Any, List
from dlt.common.configuration import configspec
from dlt.common.typing import TSecretStrValue
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.destinations.impl.synapse.configuration import SynapseCredentials


@configspec(init=False)
class FabricCredentials(SynapseCredentials):
    """Credentials for Microsoft Fabric Warehouse with Service Principal authentication.

    Fabric Warehouse requires Azure AD Service Principal authentication.
    This class automatically configures the connection string with:
    - ActiveDirectoryServicePrincipal authentication
    - LongAsMax=yes (required for UTF-8 collation support)
    - Proper ODBC driver settings
    """

    # Override parent username/password to make them optional (will be set from Service Principal)
    username: Optional[str] = None
    password: Optional[TSecretStrValue] = None

    # Service Principal credentials
    azure_tenant_id: Optional[str] = None
    """Azure AD Tenant ID"""

    azure_client_id: Optional[str] = None
    """Azure AD Application (Service Principal) Client ID"""

    azure_client_secret: Optional[TSecretStrValue] = None
    """Azure AD Application (Service Principal) Client Secret"""

    # Mark username/password as auto-generated so dlt doesn't require them
    __config_gen_annotations__: ClassVar[list[str]] = ["username", "password"]

    def on_partial(self) -> None:
        """Called during configuration resolution, before validation.
        Sets username/password from Service Principal credentials."""
        # Set username as client_id@tenant_id format for Service Principal
        if self.azure_client_id and self.azure_tenant_id:
            self.username = f"{self.azure_client_id}@{self.azure_tenant_id}"

        # Set password to client_secret
        if self.azure_client_secret:
            self.password = self.azure_client_secret

        # Call parent on_partial
        super().on_partial()

    def on_resolved(self) -> None:
        """Called after configuration is fully resolved."""
        # Ensure username/password are set (should be done in on_partial already)
        if not self.username and self.azure_client_id and self.azure_tenant_id:
            self.username = f"{self.azure_client_id}@{self.azure_tenant_id}"

        if not self.password and self.azure_client_secret:
            self.password = self.azure_client_secret

        # Call parent on_resolved
        super().on_resolved()

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build ODBC DSN dictionary with Fabric-specific settings."""
        params = super().get_odbc_dsn_dict()

        # Add Fabric-required parameters
        params["AUTHENTICATION"] = "ActiveDirectoryServicePrincipal"

        return params


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
            staging_config=filesystem(
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
            staging_config=filesystem(
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

    def on_resolved(self) -> None:
        """Auto-configure staging credentials from warehouse Service Principal credentials.

        When using OneLake/Lakehouse staging with Fabric, the filesystem destination needs
        Service Principal credentials (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
        and the OneLake blob endpoint (azure_account_host).
        Instead of requiring users to specify these, we automatically propagate them
        from the Fabric warehouse credentials to the staging filesystem credentials.
        """

        # If we have staging config and warehouse credentials with Service Principal
        if self.staging_config and self.credentials:
            staging_creds = self.staging_config.credentials

            # Check if staging is using OneLake (azure_storage_account_name == "onelake")
            if hasattr(staging_creds, "azure_storage_account_name"):
                if staging_creds.azure_storage_account_name == "onelake":
                    # OneLake staging - propagate Service Principal credentials
                    if self.credentials.azure_client_id and not hasattr(
                        staging_creds, "azure_client_id"
                    ):
                        staging_creds.azure_client_id = self.credentials.azure_client_id  # type: ignore
                    if self.credentials.azure_client_secret and not hasattr(
                        staging_creds, "azure_client_secret"
                    ):
                        staging_creds.azure_client_secret = self.credentials.azure_client_secret  # type: ignore
                    if self.credentials.azure_tenant_id and not hasattr(
                        staging_creds, "azure_tenant_id"
                    ):
                        staging_creds.azure_tenant_id = self.credentials.azure_tenant_id  # type: ignore

                    # Also set the OneLake blob endpoint if not already set
                    if (
                        not hasattr(staging_creds, "azure_account_host")
                        or not staging_creds.azure_account_host
                    ):
                        staging_creds.azure_account_host = "onelake.blob.fabric.microsoft.com"  # type: ignore

    # Set to False by default because PRIMARY KEY and UNIQUE constraints
    # are NOT ENFORCED in Fabric and can lead to inaccurate results
    create_indexes: bool = False
    """Whether `primary_key` and `unique` column hints are applied."""

    staging_use_msi: bool = False
    """Whether the managed identity of the Fabric workspace is used to authorize access to the staging Storage Account."""

    has_case_sensitive_identifiers: bool = False
    """Whether identifiers (table/column names) are case-sensitive. Depends on database collation."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "default_table_index_type",
        "create_indexes",
        "staging_use_msi",
    ]

    """Database collation for varchar columns. Fabric supports:
    - Latin1_General_100_BIN2_UTF8 (default, case-sensitive)
    - Latin1_General_100_CI_AS_KS_WS_SC_UTF8 (case-insensitive)

    Both have UTF-8 encoding. LongAsMax=yes is automatically configured.
    """


__all__ = ["FabricCredentials", "FabricClientConfiguration"]
