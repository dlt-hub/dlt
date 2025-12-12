"""Configuration for Fabric Warehouse destination - extends MSSQL configuration"""

import dataclasses
from typing import Optional, Final, ClassVar, Dict, Any
from dlt.common.configuration import configspec
from dlt.common.typing import TSecretStrValue
from dlt.destinations.impl.mssql.configuration import (
    MsSqlCredentials,
    MsSqlClientConfiguration,
)


@configspec(init=False)
class FabricCredentials(MsSqlCredentials):
    """Credentials for Microsoft Fabric Warehouse with Service Principal authentication.
    
    Fabric Warehouse requires Azure AD Service Principal authentication.
    This class automatically configures the connection string with:
    - ActiveDirectoryServicePrincipal authentication
    - LongAsMax=yes (required for UTF-8 collation support)
    - Proper ODBC driver settings
    """
    drivername: Final[str] = dataclasses.field(default="fabric", init=False, repr=False, compare=False)  # type: ignore
    
    # Override parent username/password to make them optional (will be set from Service Principal)
    username: str = None
    password: TSecretStrValue = None
    
    # Service Principal credentials
    tenant_id: str = None
    """Azure AD Tenant ID"""
    
    client_id: str = None
    """Azure AD Application (Service Principal) Client ID"""
    
    client_secret: TSecretStrValue = None
    """Azure AD Application (Service Principal) Client Secret"""
    
    # Mark username/password as auto-generated so dlt doesn't require them
    __config_gen_annotations__: ClassVar[list] = ["username", "password"]
    
    def on_partial(self) -> None:
        """Called during configuration resolution, before validation.
        Sets username/password from Service Principal credentials."""
        # Set username as client_id@tenant_id format for Service Principal
        if self.client_id and self.tenant_id:
            self.username = f"{self.client_id}@{self.tenant_id}"
        
        # Set password to client_secret
        if self.client_secret:
            self.password = self.client_secret
        
        # Call parent on_partial
        super().on_partial()
    
    def on_resolved(self) -> None:
        """Called after configuration is fully resolved."""
        # Ensure username/password are set (should be done in on_partial already)
        if not self.username and self.client_id and self.tenant_id:
            self.username = f"{self.client_id}@{self.tenant_id}"
        
        if not self.password and self.client_secret:
            self.password = self.client_secret
        
        # Call parent on_resolved
        super().on_resolved()
    
    def _get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build ODBC DSN dictionary with Fabric-specific settings."""
        params = super()._get_odbc_dsn_dict()
        
        # Add Fabric-required parameters
        params["AUTHENTICATION"] = "ActiveDirectoryServicePrincipal"
        params["LONGASMAX"] = "yes"  # Critical for UTF-8 collation support
        
        return params

@configspec
class FabricClientConfiguration(MsSqlClientConfiguration):
    """Configuration for Fabric Warehouse destination with collation support.
    
    Uses FabricCredentials for Service Principal authentication.
    LongAsMax=yes is automatically added to support UTF-8 collations.
    
    Example usage:
        fabric(
            credentials={
                "host": "mywarehouse.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "tenant_id": "your-tenant-id",
                "client_id": "your-client-id",
                "client_secret": "your-client-secret",
            },
            collation="Latin1_General_100_BIN2_UTF8",
        )
    """
    destination_type: Final[str] = "fabric"  # type: ignore[misc]
    credentials: FabricCredentials = None
    
    collation: Optional[str] = "Latin1_General_100_BIN2_UTF8"
    """Database collation for varchar columns. Fabric supports:
    - Latin1_General_100_BIN2_UTF8 (default, case-sensitive)
    - Latin1_General_100_CI_AS_KS_WS_SC_UTF8 (case-insensitive)
    
    Both have UTF-8 encoding. LongAsMax=yes is automatically configured.
    """

__all__ = ["FabricCredentials", "FabricClientConfiguration"]
