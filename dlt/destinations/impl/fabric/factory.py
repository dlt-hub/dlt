"""
Custom Fabric Warehouse destination for dlt.

This module provides a custom destination class for Microsoft Fabric Warehouse
that extends Synapse capabilities with Fabric-specific requirements:
- Uses 'fabric' sqlglot dialect
- Uses varchar instead of nvarchar (Fabric doesn't support nvarchar)
- Supports COPY INTO for efficient data loading from staging
"""

from typing import Type, TYPE_CHECKING

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_mssql_literal
from dlt.destinations.impl.synapse.factory import synapse, SynapseTypeMapper
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema

from .configuration import FabricClientConfiguration

if TYPE_CHECKING:
    from .fabric import FabricClient


class FabricTypeMapper(SynapseTypeMapper):
    """Custom type mapper for Fabric Warehouse - replaces nvarchar with varchar"""
    
    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        """Override to use varchar instead of nvarchar for all text types"""
        sc_t = column["data_type"]
        if sc_t == "json":
            # Fabric doesn't have native JSON type, use varchar instead of nvarchar
            return "varchar(%s)" % column.get("precision", "max")
        
        # Get base type from parent
        db_type = super().to_destination_type(column, table)
        
        # Replace any nvarchar with varchar (Fabric doesn't support nvarchar)
        if "nvarchar" in db_type.lower():
            db_type = db_type.replace("nvarchar", "varchar").replace("NVARCHAR", "varchar")
        
        return db_type


class fabric(synapse):
    """Fabric Warehouse destination that extends Synapse with Fabric-specific dialect and type mappings"""
    
    spec: Type[FabricClientConfiguration] = FabricClientConfiguration
    
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.sqlglot_dialect = "fabric"  # type: ignore[assignment]
        caps.type_mapper = FabricTypeMapper
        return caps

    @property
    def client_class(self) -> Type["FabricClient"]:
        from .fabric import FabricClient
        return FabricClient


# Register the destination
fabric.register()