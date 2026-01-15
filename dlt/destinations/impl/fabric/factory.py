"""
Custom Fabric Warehouse destination for dlt.

This module provides a custom destination class for Microsoft Fabric Warehouse
that extends Synapse capabilities with Fabric-specific requirements:
- Uses 'fabric' sqlglot dialect
- Uses varchar instead of nvarchar (Fabric doesn't support nvarchar)
- Supports COPY INTO for efficient data loading from staging
"""

from typing import Type, TYPE_CHECKING, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.destinations.impl.synapse.factory import synapse, SynapseTypeMapper
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema

from .configuration import FabricClientConfiguration

if TYPE_CHECKING:
    from .fabric import FabricClient


class FabricTypeMapper(SynapseTypeMapper):
    """Custom type mapper for Fabric Warehouse - replaces nvarchar with varchar and datetimeoffset with datetime2"""

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Override to limit precision to 6 and use datetime2 instead of datetimeoffset"""
        precision = column.get("precision")

        # Fabric only supports precision 0-6 (not 7 like SQL Server)
        if precision is not None:
            # Clamp to valid range for Fabric
            if precision < 0 or precision > 6:
                precision = 6

        # Fabric doesn't support datetimeoffset, always use datetime2
        # (timezone info is lost, but that's a Fabric limitation)
        if precision is not None:
            return f"datetime2({precision})"
        # Use precision 6 as default (instead of 7 in SQL Server)
        return "datetime2(6)"

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        """Override to use varchar instead of nvarchar and datetime2 instead of datetimeoffset"""
        sc_t = column["data_type"]
        if sc_t == "json":
            # Fabric doesn't have native JSON type, use varchar instead of nvarchar
            return "varchar(%s)" % column.get("precision", "max")

        if sc_t == "time":
            # Fabric Warehouse does not support TIME with precision parameter
            # Unlike SQL Server/Synapse, TIME must be used without precision
            return "time"

        # Get base type from parent
        db_type = super().to_destination_type(column, table)

        # Replace any nvarchar with varchar (Fabric doesn't support nvarchar)
        if "nvarchar" in db_type.lower():
            db_type = db_type.replace("nvarchar", "varchar").replace("NVARCHAR", "varchar")

        # Replace any datetimeoffset with datetime2 (Fabric doesn't support datetimeoffset)
        # This should be handled by to_db_datetime_type but just in case
        if "datetimeoffset" in db_type.lower():
            # Extract precision if present, e.g., datetimeoffset(7) -> datetime2(6)
            if "datetimeoffset(" in db_type.lower():
                import re

                match = re.search(r"datetimeoffset\((\d+)\)", db_type, re.IGNORECASE)
                if match:
                    precision = int(match.group(1))
                    precision = min(6, max(0, precision))  # Limit to 0-6
                    db_type = re.sub(
                        r"datetimeoffset\(\d+\)",
                        f"datetime2({precision})",
                        db_type,
                        flags=re.IGNORECASE,
                    )
                else:
                    db_type = db_type.replace("datetimeoffset", "datetime2").replace(
                        "DATETIMEOFFSET", "datetime2"
                    )
            else:
                db_type = db_type.replace("datetimeoffset", "datetime2").replace(
                    "DATETIMEOFFSET", "datetime2"
                )

        return db_type


class fabric(synapse):
    """Fabric Warehouse destination that extends Synapse with Fabric-specific dialect and type mappings"""

    spec: Type[FabricClientConfiguration] = FabricClientConfiguration  # type: ignore[assignment]

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        # Fabric uses the same file format strategy as Synapse:
        # - Direct loading: insert_values only (inherited from synapse)
        # - Staging: parquet (inherited from synapse)
        # Don't override preferred_loader_file_format or supported_loader_file_formats
        caps.has_case_sensitive_identifiers = True
        caps.sqlglot_dialect = "fabric"
        caps.type_mapper = FabricTypeMapper
        # Fabric only supports precision 0-6 for datetime2/time (not 7 like SQL Server)
        caps.max_timestamp_precision = 6
        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: FabricClientConfiguration,  # type: ignore[override]
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        """Adjust capabilities based on collation case sensitivity"""
        # Modify caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)  # type: ignore[arg-type]

    @property
    def client_class(self) -> Type["FabricClient"]:
        from .fabric import FabricClient

        return FabricClient


# Register the destination
fabric.register()
