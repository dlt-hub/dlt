"""
Custom Fabric Warehouse destination for dlt.

This module provides a custom destination class for Microsoft Fabric Warehouse
that overrides the default MSSQL behavior to work with Fabric's specific requirements:
- Uses 'fabric' sqlglot dialect instead of 'tsql'
- Uses varchar instead of nvarchar (Fabric doesn't support nvarchar)
"""

from typing import Type, TYPE_CHECKING

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.impl.mssql.factory import mssql as MsSqlDestination, MsSqlTypeMapper
from dlt.common.normalizers.naming.naming import NamingConvention
from .configuration import FabricClientConfiguration

if TYPE_CHECKING:
    from dlt_pipelines.destinations.impl.fabric.fabric import FabricJobClient


class FabricTypeMapper(MsSqlTypeMapper):
    """Custom type mapper for Fabric Warehouse - replaces nvarchar with varchar and datetimeoffset with datetime2"""
    
    sct_to_unbound_dbt = {
        "json": "varchar(max)",  # Fabric doesn't have native JSON type, use varchar(max)
        "text": "varchar(max)",  # Fabric doesn't support nvarchar, use varchar
        "double": "float",
        "bool": "bit",
        "bigint": "bigint",
        "binary": "varbinary(max)",
        "date": "date",
        "timestamp": "datetime2(6)",  # Fabric requires explicit precision, default to 6
        "time": "time(6)",  # Fabric requires explicit precision for time
    }

    sct_to_dbt = {
        "text": "varchar(%i)",  # Fabric doesn't support nvarchar, use varchar
        "timestamp": "datetime2(%i)",  # Fabric doesn't support datetimeoffset, use datetime2
        "binary": "varbinary(%i)",
        "decimal": "decimal(%i,%i)",
        "time": "time(%i)",
        "wei": "decimal(%i,%i)",
    }

    dbt_to_sct = {
        "varchar": "text",  # Fabric uses varchar instead of nvarchar
        "float": "double",
        "bit": "bool",
        "datetime2": "timestamp",  # Fabric uses datetime2 instead of datetimeoffset
        "date": "date",
        "bigint": "bigint",
        "varbinary": "binary",
        "decimal": "decimal",
        "time": "time",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
        # No json mapping - varchar is used for json storage
    }

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Override to always use datetime2 instead of datetimeoffset (Fabric doesn't support datetimeoffset)"""
        from dlt.common import logger
        
        column_name = column["name"]
        table_name = table["name"] if table else "unknown"
        precision = column.get("precision")

        # Fabric's datetime2 allows precision from 0-6 (not 7 like MSSQL)
        if precision is not None and (precision < 0 or precision > 6):
            logger.warn(
                f"Fabric Warehouse supports precision between 0-6 for column"
                f" '{column_name}' in table '{table_name}'. Will use precision 6."
            )
            precision = 6
        
        # Default to 6 if not specified
        if precision is None:
            precision = 6

        # Always use datetime2, even if timezone is requested (Fabric doesn't support datetimeoffset)
        return f"datetime2({precision})"

    def to_db_time_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        """Override to ensure time types always have explicit precision (required by Fabric)"""
        precision = column.get("precision")
        
        # Fabric requires precision for time, limit to 0-6
        if precision is not None and (precision < 0 or precision > 6):
            precision = 6
        
        # Default to 6 if not specified
        if precision is None:
            precision = 6
            
        return f"time({precision})"


class fabric(MsSqlDestination):
    """Custom MSSQL destination that uses 'fabric' sqlglot dialect instead of 'tsql'"""
    
    spec = FabricClientConfiguration
    
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.sqlglot_dialect = "fabric"  # type: ignore[assignment]
        caps.type_mapper = FabricTypeMapper
        return caps

    @property
    def client_class(self) -> Type["FabricJobClient"]:
        from .fabric import FabricJobClient
        return FabricJobClient


# Register the destination
fabric.register()  # type: ignore[attr-defined]