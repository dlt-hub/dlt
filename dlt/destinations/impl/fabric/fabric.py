"""Fabric Warehouse job client implementation"""

from typing import Type, Any, Dict

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema, TTableSchema
from dlt.common.schema import utils as schema_utils
from dlt.destinations.impl.mssql.mssql import MsSqlJobClient
from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient


class FabricJobClient(MsSqlJobClient):
    """Custom job client for Fabric Warehouse that uses varchar instead of nvarchar"""

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        """Override to use varchar instead of nvarchar for unique text columns"""
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # Fabric does not support nvarchar - use varchar with max length 900 for unique columns
            db_type = "varchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_destination_type(c, table)
        
        # Don't add COLLATE clause here - let the database default handle it
        # The warehouse-level collation will be applied automatically
        
        hints_str = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        """Override to ensure _dlt_version table uses varchar instead of text"""
        table = super().prepare_load_table(table_name)
        
        # For _dlt_version table, convert all text columns to varchar(max) to avoid
        # pyodbc binding them as legacy text/ntext types which don't support UTF-8 collations
        if table_name == self.schema.version_table_name:
            for column in table["columns"].values():
                if column.get("data_type") == "text":
                    # Override type mapper behavior for this specific table
                    # Use varchar(max) with explicit precision to avoid text/ntext binding
                    column["precision"] = 2147483647  # max value for varchar(max)
        
        return table
