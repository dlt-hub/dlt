"""Fabric Warehouse job client implementation - based on Synapse with COPY INTO support"""

from typing import Type, Sequence, List, cast
from copy import deepcopy

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema import Schema, TColumnHint
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.destinations.impl.synapse.synapse import SynapseClient, HINT_TO_SYNAPSE_ATTR, TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR
from dlt.destinations.impl.synapse.synapse_adapter import TABLE_INDEX_TYPE_HINT, TTableIndexType
from dlt.destinations.impl.fabric.configuration import FabricClientConfiguration
from dlt.destinations.impl.fabric.sql_client import FabricSqlClient


class FabricClient(SynapseClient):
    """Custom job client for Fabric Warehouse that uses varchar instead of nvarchar and supports COPY INTO"""

    def __init__(
        self,
        schema: Schema,
        config: FabricClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # Call grandparent init (MsSqlJobClient) but set up Fabric-specific client
        # We'll initialize our own sql_client below
        super(SynapseClient, self).__init__(schema, config, capabilities)
        self.config: FabricClientConfiguration = config
        
        # Create Fabric-specific SQL client
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient
        dataset_name, staging_dataset_name = MsSqlJobClient.create_dataset_names(schema, config)
        self.sql_client = FabricSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

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
        """Override to ensure proper table configuration for Fabric"""
        table = super(SynapseClient, self).prepare_load_table(table_name)
        
        if self.in_staging_dataset_mode:
            # Staging tables should always be heap tables
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]

        table_index_type = cast(TTableIndexType, table.get(TABLE_INDEX_TYPE_HINT))
        if table_name in self.schema.dlt_table_names():
            # dlt tables should always be heap tables
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]
        else:
            if TABLE_INDEX_TYPE_HINT not in table:
                # If present in parent table, fetch hint from there
                table[TABLE_INDEX_TYPE_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                    self.schema.tables, table_name, TABLE_INDEX_TYPE_HINT, allow_none=True
                )
        if table[TABLE_INDEX_TYPE_HINT] is None:  # type: ignore[typeddict-item]
            # Hint still not defined, fall back to default
            table[TABLE_INDEX_TYPE_HINT] = self.config.default_table_index_type  # type: ignore[typeddict-unknown-key]
        
        # For _dlt_version table, convert all text columns to varchar(max) to avoid
        # pyodbc binding them as legacy text/ntext types which don't support UTF-8 collations
        if table_name == self.schema.version_table_name:
            for column in table["columns"].values():
                if column.get("data_type") == "text":
                    # Override type mapper behavior for this specific table
                    # Use varchar(max) with explicit precision to avoid text/ntext binding
                    column["precision"] = 2147483647  # max value for varchar(max)
        
        return table

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
