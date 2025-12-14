"""SQL client for Fabric Warehouse - extends Synapse SQL client"""

from contextlib import suppress

from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation


class FabricSqlClient(SynapseSqlClient):
    """SQL client for Microsoft Fabric Warehouse

    Inherits all behavior from Synapse since Fabric Warehouse is built on Synapse technology.
    """

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        # Fabric Warehouse does not support DROP TABLE IF EXISTS (same as Synapse)
        # Workaround: use DROP TABLE and suppress non-existence errors
        statements = [f"DROP TABLE {self.make_qualified_table_name(table)}" for table in tables]
        for statement in statements:
            with suppress(DatabaseUndefinedRelation):
                self.execute_sql(statement)
