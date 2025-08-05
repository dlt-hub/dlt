from contextlib import suppress

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation


class SynapseSqlClient(PyOdbcMsSqlClient):
    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        # Synapse does not support DROP TABLE IF EXISTS.
        # Workaround: use DROP TABLE and suppress non-existence errors.
        statements = [f"DROP TABLE {self.make_qualified_table_name(table)}" for table in tables]
        for statement in statements:
            with suppress(DatabaseUndefinedRelation):
                self.execute_sql(statement)
