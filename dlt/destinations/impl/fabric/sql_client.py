from contextlib import suppress

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.mssql.mssql_python_sql_client import MssqlPythonSqlClient


class FabricSqlClient(MssqlPythonSqlClient):
    def drop_tables(self, *tables: str) -> None:
        # same as Synapse: `DROP TABLE` without `IF EXISTS`, missing tables are skipped
        for table in tables:
            with suppress(DatabaseUndefinedRelation):
                self.execute_sql(f"DROP TABLE {self.make_qualified_table_name(table)}")
