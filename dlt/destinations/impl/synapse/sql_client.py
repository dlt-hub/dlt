from contextlib import suppress

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation


class SynapseSqlClient(PyOdbcMsSqlClient):
    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        # Synapse does not support DROP TABLE IF EXISTS.
        # Workaround: use DROP TABLE and suppress non-existence errors.
        statements = []
        for table in tables:
            qual_table_name, qual_staging_table_name = self.get_qualified_table_names(table)
            statements += [f"DROP TABLE {qual_table_name};"]
            statements += [f"DROP TABLE {qual_staging_table_name};"]

        for statement in statements:
            with suppress(DatabaseUndefinedRelation):
                self.execute_sql(statement)
