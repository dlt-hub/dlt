from typing import List, Dict, Union, cast
from contextlib import suppress

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation


class SynapseSqlClient(PyOdbcMsSqlClient):
    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        # Synapse does not support DROP TABLE IF EXISTS.
        # Workaround: use DROP TABLE and suppress non-existence errors.
        statements = [f"DROP TABLE {self.make_qualified_table_name(table)};" for table in tables]
        for statement in statements:
            with suppress(DatabaseUndefinedRelation):
                self.execute_sql(statement)

    def drop_columns(self, from_tables_drop_cols: List[Dict[str, Union[str, List[str]]]]) -> None:
        """Drops specified columns from specified tables if they exist"""
        statements = []
        for from_table_drop_cols in from_tables_drop_cols:
            table = cast(str, from_table_drop_cols["from_table"])
            for column in from_table_drop_cols["drop_columns"]:
                statements.append(
                    f"ALTER TABLE {self.make_qualified_table_name(table)} DROP COLUMN"
                    f" {self.escape_column_name(column)};"
                )

        self.execute_many(statements)
