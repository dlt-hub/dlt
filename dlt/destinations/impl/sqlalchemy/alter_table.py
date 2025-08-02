from typing import List

import sqlalchemy as sa
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.schema import CreateColumn

from dlt.common import logger


class ListBuffer:
    """A partial implementation of string IO to use with alembic.
    SQL statements are stored in a list instead of file/stdio
    """

    def __init__(self) -> None:
        self._buf = ""
        self.sql_lines: List[str] = []

    def write(self, data: str) -> None:
        self._buf += data

    def flush(self) -> None:
        if self._buf:
            self.sql_lines.append(self._buf)
            self._buf = ""


class MigrationMaker:
    def __init__(self, dialect: sa.engine.Dialect) -> None:
        self._buf = ListBuffer()
        self.dialect = dialect

        # Try to create MigrationContext, fallback to None if dialect not supported
        try:
            self.ctx = MigrationContext(
                dialect,
                None,
                {
                    "as_sql": True,
                    "output_buffer": self._buf,
                    "mssql_batch_separator": None,
                    "oracle_batch_separator": None,
                },
            )
            self.ops = Operations(self.ctx)
        except KeyError:
            # dialect was not found
            logger.warning(
                f"Alembic migrations not available for {dialect}, falling back to SQL generation."
            )
            self.ctx = None
            self.ops = None

    def add_column(self, table_name: str, column: sa.Column, schema: str) -> None:
        if self.ops:
            # Use alembic if available
            self.ops.add_column(table_name, column, schema=schema)
        else:
            # Fallback to direct SQLAlchemy for unsupported dialects
            prep = self.dialect.identifier_preparer  # type: ignore[attr-defined]
            fq_table = prep.quote_identifier(table_name)
            if schema:
                fq_table = f"{prep.quote_identifier(schema)}.{fq_table}"

            # Create column DDL
            col_ddl = CreateColumn(column).compile(
                dialect=self.dialect, compile_kwargs={"literal_binds": True}
            )

            # Generate ALTER TABLE statement
            sql_statement = f"ALTER TABLE {fq_table} ADD COLUMN {col_ddl}"
            self._buf.sql_lines.append(sql_statement)

    def consume_statements(self) -> List[str]:
        lines = self._buf.sql_lines[:]
        self._buf.sql_lines.clear()
        return lines
