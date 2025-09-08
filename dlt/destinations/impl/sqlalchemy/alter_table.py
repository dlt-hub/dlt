from typing import List

import sqlalchemy as sa
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations


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
        self.ctx = MigrationContext(dialect, None, {"as_sql": True, "output_buffer": self._buf})
        self.ops = Operations(self.ctx)

    def add_column(self, table_name: str, column: sa.Column, schema: str) -> None:
        self.ops.add_column(table_name, column, schema=schema)

    def consume_statements(self) -> List[str]:
        lines = self._buf.sql_lines[:]
        self._buf.sql_lines.clear()
        return lines
