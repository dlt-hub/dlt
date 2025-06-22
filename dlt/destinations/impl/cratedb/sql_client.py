import platform

from dlt.destinations.impl.cratedb.utils import SystemColumnWorkaround
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.common import logger
from dlt.destinations.typing import DBTransaction

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    from psycopg2cffi.sql import SQL, Composed, Composable
else:
    import psycopg2

from contextlib import contextmanager
from typing import Any, AnyStr, Iterator, Optional, Sequence, List

from dlt.common.destination.dataset import DBApiCursor

from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    raise_database_error,
)


class CrateDbApiCursorImpl(DBApiCursorImpl):
    """
    Compensate for patches by `SystemColumnWorkaround`.
    """

    def _get_columns(self) -> List[str]:
        if self.native_cursor.description:
            return [SystemColumnWorkaround.unquirk(c[0]) for c in self.native_cursor.description]
        return []


class CrateDbSqlClient(Psycopg2SqlClient):
    """
    CrateDB SQL client, mostly compatible with PostgreSQL, with a few deviations.

    - Use `doc` as a default search path.
    - Disable transactions.
    - Apply I/O patches provided by `SystemColumnWorkaround`.
    """

    def open_connection(self) -> "psycopg2.connection":
        """
        Use `doc` instead of `public` as search path with CrateDB.
        """
        self._conn = psycopg2.connect(
            dsn=self.credentials.to_native_representation(),
            options=f"-c search_path={self.fully_qualified_dataset_name()},doc",
        )
        self._reset_connection()
        return self._conn

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        """
        CrateDB does not support transactions. Make emitting `BEGIN TRANSACTION` a no-op.
        """
        logger.warning(
            "CrateDB does not support transactions. Each SQL statement is auto-committed"
            " separately."
        )
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        """
        CrateDB does not support transactions. Make emitting `COMMIT` a no-op.
        """
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        """
        CrateDB does not support transactions. Raise an exception on `ROLLBACK`.

        TODO: Any better idea?
        """
        raise NotImplementedError("CrateDB statements can not be rolled back")

    # @raise_database_error
    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        """
        Need to patch the result returned from the database.
        """
        result = super().execute_sql(sql, *args, **kwargs)
        return SystemColumnWorkaround.patch_result(result)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        """
        Need to patch the SQL statement and use the custom `CrateDbApiCursorImpl`.
        """
        query = SystemColumnWorkaround.patch_sql(query)
        curr: DBApiCursor = None
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, db_args)
                yield CrateDbApiCursorImpl(curr)
            except psycopg2.Error as outer:
                try:
                    self._reset_connection()
                except psycopg2.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer
