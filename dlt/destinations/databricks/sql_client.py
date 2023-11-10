from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, List

from databricks import sql as databricks_lib
from databricks.sql.client import (
    Connection as DatabricksSQLConnection,
    Cursor as DatabricksSQLCursor,
)

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase, raise_database_error, raise_open_connection_error
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame
from dlt.destinations.databricks.configuration import DatabricksCredentials
from dlt.destinations.databricks import capabilities

class DatabricksCursorImpl(DBApiCursorImpl):
    native_cursor: DatabricksSQLCursor  # type: ignore[assignment]

    def df(self, chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]:
        if chunk_size is None:
            return self.native_cursor.fetchall(**kwargs)
        return super().df(chunk_size=chunk_size, **kwargs)


class DatabricksSqlClient(SqlClientBase[DatabricksSQLConnection], DBTransaction):

    dbapi: ClassVar[DBApi] = databricks_lib
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: DatabricksCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: databricks_lib.connect(credentials) = None
        self.credentials = credentials

    def open_connection(self) -> DatabricksSQLConnection:
        conn_params = self.credentials.to_connector_params()
        # set the timezone to UTC so when loading from file formats that do not have timezones
        # we get dlt expected UTC
        if "timezone" not in conn_params:
            conn_params["timezone"] = "UTC"
        self._conn = databricks_lib.connect(
            **conn_params
        )
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.autocommit(False)
            yield self
            self.commit_transaction()
        except Exception:
            self.rollback_transaction()
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()
        self._conn.autocommit(True)

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()
        self._conn.autocommit(True)

    @property
    def native_connection(self) -> "DatabricksSQLConnection":
        return self._conn

    def drop_tables(self, *tables: str) -> None:
        # Tables are drop with `IF EXISTS`, but databricks raises when the schema doesn't exist.
        # Multi statement exec is safe and the error can be ignored since all tables are in the same schema.
        with suppress(DatabaseUndefinedRelation):
            super().drop_tables(*tables)

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        curr: DBApiCursor = None
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:  # type: ignore[assignment]
            try:
                curr.execute(query, db_args, num_statements=0)
                yield DatabricksCursorImpl(curr)  # type: ignore[abstract]
            except databricks_lib.Error as outer:
                try:
                    self._reset_connection()
                except databricks_lib.Error:
                    self.close_connection()
                    self.open_connection()
                raise outer

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        # Always escape for uppercase
        if escape:
            return self.capabilities.escape_identifier(self.dataset_name)
        return self.dataset_name.upper()

    def _reset_connection(self) -> None:
        self._conn.rollback()
        self._conn.autocommit(True)

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, databricks_lib.ProgrammingError):
            if ex.sqlstate == 'P0000' and ex.errno == 100132:
                # Error in a multi statement execution. These don't show the original error codes
                msg = str(ex)
                if "NULL result in a non-nullable column" in msg:
                    return DatabaseTerminalException(ex)
                elif "does not exist or not authorized" in msg:  # E.g. schema not found
                    return DatabaseUndefinedRelation(ex)
                else:
                    return DatabaseTransientException(ex)
            if ex.sqlstate in {'42S02', '02000'}:
                return DatabaseUndefinedRelation(ex)
            elif ex.sqlstate == '22023':  # Adding non-nullable no-default column
                return DatabaseTerminalException(ex)
            elif ex.sqlstate == '42000' and ex.errno == 904:  # Invalid identifier
                return DatabaseTerminalException(ex)
            elif ex.sqlstate == "22000":
                return DatabaseTerminalException(ex)
            else:
                return DatabaseTransientException(ex)

        elif isinstance(ex, databricks_lib.IntegrityError):
            raise DatabaseTerminalException(ex)
        elif isinstance(ex, databricks_lib.DatabaseError):
            term = cls._maybe_make_terminal_exception_from_data_error(ex)
            if term:
                return term
            else:
                return DatabaseTransientException(ex)
        elif isinstance(ex, TypeError):
            # databricks raises TypeError on malformed query parameters
            return DatabaseTransientException(databricks_lib.ProgrammingError(str(ex)))
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(databricks_ex: databricks_lib.DatabaseError) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, databricks_lib.DatabaseError)
