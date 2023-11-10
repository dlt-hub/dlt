from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, List

from databricks import sql as databricks_lib
from databricks.sql.client import (
    Connection as DatabricksSQLConnection,
    Cursor as DatabricksSQLCursor,
)
from databricks.sql.exc import Error as DatabricksSQLError

from dlt.common import logger
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
        self._conn: DatabricksSQLConnection = None
        self.credentials = credentials

    def open_connection(self) -> DatabricksSQLConnection:
        conn_params = self.credentials.to_connector_params()
        self._conn = databricks_lib.connect(
            **conn_params
        )
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        try:
            self._conn.close()
            self._conn = None
        except DatabricksSQLError as exc:
            logger.warning("Exception while closing connection: {}".format(exc))

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        logger.warning("NotImplemented: Databricks does not support transactions. Each SQL statement is auto-committed separately.")
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        logger.warning("NotImplemented: rollback")

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
        if escape:
            return self.capabilities.escape_identifier(self.dataset_name)
        return self.dataset_name

    def _reset_connection(self) -> None:
        self.close_connection()
        self.open_connection()

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        if isinstance(ex, databricks_lib.OperationalError):
            if "TABLE_OR_VIEW_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            return DatabaseTerminalException(ex)
        elif isinstance(ex, (databricks_lib.ProgrammingError, databricks_lib.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif isinstance(ex, databricks_lib.DatabaseError):
            return DatabaseTransientException(ex)
        else:
            return ex
