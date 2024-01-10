from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence, List

from databricks import sql as databricks_lib
from databricks.sql.client import (
    Connection as DatabricksSqlConnection,
    Cursor as DatabricksSqlCursor,
)
from databricks.sql.exc import Error as DatabricksSqlError

from dlt.common import pendulum
from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame
from dlt.destinations.impl.databricks.configuration import DatabricksCredentials
from dlt.destinations.impl.databricks import capabilities
from dlt.common.time import to_py_date, to_py_datetime


class DatabricksCursorImpl(DBApiCursorImpl):
    native_cursor: DatabricksSqlCursor

    def df(self, chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]:
        if chunk_size is None:
            return self.native_cursor.fetchall(**kwargs)
        return super().df(chunk_size=chunk_size, **kwargs)


class DatabricksSqlClient(SqlClientBase[DatabricksSqlConnection], DBTransaction):
    dbapi: ClassVar[DBApi] = databricks_lib
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: DatabricksCredentials) -> None:
        super().__init__(credentials.catalog, dataset_name)
        self._conn: DatabricksSqlConnection = None
        self.credentials = credentials

    def open_connection(self) -> DatabricksSqlConnection:
        conn_params = self.credentials.to_connector_params()
        self._conn = databricks_lib.connect(**conn_params)
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        logger.warning(
            "NotImplemented: Databricks does not support transactions. Each SQL statement is"
            " auto-committed separately."
        )
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        logger.warning("NotImplemented: commit")
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        logger.warning("NotImplemented: rollback")

    @property
    def native_connection(self) -> "DatabricksSqlConnection":
        return self._conn

    def drop_tables(self, *tables: str) -> None:
        # Tables are drop with `IF EXISTS`, but databricks raises when the schema doesn't exist.
        # Multi statement exec is safe and the error can be ignored since all tables are in the same schema.
        with suppress(DatabaseUndefinedRelation):
            super().drop_tables(*tables)

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    def execute_fragments(
        self, fragments: Sequence[AnyStr], *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        """
        Executes several SQL fragments as efficiently as possible to prevent data copying.
        Default implementation just joins the strings and executes them together.
        """
        return [self.execute_sql(fragment, *args, **kwargs) for fragment in fragments]

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        curr: DBApiCursor = None
        if args:
            keys = [f"arg{i}" for i in range(len(args))]
            # Replace position arguments (%s) with named arguments (:arg0, :arg1, ...)
            query = query % tuple(f":{key}" for key in keys)
            db_args = {}
            for key, db_arg in zip(keys, args):
                # Databricks connector doesn't accept pendulum objects
                if isinstance(db_arg, pendulum.DateTime):
                    db_arg = to_py_datetime(db_arg)
                elif isinstance(db_arg, pendulum.Date):
                    db_arg = to_py_date(db_arg)
                db_args[key] = db_arg
        else:
            db_args = None
        with self._conn.cursor() as curr:
            try:
                curr.execute(query, db_args)
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
            catalog = self.capabilities.escape_identifier(self.credentials.catalog)
            dataset_name = self.capabilities.escape_identifier(self.dataset_name)
        else:
            catalog = self.credentials.catalog
            dataset_name = self.dataset_name
        return f"{catalog}.{dataset_name}"

    def _reset_connection(self) -> None:
        self.close_connection()
        self.open_connection()

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        if isinstance(ex, databricks_lib.ServerOperationError):
            if "TABLE_OR_VIEW_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            return DatabaseTerminalException(ex)
        return ex
        # elif isinstance(ex, databricks_lib.OperationalError):
        #     return DatabaseTerminalException(ex)
        # elif isinstance(ex, (databricks_lib.ProgrammingError, databricks_lib.IntegrityError)):
        #     return DatabaseTerminalException(ex)
        # elif isinstance(ex, databricks_lib.DatabaseError):
        #     return DatabaseTransientException(ex)
        # else:
        #     return ex

    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(
        databricks_ex: databricks_lib.DatabaseError,
    ) -> Optional[Exception]:
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, databricks_lib.DatabaseError)
