from contextlib import contextmanager, suppress
from typing import (
    Any,
    AnyStr,
    ClassVar,
    Generator,
    Iterator,
    Optional,
    Sequence,
)
from databricks import sql as databricks_lib
from databricks.sql.client import (
    Connection as DatabricksSqlConnection,
    Cursor as DatabricksSqlCursor,
)
from databricks.sql.exc import Error as DatabricksSqlError

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
from dlt.destinations.typing import ArrowTable, DBApi, DBTransaction, DataFrame
from dlt.destinations.impl.databricks.configuration import DatabricksCredentials
from dlt.common.destination.dataset import DBApiCursor


class DatabricksCursorImpl(DBApiCursorImpl):
    """Use native data frame support if available"""

    native_cursor: DatabricksSqlCursor  # type: ignore[assignment, unused-ignore]
    vector_size: ClassVar[int] = 2048  # vector size is 2048

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        if chunk_size is None:
            yield self.native_cursor.fetchall_arrow()
            return
        while True:
            table = self.native_cursor.fetchmany_arrow(chunk_size)
            if table.num_rows == 0:
                return
            yield table

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        for table in self.iter_arrow(chunk_size=chunk_size):
            yield table.to_pandas()


class DatabricksSqlClient(SqlClientBase[DatabricksSqlConnection], DBTransaction):
    dbapi: ClassVar[DBApi] = databricks_lib

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: DatabricksCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.catalog, dataset_name, staging_dataset_name, capabilities)
        self._conn: DatabricksSqlConnection = None
        self.credentials = credentials

    def open_connection(self) -> DatabricksSqlConnection:
        conn_params = self.credentials.to_connector_params()

        self._conn = databricks_lib.connect(
            **conn_params, schema=self.dataset_name, use_inline_params="silent"
        )
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def begin_transaction(self) -> Iterator[DBTransaction]:
        # Databricks does not support transactions
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        # Databricks does not support transactions
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        # Databricks does not support transactions
        pass

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

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # TODO: Inline param support will be dropped in future databricks driver, switch to :named paramstyle
        # This will drop support for cluster runtime v13.x
        # db_args: Optional[Dict[str, Any]]
        # if args:
        #     keys = [f"arg{i}" for i in range(len(args))]
        #     # Replace position arguments (%s) with named arguments (:arg0, :arg1, ...)
        #     query = query % tuple(f":{key}" for key in keys)
        #     db_args = {}
        #     for key, db_arg in zip(keys, args):
        #         # Databricks connector doesn't accept pendulum objects
        #         if isinstance(db_arg, pendulum.DateTime):
        #             db_arg = to_py_datetime(db_arg)
        #         elif isinstance(db_arg, pendulum.Date):
        #             db_arg = to_py_date(db_arg)
        #         db_args[key] = db_arg
        # else:
        #     db_args = kwargs or None

        assert isinstance(query, str)
        db_args = args or kwargs or None
        with self._conn.cursor() as curr:
            curr.execute(query, db_args)
            yield DatabricksCursorImpl(curr)  # type: ignore[arg-type, abstract, unused-ignore]

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        if casefold:
            catalog = self.capabilities.casefold_identifier(self.credentials.catalog)
        else:
            catalog = self.credentials.catalog
        if quote:
            catalog = self.capabilities.escape_identifier(catalog)
        return catalog

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        if isinstance(ex, databricks_lib.ServerOperationError):
            if "TABLE_OR_VIEW_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            elif "SCHEMA_NOT_FOUND" in str(ex):
                return DatabaseUndefinedRelation(ex)
            elif "PARSE_SYNTAX_ERROR" in str(ex):
                return DatabaseTransientException(ex)
            elif "The protocol version of the Delta table has been changed" in str(ex):
                # concurrent write to tables - repeat
                raise DatabaseTransientException(ex)
            return DatabaseTerminalException(ex)
        elif isinstance(ex, databricks_lib.OperationalError):
            return DatabaseTransientException(ex)
        elif isinstance(ex, (databricks_lib.ProgrammingError, databricks_lib.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif isinstance(ex, databricks_lib.DatabaseError):
            return DatabaseTransientException(ex)
        else:
            return DatabaseTransientException(ex)

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, databricks_lib.DatabaseError)
