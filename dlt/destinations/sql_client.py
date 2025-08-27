from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import wraps
import inspect
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    ContextManager,
    Dict,
    Generic,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    AnyStr,
    List,
    Generator,
    cast,
)

from dlt.common.typing import TFun, TypedDict, Self
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import concat_strings_with_limit
from dlt.common.destination.client import JobClientBase

from dlt.destinations.exceptions import (
    DestinationConnectionError,
    LoadClientNotConnected,
)
from dlt.destinations.typing import (
    DBApi,
    TNativeConn,
    DataFrame,
    DBTransaction,
    ArrowTable,
)
from dlt.common.destination.dataset import DBApiCursor


class TJobQueryTags(TypedDict):
    """Applied to sql client when a job using it starts. Using to tag queries"""

    source: str
    resource: str
    table: str
    load_id: str
    pipeline_name: str


class SqlClientBase(ABC, Generic[TNativeConn]):
    dbapi: ClassVar[DBApi] = None

    database_name: Optional[str]
    """Database or catalog name, optional"""
    dataset_name: str
    """Normalized dataset name"""
    staging_dataset_name: str
    """Normalized staging dataset name"""
    capabilities: DestinationCapabilitiesContext
    """Instance of adjusted destination capabilities"""

    def __init__(
        self,
        database_name: str,
        dataset_name: str,
        staging_dataset_name: str,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.dataset_name = dataset_name
        self.staging_dataset_name = staging_dataset_name
        self.database_name = database_name
        self.capabilities = capabilities
        self._query_tags: TJobQueryTags = None

    @abstractmethod
    def open_connection(self) -> TNativeConn:
        pass

    @abstractmethod
    def close_connection(self) -> None:
        pass

    @abstractmethod
    def begin_transaction(self) -> ContextManager[DBTransaction]:
        pass

    def __getattr__(self, name: str) -> Any:
        # pass unresolved attrs to native connections
        if not self.native_connection:
            raise AttributeError(name)
        return getattr(self.native_connection, name)

    def __enter__(self) -> Self:
        self.open_connection()
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        self.close_connection()

    @property
    @abstractmethod
    def native_connection(self) -> TNativeConn:
        pass

    def has_dataset(self) -> bool:
        query = """
SELECT 1
    FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE """
        catalog_name, schema_name, _ = self._get_information_schema_components()
        db_params: List[str] = []
        if catalog_name is not None:
            query += " catalog_name = %s AND "
            db_params.append(catalog_name)
        db_params.append(schema_name)
        query += "schema_name = %s"
        rows = self.execute_sql(query, *db_params)
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql("CREATE SCHEMA %s" % self.fully_qualified_dataset_name())

    def drop_dataset(self) -> None:
        self.execute_sql("DROP SCHEMA %s CASCADE" % self.fully_qualified_dataset_name())

    def truncate_tables(self, *tables: str) -> None:
        statements = [self._truncate_table_sql(self.make_qualified_table_name(t)) for t in tables]
        self.execute_many(statements)

    def drop_tables(self, *tables: str) -> None:
        """Drops a set of tables if they exist"""
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)}" for table in tables
        ]
        self.execute_many(statements)

    def _to_named_paramstyle(self, query: str, args: Sequence[Any]) -> Tuple[str, Dict[str, Any]]:
        """Convert a query from "format" ( %s ) paramstyle to "named" ( :param_name ) paramstyle.
        The %s are replaced with :arg0, :arg1, ... and the arguments are returned as a dictionary.

        Args:
            query: SQL query with %s placeholders
            args: arguments to be passed to the query

        Returns:
            Tuple of the new query and a dictionary of named arguments
        """
        keys = [f"arg{i}" for i in range(len(args))]
        # Replace position arguments (%s) with named arguments (:arg0, :arg1, ...)
        query = query % tuple(f":{key}" for key in keys)
        db_args = {key: db_arg for key, db_arg in zip(keys, args)}
        return query, db_args

    @abstractmethod
    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        pass

    @abstractmethod
    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> ContextManager[DBApiCursor]:
        pass

    def execute_fragments(
        self, fragments: Sequence[AnyStr], *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        """Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together."""
        return self.execute_sql("".join(fragments), *args, **kwargs)  # type: ignore

    def execute_many(
        self, statements: Sequence[str], *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        """Executes multiple SQL statements as efficiently as possible. When client supports multiple statements in a single query
        they are executed together in as few database calls as possible.
        """
        ret = []
        if self.capabilities.supports_multiple_statements:
            for sql_fragment in concat_strings_with_limit(
                list(statements), ";\n", self.capabilities.max_query_length // 2
            ):
                ret.append(self.execute_sql(sql_fragment + ";", *args, **kwargs))
        else:
            for statement in statements:
                result = self.execute_sql(statement, *args, **kwargs)
                if result is not None:
                    ret.append(result)
        return ret

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        # default is no catalogue component of the name, which typically means that
        # connection is scoped to a current database
        return None

    def fully_qualified_dataset_name(
        self, quote: bool = True, staging: bool = False, casefold: bool = True
    ) -> str:
        if staging:
            with self.with_staging_dataset():
                path = self.make_qualified_table_name_path(None, quote=quote, casefold=casefold)
        else:
            path = self.make_qualified_table_name_path(None, quote=quote, casefold=casefold)
        return ".".join(path)

    def make_qualified_table_name(
        self, table_name: str, quote: bool = True, casefold: bool = True
    ) -> str:
        return ".".join(
            self.make_qualified_table_name_path(table_name, quote=quote, casefold=casefold)
        )

    def make_qualified_table_name_path(
        self, table_name: Optional[str], quote: bool = True, casefold: bool = True
    ) -> List[str]:
        """Returns a list with path components leading from catalog to table_name.
        Used to construct fully qualified names. `table_name` is optional.
        """
        path: List[str] = []
        if catalog_name := self.catalog_name(quote=quote, casefold=casefold):
            path.append(catalog_name)
        dataset_name = self.dataset_name
        if casefold:
            dataset_name = self.capabilities.casefold_identifier(self.dataset_name)
        if quote:
            dataset_name = self.capabilities.escape_identifier(dataset_name)
        path.append(dataset_name)
        if table_name:
            if casefold:
                table_name = self.capabilities.casefold_identifier(table_name)
            if quote:
                table_name = self.capabilities.escape_identifier(table_name)
            path.append(table_name)
        return path

    def get_qualified_table_names(
        self, table_name: str, quote: bool = True, casefold: bool = True
    ) -> Tuple[str, str]:
        """Returns qualified names for table and corresponding staging table as tuple."""
        with self.with_staging_dataset():
            staging_table_name = self.make_qualified_table_name(
                table_name, quote=quote, casefold=casefold
            )
        return (
            self.make_qualified_table_name(table_name, quote=quote, casefold=casefold),
            staging_table_name,
        )

    def escape_column_name(
        self, column_name: str, quote: bool = True, casefold: bool = True
    ) -> str:
        if casefold:
            column_name = self.capabilities.casefold_identifier(column_name)
        if quote:
            return self.capabilities.escape_identifier(column_name)
        return column_name

    @contextmanager
    def with_alternative_dataset_name(
        self, dataset_name: str
    ) -> Iterator["SqlClientBase[TNativeConn]"]:
        """Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection."""
        current_dataset_name = self.dataset_name
        try:
            self.dataset_name = dataset_name
            yield self
        finally:
            # restore previous dataset name
            self.dataset_name = current_dataset_name

    def with_staging_dataset(self) -> ContextManager["SqlClientBase[TNativeConn]"]:
        """Temporarily switch sql client to staging dataset name"""
        return self.with_alternative_dataset_name(self.staging_dataset_name)

    @property
    def is_staging_dataset_active(self) -> bool:
        """Checks if staging dataset is currently active"""
        return self.dataset_name == self.staging_dataset_name

    def set_query_tags(self, tags: TJobQueryTags) -> None:
        """Sets current schema (source), resource, load_id and table name when a job starts"""
        self._query_tags = tags

    def _ensure_native_conn(self) -> None:
        if not self.native_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)

    @staticmethod
    @abstractmethod
    def _make_database_exception(ex: Exception) -> Exception:
        pass

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        # crude way to detect dbapi DatabaseError: there's no common set of exceptions, each module must reimplement
        mro = type.mro(type(ex))
        return any(t.__name__ in ("DatabaseError", "DataError") for t in mro)

    def _get_information_schema_components(self, *tables: str) -> Tuple[str, str, List[str]]:
        """Gets catalog name, schema name and name of the tables in format that can be directly
        used to query INFORMATION_SCHEMA. catalog name is optional: in that case None is
        returned in the first element of the tuple.
        """
        schema_path = self.make_qualified_table_name_path(None, quote=False)
        return (
            self.catalog_name(quote=False),
            schema_path[-1],
            [self.make_qualified_table_name_path(table, quote=False)[-1] for table in tables],
        )

    #
    # generate sql statements
    #
    def _truncate_table_sql(self, qualified_table_name: str) -> str:
        if self.capabilities.supports_truncate_command:
            return f"TRUNCATE TABLE {qualified_table_name}"
        else:
            return f"DELETE FROM {qualified_table_name} WHERE 1=1"

    def _limit_clause_sql(self, limit: int) -> Tuple[str, str]:
        return "", f"LIMIT {limit}"


class WithSqlClient(ABC):
    @property
    @abstractmethod
    def sql_client(self) -> SqlClientBase[TNativeConn]:
        pass

    # Sqlclient type so we know when WithTableScanners is implemented
    @property
    @abstractmethod
    def sql_client_class(self) -> Type[SqlClientBase[TNativeConn]]:
        pass


class DBApiCursorImpl(DBApiCursor):
    """A DBApi Cursor wrapper with dataframes reading functionality"""

    def __init__(self, curr: DBApiCursor) -> None:
        self.native_cursor = curr
        self._set_default_schema_columns()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native_cursor, name)

    def _get_columns(self) -> List[str]:
        if self.native_cursor.description:
            return [c[0] for c in self.native_cursor.description]
        return []

    # wire protocol methods
    def execute(self, *args: Any, **kwargs: Any) -> None:
        self.native_cursor.execute(*args, **kwargs)

    def fetchall(self, *args: Any, **kwargs: Any) -> List[Tuple[Any, ...]]:
        return self.native_cursor.fetchall(*args, **kwargs)

    def fetchmany(self, *args: Any, **kwargs: Any) -> List[Tuple[Any, ...]]:
        return self.native_cursor.fetchmany(*args, **kwargs)

    def fetchone(self, *args: Any, **kwargs: Any) -> Tuple[Any, ...]:
        return self.native_cursor.fetchone(*args, **kwargs)

    def close(self, *args: Any, **kwargs: Any) -> None:
        self.native_cursor.close(*args, **kwargs)

    def _set_default_schema_columns(self) -> None:
        self.columns_schema = cast(
            TTableSchemaColumns, {c: {"name": c, "nullable": True} for c in self._get_columns()}
        )

    def df(self, chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]:
        """Fetches results as data frame in full or in specified chunks.

        May use native pandas/arrow reader if available. Depending on
        the native implementation chunk size may vary.
        """
        try:
            return next(self.iter_df(chunk_size=chunk_size))
        except StopIteration:
            return None

    def arrow(self, chunk_size: int = None, **kwargs: Any) -> Optional[ArrowTable]:
        """Fetches results as data frame in full or in specified chunks.

        May use native pandas/arrow reader if available. Depending on
        the native implementation chunk size may vary.
        """
        try:
            return next(self.iter_arrow(chunk_size=chunk_size))
        except StopIteration:
            return None

    def iter_fetch(self, chunk_size: int) -> Generator[List[Tuple[Any, ...]], Any, Any]:
        while True:
            if not (result := self.fetchmany(chunk_size)):
                return
            yield result

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        """Default implementation converts arrow to df"""
        from dlt.common.libs.pandas import pandas as pd

        for table in self.iter_arrow(chunk_size=chunk_size):
            # NOTE: we go via arrow table, types are created for arrow is columns are known
            # https://github.com/apache/arrow/issues/38644 for reference on types_mapper
            yield table.to_pandas()

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        """Default implementation converts query result to arrow table"""
        from dlt.common.libs.pyarrow import row_tuples_to_arrow
        from dlt.common.configuration.container import Container

        # get capabilities of possibly currently active pipeline
        caps = (
            Container().get(DestinationCapabilitiesContext)
            or DestinationCapabilitiesContext.generic_capabilities()
        )

        # note that columns_schema is inferred from the cursor and is not normalized
        if not chunk_size:
            result = self.fetchall()
            yield row_tuples_to_arrow(result, caps, self.columns_schema, tz="UTC")
            return

        for result in self.iter_fetch(chunk_size=chunk_size):
            yield row_tuples_to_arrow(result, caps, self.columns_schema, tz="UTC")


def raise_database_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap_gen(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return (yield from f(self, *args, **kwargs))
        except Exception as ex:
            db_ex = self._make_database_exception(ex)
            raise db_ex.with_traceback(ex.__traceback__) from ex

    @wraps(f)
    def _wrap(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except Exception as ex:
            db_ex = self._make_database_exception(ex)
            raise db_ex.with_traceback(ex.__traceback__) from ex

    if inspect.isgeneratorfunction(f):
        return _wrap_gen  # type: ignore[return-value]
    return _wrap  # type: ignore[return-value]


def raise_open_connection_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except Exception as ex:
            raise DestinationConnectionError(type(self).__name__, self.dataset_name, str(ex), ex)

    return _wrap  # type: ignore
