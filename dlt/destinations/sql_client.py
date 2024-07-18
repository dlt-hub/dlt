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
    TypedDict,
)

from dlt.common.typing import TFun
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import concat_strings_with_limit

from dlt.destinations.exceptions import (
    DestinationConnectionError,
    LoadClientNotConnected,
)
from dlt.destinations.typing import DBApi, TNativeConn, DBApiCursor, DataFrame, DBTransaction


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
        if not dataset_name:
            raise ValueError(dataset_name)
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

    def __enter__(self) -> "SqlClientBase[TNativeConn]":
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
        self.execute_sql("DROP SCHEMA %s CASCADE;" % self.fully_qualified_dataset_name())

    def truncate_tables(self, *tables: str) -> None:
        statements = [self._truncate_table_sql(self.make_qualified_table_name(t)) for t in tables]
        self.execute_many(statements)

    def drop_tables(self, *tables: str) -> None:
        """Drops a set of tables if they exist"""
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)};" for table in tables
        ]
        self.execute_many(statements)

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
                list(statements), "\n", self.capabilities.max_query_length // 2
            ):
                ret.append(self.execute_sql(sql_fragment, *args, **kwargs))
        else:
            for statement in statements:
                result = self.execute_sql(statement, *args, **kwargs)
                if result is not None:
                    ret.append(result)
        return ret

    def catalog_name(self, escape: bool = True) -> Optional[str]:
        # default is no catalogue component of the name, which typically means that
        # connection is scoped to a current database
        return None

    def fully_qualified_dataset_name(self, escape: bool = True, staging: bool = False) -> str:
        if staging:
            with self.with_staging_dataset():
                path = self.make_qualified_table_name_path(None, escape=escape)
        else:
            path = self.make_qualified_table_name_path(None, escape=escape)
        return ".".join(path)

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        return ".".join(self.make_qualified_table_name_path(table_name, escape=escape))

    def make_qualified_table_name_path(
        self, table_name: Optional[str], escape: bool = True
    ) -> List[str]:
        """Returns a list with path components leading from catalog to table_name.
        Used to construct fully qualified names. `table_name` is optional.
        """
        path: List[str] = []
        if catalog_name := self.catalog_name(escape=escape):
            path.append(catalog_name)
        dataset_name = self.capabilities.casefold_identifier(self.dataset_name)
        if escape:
            dataset_name = self.capabilities.escape_identifier(dataset_name)
        path.append(dataset_name)
        if table_name:
            table_name = self.capabilities.casefold_identifier(table_name)
            if escape:
                table_name = self.capabilities.escape_identifier(table_name)
            path.append(table_name)
        return path

    def get_qualified_table_names(self, table_name: str, escape: bool = True) -> Tuple[str, str]:
        """Returns qualified names for table and corresponding staging table as tuple."""
        with self.with_staging_dataset():
            staging_table_name = self.make_qualified_table_name(table_name, escape)
        return self.make_qualified_table_name(table_name, escape), staging_table_name

    def escape_column_name(self, column_name: str, escape: bool = True) -> str:
        column_name = self.capabilities.casefold_identifier(column_name)
        if escape:
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
        return self.with_alternative_dataset_name(self.staging_dataset_name)

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
        schema_path = self.make_qualified_table_name_path(None, escape=False)
        return (
            self.catalog_name(escape=False),
            schema_path[-1],
            [self.make_qualified_table_name_path(table, escape=False)[-1] for table in tables],
        )

    #
    # generate sql statements
    #
    def _truncate_table_sql(self, qualified_table_name: str) -> str:
        if self.capabilities.supports_truncate_command:
            return f"TRUNCATE TABLE {qualified_table_name};"
        else:
            return f"DELETE FROM {qualified_table_name} WHERE 1=1;"


class DBApiCursorImpl(DBApiCursor):
    """A DBApi Cursor wrapper with dataframes reading functionality"""

    def __init__(self, curr: DBApiCursor) -> None:
        self.native_cursor = curr

        # wire protocol methods
        self.execute = curr.execute  # type: ignore
        self.fetchall = curr.fetchall  # type: ignore
        self.fetchmany = curr.fetchmany  # type: ignore
        self.fetchone = curr.fetchone  # type: ignore

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native_cursor, name)

    def _get_columns(self) -> List[str]:
        return [c[0] for c in self.native_cursor.description]

    def df(self, chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]:
        """Fetches results as data frame in full or in specified chunks.

        May use native pandas/arrow reader if available. Depending on
        the native implementation chunk size may vary.
        """
        from dlt.common.libs.pandas_sql import _wrap_result

        columns = self._get_columns()
        if chunk_size is None:
            return _wrap_result(self.native_cursor.fetchall(), columns, **kwargs)
        else:
            df = _wrap_result(self.native_cursor.fetchmany(chunk_size), columns, **kwargs)
            # if no rows return None
            if df.shape[0] == 0:
                return None
            else:
                return df


def raise_database_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap_gen(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            self._ensure_native_conn()
            return (yield from f(self, *args, **kwargs))
        except Exception as ex:
            raise self._make_database_exception(ex)

    @wraps(f)
    def _wrap(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            self._ensure_native_conn()
            return f(self, *args, **kwargs)
        except Exception as ex:
            raise self._make_database_exception(ex)

    if inspect.isgeneratorfunction(f):
        return _wrap_gen  # type: ignore
    else:
        return _wrap  # type: ignore


def raise_open_connection_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap(self: SqlClientBase[Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except Exception as ex:
            raise DestinationConnectionError(type(self).__name__, self.dataset_name, str(ex), ex)

    return _wrap  # type: ignore
