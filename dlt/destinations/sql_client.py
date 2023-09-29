from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import wraps
import inspect
from types import TracebackType
from typing import Any, ClassVar, ContextManager, Generic, Iterator, Optional, Sequence, Tuple, Type, AnyStr, List

from dlt.common.typing import TFun
from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.exceptions import DestinationConnectionError, LoadClientNotConnected
from dlt.destinations.typing import DBApi, TNativeConn, DBApiCursor, DataFrame, DBTransaction


class SqlClientBase(ABC, Generic[TNativeConn]):

    dbapi: ClassVar[DBApi] = None
    capabilities: ClassVar[DestinationCapabilitiesContext] = None

    def __init__(self, database_name: str, dataset_name: str) -> None:
        if not dataset_name:
            raise ValueError(dataset_name)
        self.dataset_name = dataset_name
        self.database_name = database_name

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

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
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
        db_params = self.fully_qualified_dataset_name(escape=False).split(".", 2)
        if len(db_params) == 2:
            query += " catalog_name = %s AND "
        query += "schema_name = %s"
        rows = self.execute_sql(query, *db_params)
        return len(rows) > 0

    def create_dataset(self) -> None:
        self.execute_sql("CREATE SCHEMA %s" % self.fully_qualified_dataset_name())

    def drop_dataset(self) -> None:
        self.execute_sql("DROP SCHEMA %s CASCADE;" % self.fully_qualified_dataset_name())

    def truncate_tables(self, *tables: str) -> None:
        statements = [self._truncate_table_sql(self.make_qualified_table_name(t)) for t in tables]
        self.execute_fragments(statements)

    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        statements = [f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)};" for table in tables]
        self.execute_fragments(statements)

    @abstractmethod
    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        pass

    @abstractmethod
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> ContextManager[DBApiCursor]:
        pass

    def execute_fragments(self, fragments: Sequence[AnyStr], *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        """Executes several SQL fragments as efficiently as possible to prevent data copying. Default implementation just joins the strings and executes them together.
        """
        return self.execute_sql("".join(fragments), *args, **kwargs)  # type: ignore

    @abstractmethod
    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        pass

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        if escape:
            table_name = self.capabilities.escape_identifier(table_name)
        return f"{self.fully_qualified_dataset_name(escape=escape)}.{table_name}"

    def escape_column_name(self, column_name: str, escape: bool = True) -> str:
        if escape:
            return self.capabilities.escape_identifier(column_name)
        return column_name

    @contextmanager
    def with_alternative_dataset_name(self, dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]:
        """Sets the `dataset_name` as the default dataset during the lifetime of the context. Does not modify any search paths in the existing connection."""
        current_dataset_name = self.dataset_name
        try:
            self.dataset_name = dataset_name
            yield self
        finally:
            # restore previous dataset name
            self.dataset_name = current_dataset_name

    def with_staging_dataset(self, staging: bool = False)-> ContextManager["SqlClientBase[TNativeConn]"]:
        dataset_name = self.dataset_name
        if staging:
            dataset_name = SqlClientBase.make_staging_dataset_name(dataset_name)
        return self.with_alternative_dataset_name(dataset_name)

    def _ensure_native_conn(self) -> None:
        if not self.native_connection:
            raise LoadClientNotConnected(type(self).__name__ , self.dataset_name)

    @staticmethod
    @abstractmethod
    def _make_database_exception(ex: Exception) -> Exception:
        pass

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        # crude way to detect dbapi DatabaseError: there's no common set of exceptions, each module must reimplement
        mro = type.mro(type(ex))
        return any(t.__name__ in ("DatabaseError", "DataError") for t in mro)

    @staticmethod
    def make_staging_dataset_name(dataset_name: str) -> str:
        return dataset_name + "_staging"

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
        from dlt.helpers.pandas_helper import _wrap_result

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
