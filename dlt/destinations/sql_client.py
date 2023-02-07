from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import wraps
import inspect
from types import TracebackType
from typing import Any, ClassVar, ContextManager, Generic, Iterator, Optional, Sequence, Type, AnyStr

from dlt.common.typing import TFun
from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.exceptions import DestinationConnectionError, LoadClientNotConnected
from dlt.destinations.typing import DBApi, TNativeConn, DBApiCursor, DBTransaction


class SqlClientBase(ABC, Generic[TNativeConn]):

    dbapi: ClassVar[DBApi] = None
    capabilities: ClassVar[DestinationCapabilitiesContext] = None

    def __init__(self, dataset_name: str) -> None:
        if not dataset_name:
            raise ValueError(dataset_name)
        self.dataset_name = dataset_name

    @abstractmethod
    def open_connection(self) -> None:
        pass

    @abstractmethod
    def close_connection(self) -> None:
        pass

    @abstractmethod
    def begin_transaction(self) -> ContextManager[DBTransaction]:
        pass

    def __enter__(self) -> "SqlClientBase[TNativeConn]":
        self.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.close_connection()

    @property
    @abstractmethod
    def native_connection(self) -> TNativeConn:
        pass

    @abstractmethod
    def has_dataset(self) -> bool:
        pass

    @abstractmethod
    def create_dataset(self) -> None:
        pass

    @abstractmethod
    def drop_dataset(self) -> None:
        pass

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

    @contextmanager
    def with_alternative_dataset_name(self, dataset_name: str) -> Iterator["SqlClientBase[TNativeConn]"]:
        current_dataset_name = self.dataset_name
        try:
            self.dataset_name = dataset_name
            yield self
        finally:
            # restore previous dataset name
            self.dataset_name = current_dataset_name

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
