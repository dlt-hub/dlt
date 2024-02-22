from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

import pyarrow

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.dremio import capabilities, pydremio
from dlt.destinations.impl.dremio.configuration import DremioCredentials
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame


class DremioCursorImpl(DBApiCursorImpl):
    native_cursor: pydremio.DremioCursor  # type: ignore[assignment]

    def df(self, chunk_size: int = None, **kwargs: Any) -> Optional[DataFrame]:
        if chunk_size is None:
            return self.native_cursor.fetch_arrow_table().to_pandas()
        return super().df(chunk_size=chunk_size, **kwargs)


class DremioSqlClient(SqlClientBase[pydremio.DremioConnection]):
    dbapi: ClassVar[DBApi] = pydremio
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, dataset_name: str, credentials: DremioCredentials) -> None:
        super().__init__(credentials.database, dataset_name)
        self._conn: Optional[pydremio.DremioConnection] = None
        self.credentials = credentials

    def open_connection(self) -> pydremio.DremioConnection:
        conn_params = self.credentials.to_connector_params()
        # set the timezone to UTC so when loading from file formats that do not have timezones
        # we get dlt expected UTC
        if "timezone" not in conn_params:
            conn_params["timezone"] = "UTC"
        self._conn = pydremio.connect(schema=self.fully_qualified_dataset_name(), **conn_params)
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        logger.warning(
            "Dremio does not support transactions! Each SQL statement is auto-committed separately."
        )
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        pass

    @raise_database_error
    def rollback_transaction(self) -> None:
        raise NotImplementedError("You cannot rollback Dremio SQL statements.")

    @property
    def native_connection(self) -> "pydremio.DremioConnection":
        return self._conn

    def drop_tables(self, *tables: str) -> None:
        # Tables are drop with `IF EXISTS`, but dremio raises when the schema doesn't exist.
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
        db_args = args if args else kwargs if kwargs else None
        with self._conn.cursor() as curr:
            curr.execute(query, db_args)
            yield DremioCursorImpl(curr)  # type: ignore[abstract]

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        if escape:
            return self.capabilities.escape_identifier(self.dataset_name)
        return self.dataset_name

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, pyarrow.lib.ArrowInvalid):
            msg = str(ex)
            if "not found" in msg:
                return DatabaseUndefinedRelation(ex)
            else:
                return DatabaseTerminalException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, pyarrow.lib.ArrowInvalid)
