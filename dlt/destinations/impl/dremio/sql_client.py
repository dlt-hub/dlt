from contextlib import contextmanager, suppress
from typing import Any, AnyStr, ClassVar, Iterator, Optional, Sequence

import pyarrow

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseUndefinedRelation,
    DatabaseTransientException,
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
        db_kwargs = self.credentials.db_kwargs()
        self._conn = pydremio.connect(uri=str(self.credentials.to_url()), db_kwargs=db_kwargs)
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
            try:
                curr.execute(query, db_args)  # type: ignore
            except pydremio.MalformedQueryError as ex:
                raise DatabaseTransientException(ex)
            yield DremioCursorImpl(curr)  # type: ignore

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        datasource_name = self.credentials.data_source
        database_name = self.credentials.database
        dataset_name = self.dataset_name
        if escape:
            datasource_name = self.capabilities.escape_identifier(datasource_name)
            database_name = self.capabilities.escape_identifier(database_name)
            dataset_name = self.capabilities.escape_identifier(dataset_name)
        if self.credentials.flatten:
            return f"{datasource_name}.{database_name}"
        else:
            return f"{datasource_name}.{database_name}.{dataset_name}"

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        if self.credentials.flatten:
            table_name = f"{self.dataset_name}__{table_name}"
        if escape:
            table_name = self.capabilities.escape_identifier(table_name)
        return f"{self.fully_qualified_dataset_name(escape=escape)}.{table_name}"

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, pyarrow.lib.ArrowInvalid):
            msg = str(ex)
            if "not found" in msg or "does not exist" in msg:
                return DatabaseUndefinedRelation(ex)
            else:
                return DatabaseTerminalException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, (pyarrow.lib.ArrowInvalid, pydremio.MalformedQueryError))

    def create_dataset(self) -> None:
        logger.info("Dremio does not implement create_dataset")

    def drop_dataset(self) -> None:
        logger.info("Dremio does not implement drop_dataset")

    def has_dataset(self) -> bool:
        if self.credentials.flatten:
            query = """
            SELECT 1
            FROM INFORMATION_SCHEMA."TABLES"
            WHERE TABLE_CATALOG = 'DREMIO' AND TABLE_SCHEMA = %s and STARTS_WITH(TABLE_NAME, %s)
            """
            db_params = [self.fully_qualified_dataset_name(escape=False), self.dataset_name + "__"]
        else:
            query = """
            SELECT 1
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE catalog_name = 'DREMIO' AND schema_name = %s
            """
            db_params = [self.fully_qualified_dataset_name(escape=False)]
        rows = self.execute_sql(query, *db_params)
        return len(rows) > 0
