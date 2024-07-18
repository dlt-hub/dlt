import datetime  # noqa: I251
from clickhouse_driver import dbapi as clickhouse_dbapi  # type: ignore[import-untyped]
from contextlib import contextmanager
from typing import (
    Iterator,
    AnyStr,
    Any,
    List,
    Optional,
    Sequence,
    ClassVar,
    Literal,
    Tuple,
    cast,
)

import clickhouse_driver
import clickhouse_driver.errors  # type: ignore[import-untyped]
from clickhouse_driver.dbapi import OperationalError  # type: ignore[import-untyped]
from clickhouse_driver.dbapi.extras import DictCursor  # type: ignore[import-untyped]
from pendulum import DateTime  # noqa: I251

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.typing import DictStrAny
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTransientException,
    DatabaseTerminalException,
)
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.clickhouse.typing import (
    TTableEngineType,
    TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR,
)
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBTransaction, DBApi
from dlt.destinations.utils import _convert_to_old_pyformat


TDeployment = Literal["ClickHouseOSS", "ClickHouseCloud"]
TRANSACTIONS_UNSUPPORTED_WARNING_MESSAGE = (
    "ClickHouse does not support transactions! Each statement is auto-committed separately."
)


class ClickHouseDBApiCursorImpl(DBApiCursorImpl):
    native_cursor: DictCursor


class ClickHouseSqlClient(
    SqlClientBase[clickhouse_driver.dbapi.connection.Connection], DBTransaction
):
    dbapi: ClassVar[DBApi] = clickhouse_dbapi

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: ClickHouseCredentials,
        capabilities: DestinationCapabilitiesContext,
        config: ClickHouseClientConfiguration,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self._conn: clickhouse_driver.dbapi.connection = None
        self.credentials = credentials
        self.database_name = credentials.database
        self.config = config

    def has_dataset(self) -> bool:
        # we do not need to normalize dataset_sentinel_table_name.
        sentinel_table = self.config.dataset_sentinel_table_name
        return sentinel_table in [
            t.split(self.config.dataset_table_separator)[1] for t in self._list_tables()
        ]

    def open_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        self._conn = clickhouse_driver.connect(dsn=self.credentials.to_native_representation())
        return self._conn

    @raise_open_connection_error
    def close_connection(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        yield self

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> clickhouse_driver.dbapi.connection.Connection:
        return self._conn

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            return None if curr.description is None else curr.fetchall()

    def create_dataset(self) -> None:
        # We create a sentinel table which defines whether we consider the dataset created.
        sentinel_table_name = self.make_qualified_table_name(
            self.config.dataset_sentinel_table_name
        )
        sentinel_table_type = cast(TTableEngineType, self.config.table_engine_type)
        self.execute_sql(f"""
            CREATE TABLE {sentinel_table_name}
            (_dlt_id String NOT NULL PRIMARY KEY)
            ENGINE={TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR.get(sentinel_table_type)}
            COMMENT 'internal dlt sentinel table'""")

    def drop_dataset(self) -> None:
        # always try to drop the sentinel table.
        sentinel_table_name = self.make_qualified_table_name(
            self.config.dataset_sentinel_table_name
        )
        # drop a sentinel table
        self.execute_sql(f"DROP TABLE {sentinel_table_name} SYNC")

        # Since ClickHouse doesn't have schemas, we need to drop all tables in our virtual schema,
        # or collection of tables, that has the `dataset_name` as a prefix.
        to_drop_results = [
            f"{self.catalog_name()}.{self.capabilities.escape_identifier(table)}"
            for table in self._list_tables()
        ]
        for table in to_drop_results:
            # The "DROP TABLE" clause is discarded if we allow clickhouse_driver to handle parameter substitution.
            # This is because the driver incorrectly substitutes the entire query string, causing the "DROP TABLE" keyword to be omitted.
            # To resolve this, we are forced to provide the full query string here.
            self.execute_sql(f"DROP TABLE {table} SYNC")

    def drop_tables(self, *tables: str) -> None:
        """Drops a set of tables if they exist"""
        if not tables:
            return
        statements = [
            f"DROP TABLE IF EXISTS {self.make_qualified_table_name(table)} SYNC;"
            for table in tables
        ]
        self.execute_many(statements)

    def _list_tables(self) -> List[str]:
        catalog_name, table_name = self.make_qualified_table_name_path("%", escape=False)
        rows = self.execute_sql(
            """
            SELECT name
            FROM system.tables
            WHERE database = %s
            AND name LIKE %s
            """,
            catalog_name,
            table_name,
        )
        return [row[0] for row in rows]

    @staticmethod
    def _sanitise_dbargs(db_args: DictStrAny) -> DictStrAny:
        """For ClickHouse OSS, the DBapi driver doesn't parse datetime types.
        We remove timezone specifications in this case."""
        for key, value in db_args.items():
            if isinstance(value, (DateTime, datetime.datetime)):
                db_args[key] = str(value.replace(microsecond=0, tzinfo=None))
        return db_args

    @contextmanager
    @raise_database_error
    def execute_query(
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> Iterator[ClickHouseDBApiCursorImpl]:
        assert isinstance(query, str), "Query must be a string."

        db_args: DictStrAny = kwargs.copy()

        if args:
            query, db_args = _convert_to_old_pyformat(query, args, OperationalError)
            db_args.update(kwargs)

        db_args = self._sanitise_dbargs(db_args)

        with self._conn.cursor() as cursor:
            for query_line in query.split(";"):
                if query_line := query_line.strip():
                    try:
                        cursor.execute(query_line, db_args)
                    except KeyError as e:
                        raise DatabaseTransientException(OperationalError()) from e

            yield ClickHouseDBApiCursorImpl(cursor)  # type: ignore[abstract]

    def catalog_name(self, escape: bool = True) -> Optional[str]:
        database_name = self.capabilities.casefold_identifier(self.database_name)
        if escape:
            database_name = self.capabilities.escape_identifier(database_name)
        return database_name

    def make_qualified_table_name_path(
        self, table_name: Optional[str], escape: bool = True
    ) -> List[str]:
        # get catalog and dataset
        path = super().make_qualified_table_name_path(None, escape=escape)
        if table_name:
            # table name combines dataset name and table name
            table_name = self.capabilities.casefold_identifier(
                f"{self.dataset_name}{self.config.dataset_table_separator}{table_name}"
            )
            if escape:
                table_name = self.capabilities.escape_identifier(table_name)
            # we have only two path components
            path[1] = table_name
        return path

    def _get_information_schema_components(self, *tables: str) -> Tuple[str, str, List[str]]:
        components = super()._get_information_schema_components(*tables)
        # clickhouse has a catalogue and no schema but uses catalogue as a schema to query the information schema 🤷
        # so we must disable catalogue search. also note that table name is prefixed with logical "dataset_name"
        return (None, components[0], components[2])

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, clickhouse_driver.dbapi.errors.OperationalError):
            if "Code: 57." in str(ex) or "Code: 82." in str(ex) or "Code: 47." in str(ex):
                return DatabaseTerminalException(ex)
            elif "Code: 60." in str(ex) or "Code: 81." in str(ex):
                return DatabaseUndefinedRelation(ex)
            else:
                return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.OperationalError,
                clickhouse_driver.dbapi.errors.InternalError,
            ),
        ):
            return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                clickhouse_driver.dbapi.errors.DataError,
                clickhouse_driver.dbapi.errors.ProgrammingError,
                clickhouse_driver.dbapi.errors.IntegrityError,
            ),
        ):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, clickhouse_driver.dbapi.Error)
