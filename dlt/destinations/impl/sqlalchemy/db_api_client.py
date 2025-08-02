from typing import Optional, Iterator, Any, Sequence, AnyStr, Union, Tuple, List, Dict, Set, cast
from contextlib import contextmanager
from functools import wraps
import inspect
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ResourceClosedError

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTerminalException,
    DatabaseTransientException,
    LoadClientNotConnected,
    DatabaseException,
)
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.typing import TFun

from dlt.destinations.typing import DBTransaction
from dlt.destinations.sql_client import SqlClientBase, raise_database_error
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials
from dlt.destinations.impl.sqlalchemy.alter_table import MigrationMaker
from dlt.destinations.sql_client import DBApiCursorImpl


class SqlaTransactionWrapper(DBTransaction):
    def __init__(self, sqla_transaction: sa.engine.Transaction) -> None:
        self.sqla_transaction = sqla_transaction

    def commit_transaction(self) -> None:
        if self.sqla_transaction.is_active:
            self.sqla_transaction.commit()

    def rollback_transaction(self) -> None:
        if self.sqla_transaction.is_active:
            self.sqla_transaction.rollback()


class SqlaDbApiCursor(DBApiCursorImpl):
    def __init__(self, curr: sa.engine.CursorResult) -> None:
        # Sqlalchemy CursorResult is *mostly* compatible with DB-API cursor
        self.native_cursor = curr  # type: ignore[assignment]
        curr.columns

        self._set_default_schema_columns()

    def _get_columns(self) -> List[str]:
        try:
            return list(cast(sa.engine.CursorResult, self.native_cursor).keys())
        except ResourceClosedError:
            # this happens if no rows are returned
            return []

    # @property
    # def description(self) -> Any:
    #     # Get the underlying driver's cursor description, this is mostly used in tests
    #     return self.native_cursor.cursor.description  # type: ignore[attr-defined]

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("execute not implemented")


class DbApiProps:
    # Only needed for some tests
    paramstyle = "named"


class SqlalchemyClient(SqlClientBase[Connection]):
    dbapi = DbApiProps  # type: ignore[assignment]
    migrations: Optional[MigrationMaker] = None  # lazy init as needed

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: SqlalchemyCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self.credentials = credentials
        self._current_connection: Optional[Connection] = None
        self._current_transaction: Optional[SqlaTransactionWrapper] = None
        self.metadata = sa.MetaData()
        # Keep a list of datasets already attached on the current connection
        self._sqlite_attached_datasets: Set[str] = set()

    @property
    def engine(self) -> sa.engine.Engine:
        return self.credentials.engine

    @property
    def dialect(self) -> sa.engine.interfaces.Dialect:
        return self.engine.dialect

    @property
    def dialect_name(self) -> str:
        return self.dialect.name

    def open_connection(self) -> Connection:
        if self._current_connection is None:
            self._current_connection = self.credentials.borrow_conn()
            if self.dialect_name == "sqlite":
                self._sqlite_reattach_dataset_if_exists(self.dataset_name)
        return self._current_connection

    def close_connection(self) -> None:
        # rollback any transactions
        try:
            if self._in_transaction():
                self.rollback_transaction()
        except Exception:
            pass
        finally:
            self._current_transaction = None
        # detach databases
        try:
            if self.dialect_name == "sqlite":
                for dataset_name in list(self._sqlite_attached_datasets):
                    self._sqlite_detach_dataset(dataset_name)
        except Exception:
            # close internal connection. that will detach all datasets
            self._current_connection.connection.close()

        try:
            if self._current_connection is not None:
                self.credentials.return_conn(self._current_connection)
        finally:
            self._current_connection = None

    @property
    def native_connection(self) -> Connection:
        return self._current_connection

    def _in_transaction(self) -> bool:
        return (
            self._current_transaction is not None
            and self._current_transaction.sqla_transaction.is_active
        )

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        trans = self._current_transaction = SqlaTransactionWrapper(self._current_connection.begin())
        try:
            yield trans
        except Exception:
            if self._in_transaction():
                self.rollback_transaction()
            raise
        else:
            if self._in_transaction():  # Transaction could be committed/rolled back before __exit__
                self.commit_transaction()
        finally:
            self._current_transaction = None

    @raise_database_error
    def commit_transaction(self) -> None:
        """Commits the current transaction."""
        self._current_transaction.commit_transaction()

    @raise_database_error
    def rollback_transaction(self) -> None:
        """Rolls back the current transaction."""
        self._current_transaction.rollback_transaction()

    @contextmanager
    @raise_database_error
    def _ensure_transaction(self) -> Iterator[DBTransaction]:
        """Context manager yielding either a new or the currently open transaction.
        New transaction will be committed/rolled back on exit.
        If the transaction is already open, finalization is handled by the top level context manager.
        """
        if self._in_transaction():
            yield self._current_transaction
            return
        with self.begin_transaction() as tx:
            yield tx

    def has_dataset(self) -> bool:
        with self._ensure_transaction():
            schema_names = self.engine.dialect.get_schema_names(self._current_connection)  # type: ignore[attr-defined]
        return self.dataset_name in schema_names

    def _sqlite_dataset_filename(self, dataset_name: str) -> str:
        current_file_path = Path(self.database_name)
        return str(
            current_file_path.parent
            / f"{current_file_path.stem}__{dataset_name}{current_file_path.suffix}"
        )

    def _sqlite_is_memory_db(self) -> bool:
        return self.database_name in (":memory:", "")

    def _sqlite_reattach_dataset_if_exists(self, dataset_name: str) -> None:
        """Re-attach previously created databases for a new sqlite connection"""
        if self._sqlite_is_memory_db():
            return
        new_db_fn = self._sqlite_dataset_filename(dataset_name)
        if Path(new_db_fn).exists():
            self._sqlite_create_dataset(dataset_name)

    def _sqlite_create_dataset(self, dataset_name: str) -> None:
        """Mimic multiple schemas in sqlite using ATTACH DATABASE to
        attach a new database file to the current connection.
        """
        if self._sqlite_is_memory_db():
            new_db_fn = ":memory:"
        else:
            new_db_fn = self._sqlite_dataset_filename(dataset_name)

            if dataset_name != "main":  # main is the current file, it is always attached
                statement = "ATTACH DATABASE :fn AS :name"
                self.execute_sql(statement, fn=new_db_fn, name=dataset_name)
            # WAL mode is applied to all currently attached databases
            self.execute_sql("PRAGMA journal_mode=WAL")
        self._sqlite_attached_datasets.add(dataset_name)

    def _sqlite_drop_dataset(self, dataset_name: str) -> None:
        """Drop a dataset in sqlite by detaching the database file
        attached to the current connection.
        """
        # Get a list of attached databases and filenames
        rows = self.execute_sql("PRAGMA database_list")
        dbs = {row[1]: row[2] for row in rows}  # db_name: filename
        if dataset_name != "main":  # main is the default database, it cannot be detached
            self._sqlite_detach_dataset(dataset_name)

        fn = dbs[dataset_name]
        if not fn:  # It's a memory database, nothing to do
            return
        # Delete the database file
        Path(fn).unlink()

    def _sqlite_detach_dataset(self, dataset_name: str) -> None:
        statement = "DETACH DATABASE :name"
        self.execute_sql(statement, name=dataset_name)
        self._sqlite_attached_datasets.discard(dataset_name)

    @contextmanager
    def with_alternative_dataset_name(
        self, dataset_name: str
    ) -> Iterator[SqlClientBase[Connection]]:
        with super().with_alternative_dataset_name(dataset_name):
            if self.dialect_name == "sqlite" and dataset_name not in self._sqlite_attached_datasets:
                if not self.native_connection:
                    # opening connection attaches dataset
                    with self:
                        pass
                else:
                    self._sqlite_reattach_dataset_if_exists(dataset_name)
            yield self

    def create_dataset(self) -> None:
        if self.dialect_name == "sqlite":
            return self._sqlite_create_dataset(self.dataset_name)
        self.execute_sql(sa.schema.CreateSchema(self.dataset_name))

    def drop_dataset(self) -> None:
        if self.dialect_name == "sqlite":
            return self._sqlite_drop_dataset(self.dataset_name)
        try:
            self.execute_sql(sa.schema.DropSchema(self.dataset_name, cascade=True))
        except DatabaseException:  # Try again in case cascade is not supported
            self.execute_sql(sa.schema.DropSchema(self.dataset_name))

    def truncate_tables(self, *tables: str) -> None:
        # TODO: alchemy doesn't have a construct for TRUNCATE TABLE
        for table in tables:
            tbl = sa.Table(table, self.metadata, schema=self.dataset_name, keep_existing=True)
            self.execute_sql(tbl.delete())

    def drop_tables(self, *tables: str) -> None:
        for table in tables:
            tbl = sa.Table(table, self.metadata, schema=self.dataset_name, keep_existing=True)
            self.execute_sql(sa.schema.DropTable(tbl, if_exists=True))

    def execute_sql(
        self, sql: Union[AnyStr, sa.sql.Executable], *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as cursor:
            if cast(sa.engine.CursorResult, cursor).returns_rows:
                return cursor.fetchall()
            return None

    @contextmanager
    def execute_query(
        self, query: Union[AnyStr, sa.sql.Executable], *args: Any, **kwargs: Any
    ) -> Iterator[DBApiCursor]:
        if args and kwargs:
            raise ValueError("Cannot use both positional and keyword arguments")
        if isinstance(query, str):
            if args:
                # Sqlalchemy text supports :named paramstyle for all dialects
                query, kwargs = self._to_named_paramstyle(query, args)  # type: ignore[assignment]
                args = (kwargs,)
            query = sa.text(query)
        if kwargs:
            # sqla2 takes either a dict or list of dicts
            args = (kwargs,)
        with self._ensure_transaction():
            cur = self._current_connection.execute(query, *args)  # type: ignore[call-overload]
            try:
                yield SqlaDbApiCursor(cur)
            finally:
                cur.close()

    def get_existing_table(self, table_name: str) -> Optional[sa.Table]:
        """Get a table object from metadata if it exists"""
        key = self.dataset_name + "." + table_name
        return self.metadata.tables.get(key)  # type: ignore[no-any-return]

    def create_table(self, table_obj: sa.Table) -> None:
        with self._ensure_transaction():
            table_obj.create(self._current_connection)

    def make_qualified_table_name_path(
        self, table_name: Optional[str], quote: bool = True, casefold: bool = True
    ) -> List[str]:
        path: List[str] = []
        # no catalog for sqlalchemy
        if catalog_name := self.catalog_name(quote=quote, casefold=casefold):
            path.append(catalog_name)

        dataset_name = self.dataset_name
        if self.dialect.requires_name_normalize and casefold:  # type: ignore[attr-defined]
            dataset_name = str(self.dialect.normalize_name(dataset_name))  # type: ignore[func-returns-value]
        if quote:
            dataset_name = self.dialect.identifier_preparer.quote_identifier(dataset_name)  # type: ignore[attr-defined]
        path.append(dataset_name)
        if table_name:
            if self.dialect.requires_name_normalize and casefold:  # type: ignore[attr-defined]
                table_name = str(self.dialect.normalize_name(table_name))  # type: ignore[func-returns-value]
            if quote:
                table_name = self.dialect.identifier_preparer.quote_identifier(table_name)  # type: ignore[attr-defined]
            path.append(table_name)
        return path

    def alter_table_add_columns(self, columns: Sequence[sa.Column]) -> None:
        if not columns:
            return
        if self.migrations is None:
            self.migrations = MigrationMaker(self.dialect)
        for column in columns:
            self.migrations.add_column(column.table.name, column, schema=self.dataset_name)
        statements = self.migrations.consume_statements()
        for statement in statements:
            self.execute_sql(statement)

    def escape_column_name(
        self, column_name: str, quote: bool = True, casefold: bool = True
    ) -> str:
        if self.dialect.requires_name_normalize and casefold:  # type: ignore[attr-defined]
            column_name = str(self.dialect.normalize_name(column_name))  # type: ignore[func-returns-value]
        if quote:
            return self.dialect.identifier_preparer.quote_identifier(column_name)  # type: ignore[attr-defined,no-any-return]
        return column_name

    def reflect_table(
        self,
        table_name: str,
        metadata: Optional[sa.MetaData] = None,
        include_columns: Optional[Sequence[str]] = None,
    ) -> Optional[sa.Table]:
        """Reflect a table from the database and return the Table object"""
        if metadata is None:
            metadata = self.metadata
        try:
            with self._ensure_transaction():
                table = sa.Table(
                    table_name,
                    metadata,
                    autoload_with=self._current_connection,
                    resolve_fks=False,
                    schema=self.dataset_name,
                    include_columns=include_columns,
                    extend_existing=True,
                )
                # assert table is not None
                return table
        except DatabaseUndefinedRelation:
            return None

    def compare_storage_table(self, table_name: str) -> Tuple[sa.Table, List[sa.Column], bool]:
        """Reflect the table from database and compare it with the version already in metadata.
        Returns a 3 part tuple:
        - The current version of the table in metadata
        - List of columns that are missing from the storage table (all columns if it doesn't exist in storage)
        - boolean indicating whether the table exists in storage
        """
        existing = self.get_existing_table(table_name)
        assert existing is not None, "Table must be present in metadata"
        all_columns = list(existing.columns)
        all_column_names = [c.name for c in all_columns]
        tmp_metadata = sa.MetaData()
        reflected = self.reflect_table(
            table_name, include_columns=all_column_names, metadata=tmp_metadata
        )
        if reflected is None:
            missing_columns = all_columns
        else:
            missing_columns = [c for c in all_columns if c.name not in reflected.columns]
        return existing, missing_columns, reflected is not None

    @staticmethod
    def _make_database_exception(e: Exception) -> Exception:
        if isinstance(e, sa.exc.NoSuchTableError):
            return DatabaseUndefinedRelation(e)
        msg = str(e).lower()
        if isinstance(e, (sa.exc.ProgrammingError, sa.exc.OperationalError)):
            patterns = [
                # MySQL / MariaDB
                r"unknown database",  # Missing schema
                r"doesn't exist",  # Missing table
                r"unknown table",  # Missing table
                # SQLite
                r"no such table",  # Missing table
                r"no such database",  # Missing table
                # PostgreSQL / Trino / Vertica / Exasol (database)
                r"does not exist",  # Missing schema, relation
                # r"does not exist",  # Missing table
                # MSSQL
                r"invalid object name",  # Missing schema or table
                # Oracle
                r"ora-00942: table or view does not exist",  # Missing schema or table
                # SAP HANA
                r"invalid schema name",  # Missing schema
                r"invalid table name",  # Missing table
                # DB2
                r"is an undefined name",  # SQL0204N... Missing schema or table
                # Apache Hive
                r"table not found",  # Missing table
                r"database does not exist",
                # Exasol
                r" not found",
            ]
            # entity not found
            for pat_ in patterns:
                if pat_ in msg:
                    return DatabaseUndefinedRelation(e)
            terminal_patterns = [
                "no such",
                "not found",
                "not exist",
                "unknown",
            ]
            for pat_ in terminal_patterns:
                if pat_ in msg:
                    return DatabaseTerminalException(e)
            return DatabaseTransientException(e)
        elif isinstance(e, sa.exc.IntegrityError):
            return DatabaseTerminalException(e)
        elif isinstance(e, sa.exc.SQLAlchemyError):
            return DatabaseTransientException(e)
        else:
            return e

    def _ensure_native_conn(self) -> None:
        if not self.native_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)
