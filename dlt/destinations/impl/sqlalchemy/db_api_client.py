from typing import (
    Optional,
    Iterator,
    Any,
    Sequence,
    AnyStr,
    Union,
    Tuple,
    List,
    Dict,
    Set,
)
from contextlib import contextmanager
from functools import wraps
import inspect
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ResourceClosedError

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import PreparedTableSchema
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTerminalException,
    DatabaseTransientException,
    LoadClientNotConnected,
    DatabaseException,
)
from dlt.common.destination.reference import DBApiCursor
from dlt.destinations.typing import DBTransaction
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials
from dlt.destinations.impl.sqlalchemy.alter_table import MigrationMaker
from dlt.common.typing import TFun
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


def raise_database_error(f: TFun) -> TFun:
    @wraps(f)
    def _wrap_gen(self: "SqlalchemyClient", *args: Any, **kwargs: Any) -> Any:
        try:
            return (yield from f(self, *args, **kwargs))
        except Exception as e:
            raise self._make_database_exception(e) from e

    @wraps(f)
    def _wrap(self: "SqlalchemyClient", *args: Any, **kwargs: Any) -> Any:
        try:
            return f(self, *args, **kwargs)
        except Exception as e:
            raise self._make_database_exception(e) from e

    if inspect.isgeneratorfunction(f):
        return _wrap_gen  # type: ignore[return-value]
    return _wrap  # type: ignore[return-value]


class SqlaDbApiCursor(DBApiCursorImpl):
    def __init__(self, curr: sa.engine.CursorResult) -> None:
        # Sqlalchemy CursorResult is *mostly* compatible with DB-API cursor
        self.native_cursor = curr  # type: ignore[assignment]
        curr.columns

        self.fetchall = curr.fetchall  # type: ignore[assignment]
        self.fetchone = curr.fetchone  # type: ignore[assignment]
        self.fetchmany = curr.fetchmany  # type: ignore[assignment]

        self.set_default_schema_columns()

    def _get_columns(self) -> List[str]:
        try:
            return list(self.native_cursor.keys())  # type: ignore[attr-defined]
        except ResourceClosedError:
            # this happens if now rows are returned
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
    external_engine: bool = False
    dbapi = DbApiProps  # type: ignore[assignment]
    migrations: Optional[MigrationMaker] = None  # lazy init as needed
    _engine: Optional[sa.engine.Engine] = None

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: SqlalchemyCredentials,
        capabilities: DestinationCapabilitiesContext,
        engine_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(credentials.database, dataset_name, staging_dataset_name, capabilities)
        self.credentials = credentials

        self.engine_args = engine_args or {}

        if credentials.engine:
            self._engine = credentials.engine
            self.external_engine = True
        else:
            # Default to nullpool because we don't use connection pooling
            self.engine_args.setdefault("poolclass", sa.pool.NullPool)

        self._current_connection: Optional[Connection] = None
        self._current_transaction: Optional[SqlaTransactionWrapper] = None
        self.metadata = sa.MetaData()
        # Keep a list of datasets already attached on the current connection
        self._sqlite_attached_datasets: Set[str] = set()

    @property
    def engine(self) -> sa.engine.Engine:
        # Create engine lazily
        if self._engine is not None:
            return self._engine
        self._engine = sa.create_engine(
            self.credentials.to_url().render_as_string(hide_password=False), **self.engine_args
        )
        return self._engine

    @property
    def dialect(self) -> sa.engine.interfaces.Dialect:
        return self.engine.dialect

    @property
    def dialect_name(self) -> str:
        return self.dialect.name

    def open_connection(self) -> Connection:
        if self._current_connection is None:
            self._current_connection = self.engine.connect()
            if self.dialect_name == "sqlite":
                self._sqlite_reattach_dataset_if_exists(self.dataset_name)
        return self._current_connection

    def close_connection(self) -> None:
        if not self.external_engine:
            try:
                if self._current_connection is not None:
                    self._current_connection.close()
                self.engine.dispose()
            finally:
                self._sqlite_attached_datasets.clear()
                self._current_connection = None
                self._current_transaction = None

    @property
    def native_connection(self) -> Connection:
        if not self._current_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)
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

    def commit_transaction(self) -> None:
        """Commits the current transaction."""
        self._current_transaction.commit_transaction()

    def rollback_transaction(self) -> None:
        """Rolls back the current transaction."""
        self._current_transaction.rollback_transaction()

    @contextmanager
    def _transaction(self) -> Iterator[DBTransaction]:
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
        with self._transaction():
            schema_names = self.engine.dialect.get_schema_names(self._current_connection)  # type: ignore[attr-defined]
        return self.dataset_name in schema_names

    def _sqlite_dataset_filename(self, dataset_name: str) -> str:
        db_name = self.engine.url.database
        current_file_path = Path(db_name)
        return str(
            current_file_path.parent
            / f"{current_file_path.stem}__{dataset_name}{current_file_path.suffix}"
        )

    def _sqlite_is_memory_db(self) -> bool:
        return self.engine.url.database == ":memory:"

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
            statement = "DETACH DATABASE :name"
            self.execute_sql(statement, name=dataset_name)
            self._sqlite_attached_datasets.discard(dataset_name)

        fn = dbs[dataset_name]
        if not fn:  # It's a memory database, nothing to do
            return
        # Delete the database file
        Path(fn).unlink()

    @contextmanager
    def with_alternative_dataset_name(
        self, dataset_name: str
    ) -> Iterator[SqlClientBase[Connection]]:
        with super().with_alternative_dataset_name(dataset_name):
            if self.dialect_name == "sqlite" and dataset_name not in self._sqlite_attached_datasets:
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
            if cursor.returns_rows:  # type: ignore[attr-defined]
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
        with self._transaction():
            yield SqlaDbApiCursor(self._current_connection.execute(query, *args))  # type: ignore[call-overload, abstract]

    def get_existing_table(self, table_name: str) -> Optional[sa.Table]:
        """Get a table object from metadata if it exists"""
        key = self.dataset_name + "." + table_name
        return self.metadata.tables.get(key)  # type: ignore[no-any-return]

    def create_table(self, table_obj: sa.Table) -> None:
        with self._transaction():
            table_obj.create(self._current_connection)

    def _make_qualified_table_name(self, table: sa.Table, escape: bool = True) -> str:
        if escape:
            return self.dialect.identifier_preparer.format_table(table)  # type: ignore[attr-defined,no-any-return]
        return table.fullname  # type: ignore[no-any-return]

    def make_qualified_table_name(self, table_name: str, escape: bool = True) -> str:
        tbl = self.get_existing_table(table_name)
        if tbl is None:
            tmp_metadata = sa.MetaData()
            tbl = sa.Table(table_name, tmp_metadata, schema=self.dataset_name)
        return self._make_qualified_table_name(tbl, escape)

    def fully_qualified_dataset_name(self, escape: bool = True, staging: bool = False) -> str:
        if staging:
            dataset_name = self.staging_dataset_name
        else:
            dataset_name = self.dataset_name
        return self.dialect.identifier_preparer.format_schema(dataset_name)  # type: ignore[attr-defined, no-any-return]

    def alter_table_add_columns(self, columns: Sequence[sa.Column]) -> None:
        if not columns:
            return
        if self.migrations is None:
            self.migrations = MigrationMaker(self.dialect)
        for column in columns:
            self.migrations.add_column(column.table.name, column, self.dataset_name)
        statements = self.migrations.consume_statements()
        for statement in statements:
            self.execute_sql(statement)

    def escape_column_name(self, column_name: str, escape: bool = True) -> str:
        if self.dialect.requires_name_normalize:  # type: ignore[attr-defined]
            column_name = self.dialect.normalize_name(column_name)  # type: ignore[func-returns-value]
        if escape:
            return self.dialect.identifier_preparer.format_column(sa.Column(column_name))  # type: ignore[attr-defined,no-any-return]
        return column_name

    def compile_column_def(self, column: sa.Column) -> str:
        """Compile a column definition including type for ADD COLUMN clause"""
        return str(sa.schema.CreateColumn(column).compile(self.engine))

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
            with self._transaction():
                return sa.Table(
                    table_name,
                    metadata,
                    autoload_with=self._current_connection,
                    schema=self.dataset_name,
                    include_columns=include_columns,
                    extend_existing=True,
                )
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
            if "exist" in msg:  # TODO: Hack
                return DatabaseUndefinedRelation(e)
            elif "unknown table" in msg:
                return DatabaseUndefinedRelation(e)
            elif "unknown database" in msg:
                return DatabaseUndefinedRelation(e)
            elif "no such table" in msg:  # sqlite # TODO: Hack
                return DatabaseUndefinedRelation(e)
            elif "no such database" in msg:  # sqlite # TODO: Hack
                return DatabaseUndefinedRelation(e)
            elif "syntax" in msg:
                return DatabaseTransientException(e)
            elif isinstance(e, (sa.exc.OperationalError, sa.exc.IntegrityError)):
                return DatabaseTerminalException(e)
            return DatabaseTransientException(e)
        elif isinstance(e, sa.exc.SQLAlchemyError):
            return DatabaseTransientException(e)
        else:
            return e
        # return DatabaseTerminalException(e)

    def _ensure_native_conn(self) -> None:
        if not self.native_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)
