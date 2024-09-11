from typing import (
    Optional,
    Iterator,
    Any,
    Sequence,
    ContextManager,
    AnyStr,
    Union,
    Tuple,
    List,
    Dict,
)
from contextlib import contextmanager
from functools import wraps
import inspect
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.exceptions import (
    DatabaseUndefinedRelation,
    DatabaseTerminalException,
    DatabaseTransientException,
    LoadClientNotConnected,
    DatabaseException,
)
from dlt.destinations.typing import DBTransaction, DBApiCursor
from dlt.destinations.sql_client import SqlClientBase, DBApiCursorImpl
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials
from dlt.common.typing import TFun


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

        self.fetchall = curr.fetchall  # type: ignore[method-assign]
        self.fetchone = curr.fetchone  # type: ignore[method-assign]
        self.fetchmany = curr.fetchmany  # type: ignore[method-assign]

    def _get_columns(self) -> List[str]:
        return list(self.native_cursor.keys())  # type: ignore[attr-defined]

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
    dialect: sa.engine.interfaces.Dialect
    dialect_name: str
    dbapi = DbApiProps  # type: ignore[assignment]

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

        if credentials.engine:
            self.engine = credentials.engine
            self.external_engine = True
        else:
            self.engine = sa.create_engine(
                credentials.to_url().render_as_string(hide_password=False), **(engine_args or {})
            )

        self._current_connection: Optional[Connection] = None
        self._current_transaction: Optional[SqlaTransactionWrapper] = None
        self.metadata = sa.MetaData()
        self.dialect = self.engine.dialect
        self.dialect_name = self.dialect.name  # type: ignore[attr-defined]

    def open_connection(self) -> Connection:
        if self._current_connection is None:
            self._current_connection = self.engine.connect()
        return self._current_connection

    def close_connection(self) -> None:
        if not self.external_engine:
            try:
                self.engine.dispose()
            finally:
                self._current_connection = None
                self._current_transaction = None

    @property
    def native_connection(self) -> Connection:
        if not self._current_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)
        return self._current_connection

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        if self._current_transaction is not None:
            raise DatabaseTerminalException("Transaction already started")
        trans = self._current_transaction = SqlaTransactionWrapper(self._current_connection.begin())
        try:
            yield trans
        except Exception:
            self.rollback_transaction()
            raise
        else:
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
        if self._current_transaction is not None:
            yield self._current_transaction
            return
        with self.begin_transaction() as tx:
            yield tx

    def has_dataset(self) -> bool:
        schema_names = self.engine.dialect.get_schema_names(self._current_connection)
        return self.dataset_name in schema_names

    def _sqlite_create_dataset(self, dataset_name: str) -> None:
        """Mimic multiple schemas in sqlite using ATTACH DATABASE to
        attach a new database file to the current connection.
        """
        db_name = self.engine.url.database
        if db_name == ":memory:":
            new_db_fn = ":memory:"
        else:
            current_file_path = Path(db_name)
            # New filename e.g. ./results.db -> ./results__new_dataset_name.db
            new_db_fn = str(
                current_file_path.parent
                / f"{current_file_path.stem}__{dataset_name}{current_file_path.suffix}"
            )

        statement = "ATTACH DATABASE :fn AS :name"
        self.execute(statement, fn=new_db_fn, name=dataset_name)

    def _sqlite_drop_dataset(self, dataset_name: str) -> None:
        """Drop a dataset in sqlite by detaching the database file
        attached to the current connection.
        """
        # Get a list of attached databases and filenames
        rows = self.execute_sql("PRAGMA database_list")
        dbs = {row[1]: row[2] for row in rows}  # db_name: filename
        if dataset_name not in dbs:
            return

        statement = "DETACH DATABASE :name"
        self.execute(statement, name=dataset_name)

        fn = dbs[dataset_name]
        if not fn:  # It's a memory database, nothing to do
            return
        # Delete the database file
        Path(fn).unlink()

    def create_dataset(self) -> None:
        if self.dialect_name == "sqlite":
            return self._sqlite_create_dataset(self.dataset_name)
        self.execute_sql(sa.schema.CreateSchema(self.dataset_name))

    def drop_dataset(self) -> None:
        if self.dialect_name == "sqlite":
            return self._sqlite_drop_dataset(self.dataset_name)
        try:
            self.execute_sql(sa.schema.DropSchema(self.dataset_name, cascade=True))
        except DatabaseTransientException as e:
            if isinstance(e.__cause__, sa.exc.ProgrammingError):
                # May not support CASCADE
                self.execute_sql(sa.schema.DropSchema(self.dataset_name))
            else:
                raise

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
            if cursor.returns_rows:
                return cursor.fetchall()
            return None

    @contextmanager
    def execute_query(
        self, query: Union[AnyStr, sa.sql.Executable], *args: Any, **kwargs: Any
    ) -> Iterator[DBApiCursor]:
        if isinstance(query, str):
            if args:
                # Sqlalchemy text supports :named paramstyle for all dialects
                query, kwargs = self._to_named_paramstyle(query, args)  # type: ignore[assignment]
                args = ()
            query = sa.text(query)
        with self._transaction():
            yield SqlaDbApiCursor(self._current_connection.execute(query, *args, **kwargs))  # type: ignore[abstract]

    def get_existing_table(self, table_name: str) -> Optional[sa.Table]:
        """Get a table object from metadata if it exists"""
        key = self.dataset_name + "." + table_name
        return self.metadata.tables.get(key)  # type: ignore[no-any-return]

    def create_table(self, table_obj: sa.Table) -> None:
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

    def alter_table_add_column(self, column: sa.Column) -> None:
        """Execute an ALTER TABLE ... ADD COLUMN ... statement for the given column.
        The column must be fully defined and attached to a table.
        """
        # TODO: May need capability to override ALTER TABLE statement for different dialects
        alter_tmpl = "ALTER TABLE {table} ADD COLUMN {column};"
        statement = alter_tmpl.format(
            table=self._make_qualified_table_name(self._make_qualified_table_name(column.table)),
            column=self.compile_column_def(column),
        )
        self.execute_sql(statement)

    def escape_column_name(self, column_name: str, escape: bool = True) -> str:
        if self.dialect.requires_name_normalize:
            column_name = self.dialect.normalize_name(column_name)
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
            return sa.Table(
                table_name,
                metadata,
                autoload_with=self._current_connection,
                schema=self.dataset_name,
                include_columns=include_columns,
                extend_existing=True,
            )
        except sa.exc.NoSuchTableError:
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
            elif "no such" in msg:  # sqlite # TODO: Hack
                return DatabaseUndefinedRelation(e)
            elif "unknown table" in msg:
                return DatabaseUndefinedRelation(e)
            elif "unknown database" in msg:
                return DatabaseUndefinedRelation(e)
            elif isinstance(e, (sa.exc.OperationalError, sa.exc.IntegrityError)):
                raise DatabaseTerminalException(e)
            return DatabaseTransientException(e)
        elif isinstance(e, sa.exc.SQLAlchemyError):
            return DatabaseTransientException(e)
        else:
            return e
        # return DatabaseTerminalException(e)

    def _ensure_native_conn(self) -> None:
        if not self.native_connection:
            raise LoadClientNotConnected(type(self).__name__, self.dataset_name)
