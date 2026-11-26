from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union

import pytest

pytest.importorskip("dlt.destinations.impl.fabric.sql_client", reason="mssql-python not installed")

import mssql_python
from mssql_python.exceptions import sqlstate_to_exception

from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.fabric.configuration import FabricCredentials
from dlt.destinations.impl.fabric.factory import fabric
from dlt.destinations.impl.fabric.sql_client import FabricSqlClient

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

MISSING_TABLE = sqlstate_to_exception("42S02", "Invalid object name 'ds.missing'.")
MISSING_SCHEMA = sqlstate_to_exception(
    "42000",
    "Cannot drop the schema 'ds', because it does not exist or you do not have permission.",
)
SYNTAX_ERROR = sqlstate_to_exception("42000", "Incorrect syntax near 'SELEC'.")
NO_TRANSACTION = sqlstate_to_exception(
    "42000",
    "An attempt to complete a transaction has failed. No corresponding transaction found.",
)


class FakeCursor:
    def __init__(
        self,
        execute_errors: Sequence[Optional[Exception]] = (),
        nextset_results: Sequence[Union[bool, Exception]] = (),
    ) -> None:
        self.description: Any = None
        self.executed: List[Tuple[Any, ...]] = []
        self.closed = False
        self._execute_errors = list(execute_errors)
        self._nextset_results = list(nextset_results)

    def execute(self, *args: Any) -> None:
        self.executed.append(args)
        error = self._execute_errors.pop(0) if self._execute_errors else None
        if error:
            raise error

    def nextset(self) -> bool:
        result = self._nextset_results.pop(0) if self._nextset_results else False
        if isinstance(result, Exception):
            raise result
        return result

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor, rollback_error: Exception = None) -> None:
        self.autocommit = True
        self.rollbacks = 0
        self._cursor = cursor
        self._rollback_error = rollback_error

    def cursor(self) -> FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rollbacks += 1
        if self._rollback_error:
            raise self._rollback_error


def _credentials() -> FabricCredentials:
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"
    return creds


def _client(conn: FakeConnection = None) -> FabricSqlClient:
    client = FabricSqlClient("ds", "ds_staging", _credentials(), fabric().capabilities())
    client._conn = conn  # type: ignore[assignment]
    return client


@pytest.mark.parametrize(
    "ex,expected",
    [
        pytest.param(MISSING_TABLE, DatabaseUndefinedRelation, id="missing-table"),
        pytest.param(MISSING_SCHEMA, DatabaseUndefinedRelation, id="missing-schema"),
        pytest.param(SYNTAX_ERROR, DatabaseTransientException, id="syntax-error"),
        pytest.param(
            sqlstate_to_exception("07002", "COUNT field incorrect or syntax error"),
            DatabaseTransientException,
            id="wrong-parameter-count",
        ),
        pytest.param(
            sqlstate_to_exception("08S01", "Communication link failure"),
            DatabaseTransientException,
            id="connection-lost",
        ),
        pytest.param(
            sqlstate_to_exception("HYT00", "Login timeout expired"),
            DatabaseTransientException,
            id="timeout",
        ),
        pytest.param(
            sqlstate_to_exception("42S22", "Invalid column name '_unk_'."),
            DatabaseTerminalException,
            id="missing-column",
        ),
        pytest.param(
            sqlstate_to_exception("23000", "Cannot insert the value NULL into column 'id'"),
            DatabaseTerminalException,
            id="not-null-violation",
        ),
        pytest.param(
            sqlstate_to_exception("22003", "Arithmetic overflow error"),
            DatabaseTerminalException,
            id="numeric-overflow",
        ),
        pytest.param(TypeError("Parameter style mismatch"), DatabaseTerminalException, id="other"),
    ],
)
def test_make_database_exception(ex: Exception, expected: Type[Exception]) -> None:
    assert type(FabricSqlClient._make_database_exception(ex)) is expected
    assert FabricSqlClient.is_dbapi_exception(ex) is isinstance(ex, mssql_python.Error)


@pytest.mark.parametrize(
    "args,kwargs,expected",
    [
        pytest.param((), {}, ("SELECT %s",), id="no-parameters"),
        pytest.param(("a", 1), {}, ("SELECT ?", "a", 1), id="positional"),
        pytest.param((), {"name": "a"}, ("SELECT %s", {"name": "a"}), id="named"),
    ],
)
def test_execute_query_parameters(
    args: Tuple[Any, ...], kwargs: Dict[str, Any], expected: Tuple[Any, ...]
) -> None:
    cursor = FakeCursor()
    client = _client(FakeConnection(cursor))
    with client.execute_query("SELECT %s", *args, **kwargs):
        pass
    assert cursor.executed == [expected]
    assert cursor.closed


def test_execute_query_raises_errors_of_later_statements() -> None:
    cursor = FakeCursor(nextset_results=[True, MISSING_TABLE, True])
    conn = FakeConnection(cursor)
    with pytest.raises(DatabaseUndefinedRelation):
        with _client(conn).execute_query("INSERT 1; INSERT 2; INSERT 3"):
            pass
    assert conn.rollbacks == 1
    assert cursor.closed
    # remaining result sets were consumed
    assert cursor.nextset() is False


def test_execute_query_error_on_execute() -> None:
    # errors raised by cleanup must not hide the original one
    cursor = FakeCursor(execute_errors=[SYNTAX_ERROR], nextset_results=[MISSING_TABLE])
    conn = FakeConnection(cursor, rollback_error=NO_TRANSACTION)
    with pytest.raises(DatabaseTransientException) as ex:
        _client(conn).execute_sql("SELEC 1")
    assert ex.value.dbapi_exception is SYNTAX_ERROR
    assert conn.rollbacks == 1
    assert cursor.closed


def test_rollback_transaction() -> None:
    # nothing to rollback is not an error
    conn = FakeConnection(FakeCursor(), rollback_error=NO_TRANSACTION)
    conn.autocommit = False
    _client(conn).rollback_transaction()
    assert conn.autocommit is True

    conn = FakeConnection(FakeCursor(), rollback_error=SYNTAX_ERROR)
    conn.autocommit = False
    with pytest.raises(DatabaseTransientException):
        _client(conn).rollback_transaction()
    assert conn.autocommit is True


def test_drop_tables_skips_missing_tables() -> None:
    cursor = FakeCursor(execute_errors=[MISSING_TABLE, None])
    client = _client(FakeConnection(cursor))
    client.drop_tables("missing", "existing")
    assert cursor.executed == [
        (f"DROP TABLE {client.make_qualified_table_name('missing')}",),
        (f"DROP TABLE {client.make_qualified_table_name('existing')}",),
    ]


def test_open_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_args: Dict[str, Any] = {}

    def _connect(connection_str: str, **kwargs: Any) -> FakeConnection:
        connect_args.update(kwargs, connection_str=connection_str)
        return FakeConnection(FakeCursor())

    monkeypatch.setattr(mssql_python, "connect", _connect)
    client = _client()
    client.open_connection()
    assert connect_args == {
        "connection_str": client.credentials.to_odbc_dsn(),
        "autocommit": True,
        "timeout": 15,
        "native_uuid": False,
    }
