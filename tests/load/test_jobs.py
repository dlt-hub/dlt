import pytest

from dlt.common import pendulum
from dlt.common.destination.client import RunnableLoadJob
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.destinations.job_impl import FinalizedLoadJob
from dlt.destinations.impl.sqlalchemy.load_jobs import build_mysql_adbc_dsn
from dlt.destinations.impl.mssql.configuration import escape_mssql_odbc_value, build_odbc_dsn


def test_instantiate_job() -> None:
    file_name = "table.1234.0.jsonl"
    file_path = "/path/" + file_name

    class SomeJob(RunnableLoadJob):
        def run(self) -> None:
            pass

    j = SomeJob(file_path)
    assert j._file_name == file_name
    assert j._file_path == file_path

    # providing only a filename is not allowed
    with pytest.raises(AssertionError):
        SomeJob(file_name)


def test_runnable_job_results() -> None:
    file_path = "/table.1234.0.jsonl"

    class MockClient:
        def prepare_load_job_execution(self, j: RunnableLoadJob):
            pass

    class SuccessfulJob(RunnableLoadJob):
        def run(self) -> None:
            5 + 5

    j: RunnableLoadJob = SuccessfulJob(file_path)
    assert j.state() == "ready"
    metrics = j.metrics()
    assert metrics.table_name == "table"
    assert metrics.state == "ready"
    assert metrics.started_at is not None
    assert metrics.finished_at is None
    assert metrics.retry_count == 0

    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "completed"
    metrics_2 = j.metrics()
    assert metrics_2.state == "completed"
    assert metrics.started_at == metrics_2.started_at
    assert metrics_2.finished_at is not None

    class RandomExceptionJob(RunnableLoadJob):
        def run(self) -> None:
            raise Exception("Oh no!")

    j = RandomExceptionJob(file_path)
    assert j.state() == "ready"
    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "retry"
    assert j.failed_message() == "Oh no!"
    assert isinstance(j.exception(), Exception)
    metrics_3 = j.metrics()
    assert metrics_3.state == "retry"
    assert metrics_3.started_at is not None
    assert metrics_3.finished_at is None
    # must change file name to increase retry count
    assert metrics_3.retry_count == 0

    class TerminalJob(RunnableLoadJob):
        def run(self) -> None:
            raise DestinationTerminalException("Oh no!")

    j = TerminalJob(file_path)
    assert j.state() == "ready"
    j.run_managed(MockClient(), None)  # type: ignore
    assert j.state() == "failed"
    assert j.failed_message() == "Oh no!"
    assert isinstance(j.exception(), Exception)
    metrics_4 = j.metrics()
    assert metrics_4.state == "failed"
    assert metrics_4.started_at is not None
    assert metrics_4.finished_at is not None
    assert metrics_4.retry_count == 0


def test_finalized_load_job() -> None:
    file_name = "table.1234.1.jsonl"
    file_path = "/path/" + file_name
    j = FinalizedLoadJob(file_path)
    assert j.state() == "completed"
    assert not j.failed_message()
    assert j.exception() is None
    assert isinstance(j._started_at, pendulum.DateTime)
    assert isinstance(j._finished_at, pendulum.DateTime)

    j = FinalizedLoadJob(
        file_path, status="failed", failed_message="oh no!", exception=RuntimeError()
    )
    assert j.state() == "failed"
    assert j.failed_message() == "oh no!"
    assert isinstance(j.exception(), RuntimeError)
    # start and finish dates will be automatically set for terminal states
    assert isinstance(j._started_at, pendulum.DateTime)
    assert isinstance(j._finished_at, pendulum.DateTime)
    metrics = j.metrics()
    assert metrics.table_name == "table"
    assert metrics.finished_at == j._finished_at
    assert metrics.started_at == j._started_at
    assert metrics.state == "failed"
    assert metrics.retry_count == 1

    j = FinalizedLoadJob(file_path, status="retry")
    assert isinstance(j._started_at, pendulum.DateTime)
    assert j._finished_at is None

    # explicit start and finish
    started_at = pendulum.now().subtract(days=-1)
    finished_at = pendulum.now().subtract(days=1)
    j = FinalizedLoadJob(file_path, status="retry", started_at=started_at, finished_at=finished_at)
    assert isinstance(j._started_at, pendulum.DateTime)
    assert j._finished_at is finished_at
    assert j._started_at is started_at

    # only actionable / terminal states are allowed
    with pytest.raises(AssertionError):
        FinalizedLoadJob(file_path, status="ready")


def test_build_mysql_adbc_dsn_basic() -> None:
    """Test basic DSN construction with simple inputs."""
    # minimal DSN
    dsn = build_mysql_adbc_dsn(database="mydb")
    assert dsn == "tcp(localhost:3306)/mydb"

    # with username only
    dsn = build_mysql_adbc_dsn(username="user", database="mydb")
    assert dsn == "user:@tcp(localhost:3306)/mydb"

    # with username and password
    dsn = build_mysql_adbc_dsn(username="user", password="pass", database="mydb")
    assert dsn == "user:pass@tcp(localhost:3306)/mydb"

    # with custom host and port
    dsn = build_mysql_adbc_dsn(
        username="user", password="pass", host="dbserver.example.com", port=3307, database="mydb"
    )
    assert dsn == "user:pass@tcp(dbserver.example.com:3307)/mydb"

    # query params
    dsn = build_mysql_adbc_dsn(
        username="user",
        password="pass",
        database="mydb",
        params={"loc": "Europe/Paris", "parseTime": "true"},
    )
    # Check that params are properly encoded
    assert dsn.startswith("user:pass@tcp(localhost:3306)/mydb?")
    assert "loc=Europe%2FParis" in dsn
    assert "parseTime=true" in dsn

    # all None
    dsn = build_mysql_adbc_dsn()
    assert dsn == "tcp(localhost:3306)/"

    # empty database
    dsn = build_mysql_adbc_dsn(database="")
    assert dsn == "tcp(localhost:3306)/"


def test_build_mysql_adbc_dsn_database_encoding() -> None:
    """Test that database names with special characters are properly URL-encoded."""
    # database with slash - should be encoded
    dsn = build_mysql_adbc_dsn(database="db/with/slashes and spaces")
    assert "db%2Fwith%2Fslashes+and+spaces" in dsn


def test_build_mysql_adbc_dsn_password_not_encoded() -> None:
    """Test that password is NOT URL-encoded (per go-sql-driver behavior)."""
    # Password with special characters - should NOT be encoded
    dsn = build_mysql_adbc_dsn(username="user", password="p@ss:word/123", database="mydb")
    # The password should appear as-is in the DSN
    assert "user:p@ss:word/123@" in dsn


def test_escape_mssql_odbc_value_basic() -> None:
    """Test basic values that don't need escaping."""
    # simple values pass through unchanged
    assert escape_mssql_odbc_value("localhost") == "localhost"
    assert escape_mssql_odbc_value("mypassword") == "mypassword"
    assert escape_mssql_odbc_value("MyDatabase") == "MyDatabase"
    assert escape_mssql_odbc_value("true") == "true"
    assert escape_mssql_odbc_value("1433") == "1433"


def test_escape_mssql_odbc_value_semicolon() -> None:
    """Test values containing semicolons are properly braced.

    Based on go-mssqldb ODBC parser (msdsn/conn_str.go):
    - Values containing `;` must be wrapped in `{}`
    """
    # semicolon in password
    assert escape_mssql_odbc_value("pass;word") == "{pass;word}"
    assert escape_mssql_odbc_value("a;b;c") == "{a;b;c}"
    # leading/trailing semicolons
    assert escape_mssql_odbc_value(";password") == "{;password}"
    assert escape_mssql_odbc_value("password;") == "{password;}"


def test_escape_mssql_odbc_value_closing_brace() -> None:
    """Test values containing closing braces are escaped.

    Based on go-mssqldb ODBC parser (msdsn/conn_str.go):
    - `}` inside braces must be doubled to `}}`
    """
    # single closing brace
    assert escape_mssql_odbc_value("pass}word") == "{pass}}word}"
    # multiple closing braces
    assert escape_mssql_odbc_value("a}b}c") == "{a}}b}}c}"
    # closing brace with semicolon
    assert escape_mssql_odbc_value("pass;wo}rd") == "{pass;wo}}rd}"


def test_escape_mssql_odbc_value_opening_brace() -> None:
    """Test values containing opening braces.

    Opening braces `{` don't need special escaping when inside braced values.
    """
    # opening brace alone doesn't trigger escaping
    assert escape_mssql_odbc_value("pass{word") == "pass{word"
    # but with semicolon, it gets braced
    assert escape_mssql_odbc_value("pass{;word") == "{pass{;word}"


def test_build_odbc_dsn_basic() -> None:
    """Test basic MSSQL DSN construction."""
    dsn = build_odbc_dsn({"SERVER": "localhost", "DATABASE": "mydb"})
    assert "SERVER=localhost" in dsn
    assert "DATABASE=mydb" in dsn
    assert dsn == "SERVER=localhost;DATABASE=mydb"


def test_build_odbc_dsn_with_credentials() -> None:
    """Test MSSQL DSN with username and password."""
    dsn = build_odbc_dsn(
        {
            "SERVER": "localhost",
            "DATABASE": "mydb",
            "UID": "sa",
            "PWD": "MyPassword123",
        }
    )
    assert "SERVER=localhost" in dsn
    assert "UID=sa" in dsn
    assert "PWD=MyPassword123" in dsn


def test_build_odbc_dsn_password_with_semicolon() -> None:
    """Test MSSQL DSN with password containing semicolons."""
    dsn = build_odbc_dsn(
        {
            "SERVER": "localhost",
            "PWD": "pass;word",
        }
    )
    assert "PWD={pass;word}" in dsn


def test_build_odbc_dsn_password_with_brace() -> None:
    """Test MSSQL DSN with password containing braces."""
    dsn = build_odbc_dsn(
        {
            "SERVER": "localhost",
            "PWD": "pass}word",
        }
    )
    assert "PWD={pass}}word}" in dsn


def test_build_odbc_dsn_skips_none() -> None:
    """Test MSSQL DSN skips None values."""
    dsn = build_odbc_dsn(
        {
            "SERVER": "localhost",
            "DATABASE": None,
            "PWD": "password",
        }
    )
    assert "SERVER=localhost" in dsn
    assert "PWD=password" in dsn
    assert "DATABASE" not in dsn
