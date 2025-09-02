from typing import Iterator, Any

import pytest

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

try:
    from tests.load.sources.sql_database.postgres_source import PostgresSourceDB
    from tests.load.sources.sql_database.mssql_source import MSSQLSourceDB
except ModuleNotFoundError:
    PostgresSourceDB = Any  # type: ignore
    MSSQLSourceDB = Any  # type: ignore


@pytest.fixture(autouse=True)
def dispose_engines():
    yield
    import gc

    # will collect and dispose all hanging engines
    gc.collect()


def _create_postgres_db(**kwargs) -> Iterator[PostgresSourceDB]:
    credentials = dlt.secrets.get(
        "destination.postgres.credentials", expected_type=ConnectionStringCredentials
    )

    db = PostgresSourceDB(credentials, **kwargs)
    db.create_schema()
    try:
        db.create_tables()
        db.insert_data()
        yield db
    finally:
        db.drop_schema()


def _create_mssql_db(**kwargs: Any) -> Iterator[MSSQLSourceDB]:
    #
    connection_url = (
        "mssql+pyodbc://sa:Strong%21Passw0rd@localhost:1433/master"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
    credentials = ConnectionStringCredentials(connection_url)
    db = MSSQLSourceDB(credentials)
    db.create_database()
    db.create_schema()
    try:
        nullable = kwargs.get("nullable", True)

        db.create_tables(nullable)
        db.generate_users()
        yield db
    finally:
        pass


@pytest.fixture(scope="function")
def mssql_db(request: pytest.FixtureRequest) -> Iterator[MSSQLSourceDB]:
    kwargs = getattr(request, "param", {})
    yield from _create_mssql_db(**kwargs)


@pytest.fixture(scope="package")
def postgres_db(request: pytest.FixtureRequest) -> Iterator[PostgresSourceDB]:
    # Without unsupported types so we can test full schema load with connector-x
    yield from _create_postgres_db(with_unsupported_types=False)


@pytest.fixture(scope="package")
def postgres_db_unsupported_types(
    request: pytest.FixtureRequest,
) -> Iterator[PostgresSourceDB]:
    yield from _create_postgres_db(with_unsupported_types=True)
