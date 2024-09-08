from typing import Iterator, Any

import pytest

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

try:
    from tests.load.sources.sql_database.sql_source import SQLAlchemySourceDB
except ModuleNotFoundError:
    SQLAlchemySourceDB = Any  # type: ignore


def _create_db(**kwargs) -> Iterator[SQLAlchemySourceDB]:
    # TODO: parametrize the fixture so it takes the credentials for all destinations
    credentials = dlt.secrets.get(
        "destination.postgres.credentials", expected_type=ConnectionStringCredentials
    )

    db = SQLAlchemySourceDB(credentials, **kwargs)
    db.create_schema()
    try:
        db.create_tables()
        db.insert_data()
        yield db
    finally:
        db.drop_schema()


@pytest.fixture(scope="package")
def sql_source_db(request: pytest.FixtureRequest) -> Iterator[SQLAlchemySourceDB]:
    # Without unsupported types so we can test full schema load with connector-x
    yield from _create_db(with_unsupported_types=False)


@pytest.fixture(scope="package")
def sql_source_db_unsupported_types(
    request: pytest.FixtureRequest,
) -> Iterator[SQLAlchemySourceDB]:
    yield from _create_db(with_unsupported_types=True)
