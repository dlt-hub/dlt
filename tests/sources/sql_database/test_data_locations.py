from typing import Any, cast

import pytest
import sqlalchemy as sa

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.sources.sql_database.helpers import (
    TSqlDatabaseDataLocation,
    record_table_input,
)


def test_sql_database_location_hides_password() -> None:
    """A location is never allowed to carry credentials"""
    # parsing a connection string does not import the dbapi driver, creating an engine would
    credentials = ConnectionStringCredentials(
        "postgresql+psycopg2://loader:top_secret@example.com:5432/dlt_data"
    )

    resource = dlt.resource([{"id": 1}], name="orders")
    record_table_input(resource, credentials, "public", "orders")

    location = cast(TSqlDatabaseDataLocation, resource.inputs[0])
    # the dbapi driver is not part of the identity, the backend is
    assert location["location"] == "postgresql://example.com:5432"
    assert location["database"] == "dlt_data"
    assert location["db_schema"] == "public"
    assert "top_secret" not in str(location)
    assert "loader" not in str(location)


def test_sql_database_location_from_engine() -> None:
    """An externally provided engine carries no credentials object, its url is read instead"""
    # sqlite needs no driver beyond the standard library, so any ci job can build this engine
    engine = sa.create_engine("sqlite://")

    resource = dlt.resource([{"id": 1}], name="orders")
    record_table_input(resource, engine, None, "orders")
    engine.dispose()

    location = cast(TSqlDatabaseDataLocation, resource.inputs[0])
    assert location["location"] == "sqlite://"
    assert location["tables"] == ["orders"]
    assert "db_schema" not in location
