from typing import Any, cast

import pytest
import sqlalchemy as sa

import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.sources.sql_database.helpers import (
    TSqlDatabaseDataLocation,
    record_table_input,
)


@pytest.mark.parametrize("as_engine", [False, True], ids=["credentials", "engine"])
def test_sql_database_location_hides_password(as_engine: bool) -> None:
    """A location is never allowed to carry credentials, whichever form they came in"""
    connection_string = "postgresql+psycopg2://loader:top_secret@example.com:5432/dlt_data"
    credentials: Any = (
        sa.create_engine(connection_string)
        if as_engine
        else ConnectionStringCredentials(connection_string)
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
