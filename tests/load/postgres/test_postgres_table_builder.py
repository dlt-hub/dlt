import pytest
from copy import deepcopy

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

from dlt.destinations.postgres.postgres import PostgresClient
from dlt.destinations.postgres.configuration import PostgresClientConfiguration

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def client(schema: Schema) -> PostgresClient:
    # return client without opening connection
    return PostgresClient(schema, PostgresClientConfiguration(dataset_name="test_" + uniq_id()))


def test_create_table(client: PostgresClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql


def test_alter_table(client: PostgresClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    canonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    # must have several ALTER TABLE statements
    assert sql.count(f"ALTER TABLE {canonical_name}\nADD COLUMN") == len(TABLE_UPDATE)
    assert "event_test_table" in sql
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision  NOT NULL' in sql
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql
    assert '"col5" varchar' in sql
    assert '"col6" numeric(38,9)  NOT NULL' in sql
    assert '"col7" bytea' in sql
    assert '"col8" numeric(156,78)' in sql
    assert '"col9" jsonb  NOT NULL' in sql


def test_create_table_with_hints(client: PostgresClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["foreign_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)
    assert '"col1" bigint  NOT NULL' in sql
    assert '"col2" double precision UNIQUE NOT NULL' in sql
    assert '"col5" varchar ' in sql
    # no hints
    assert '"col3" boolean  NOT NULL' in sql
    assert '"col4" timestamp with time zone  NOT NULL' in sql

    # same thing without indexes
    client = PostgresClient(client.schema, PostgresClientConfiguration(dataset_name="test_" + uniq_id(), create_indexes=False))
    sql = client._get_table_update_sql("event_test_table", mod_update, False)
    assert '"col2" double precision  NOT NULL' in sql