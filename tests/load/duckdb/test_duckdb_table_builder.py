import pytest
from copy import deepcopy
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

from dlt.destinations.duckdb.duck import DuckDbClient
from dlt.destinations.duckdb.configuration import DuckDbClientConfiguration

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def client(schema: Schema) -> DuckDbClient:
    # return client without opening connection
    return DuckDbClient(schema, DuckDbClientConfiguration(dataset_name="test_" + uniq_id()))


def test_create_table_with_hints(client: DuckDbClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["foreign_key"] = True
    sql = client._get_table_update_sql("event_test_table", mod_update, False)
    assert '"col1" BIGINT  NOT NULL' in sql
    assert '"col2" DOUBLE  NOT NULL' in sql
    assert '"col5" VARCHAR ' in sql
    assert '"col10" DATE ' in sql
    # no hints
    assert '"col3" BOOLEAN  NOT NULL' in sql
    assert '"col4" TIMESTAMP WITH TIME ZONE  NOT NULL' in sql

    # same thing with indexes
    client = DuckDbClient(client.schema, DuckDbClientConfiguration(dataset_name="test_" + uniq_id(), create_indexes=True))
    sql = client._get_table_update_sql("event_test_table", mod_update, False)
    sqlfluff.parse(sql)
    assert '"col2" DOUBLE UNIQUE NOT NULL' in sql


def test_alter_table(client: DuckDbClient) -> None:
    # existing table has no columns
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    sqlfluff.parse(sql)
    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == len(TABLE_UPDATE)
    assert "event_test_table" in sql
