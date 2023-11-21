import pytest
from copy import deepcopy
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema

from dlt.destinations.impl.duckdb.duck import DuckDbClient
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def client(schema: Schema) -> DuckDbClient:
    # return client without opening connection
    return DuckDbClient(schema, DuckDbClientConfiguration(dataset_name="test_" + uniq_id()))


def test_create_table(client: DuckDbClient) -> None:
    # non existing table
    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)[0]
    sqlfluff.parse(sql, dialect="duckdb")
    assert "event_test_table" in sql
    assert '"col1" BIGINT  NOT NULL' in sql
    assert '"col2" DOUBLE  NOT NULL' in sql
    assert '"col3" BOOLEAN  NOT NULL' in sql
    assert '"col4" TIMESTAMP WITH TIME ZONE  NOT NULL' in sql
    assert '"col5" VARCHAR ' in sql
    assert '"col6" DECIMAL(38,9)  NOT NULL' in sql
    assert '"col7" BLOB ' in sql
    assert '"col8" DECIMAL(38,0)' in sql
    assert '"col9" JSON  NOT NULL' in sql
    assert '"col10" DATE  NOT NULL' in sql
    assert '"col11" TIME  NOT NULL' in sql
    assert '"col1_precision" SMALLINT  NOT NULL' in sql
    assert '"col4_precision" TIMESTAMP_MS  NOT NULL' in sql
    assert '"col5_precision" VARCHAR' in sql
    assert '"col6_precision" DECIMAL(6,2)  NOT NULL' in sql
    assert '"col7_precision" BLOB  NOT NULL' in sql
    assert '"col11_precision" TIME  NOT NULL' in sql


def test_alter_table(client: DuckDbClient) -> None:
    # existing table has no columns
    sqls = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    for sql in sqls:
        sqlfluff.parse(sql, dialect="duckdb")
    cannonical_name = client.sql_client.make_qualified_table_name("event_test_table")
    # must have several ALTER TABLE statements
    sql = ";\n".join(sqls)
    assert sql.count(f"ALTER TABLE {cannonical_name}\nADD COLUMN") == 28
    assert "event_test_table" in sql
    assert '"col1" BIGINT  NOT NULL' in sql
    assert '"col2" DOUBLE  NOT NULL' in sql
    assert '"col3" BOOLEAN  NOT NULL' in sql
    assert '"col4" TIMESTAMP WITH TIME ZONE  NOT NULL' in sql
    assert '"col5" VARCHAR ' in sql
    assert '"col6" DECIMAL(38,9)  NOT NULL' in sql
    assert '"col7" BLOB ' in sql
    assert '"col8" DECIMAL(38,0)' in sql
    assert '"col9" JSON  NOT NULL' in sql
    assert '"col10" DATE  NOT NULL' in sql
    assert '"col11" TIME  NOT NULL' in sql
    assert '"col1_precision" SMALLINT  NOT NULL' in sql
    assert '"col4_precision" TIMESTAMP_MS  NOT NULL' in sql
    assert '"col5_precision" VARCHAR' in sql
    assert '"col6_precision" DECIMAL(6,2)  NOT NULL' in sql
    assert '"col7_precision" BLOB  NOT NULL' in sql
    assert '"col11_precision" TIME  NOT NULL' in sql


def test_create_table_with_hints(client: DuckDbClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["unique"] = True
    mod_update[4]["foreign_key"] = True
    sql = ';'.join(client._get_table_update_sql("event_test_table", mod_update, False))
    assert '"col1" BIGINT  NOT NULL' in sql
    assert '"col2" DOUBLE  NOT NULL' in sql
    assert '"col5" VARCHAR ' in sql
    assert '"col10" DATE ' in sql
    # no hints
    assert '"col3" BOOLEAN  NOT NULL' in sql
    assert '"col4" TIMESTAMP WITH TIME ZONE  NOT NULL' in sql

    # same thing with indexes
    client = DuckDbClient(client.schema, DuckDbClientConfiguration(dataset_name="test_" + uniq_id(), create_indexes=True))
    sql = client._get_table_update_sql("event_test_table", mod_update, False)[0]
    sqlfluff.parse(sql)
    assert '"col2" DOUBLE UNIQUE NOT NULL' in sql
