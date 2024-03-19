from copy import deepcopy

import pytest
import sqlfluff

from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.impl.clickhouse.clickhouse import ClickhouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseCredentials,
    ClickhouseClientConfiguration,
)
from tests.load.utils import TABLE_UPDATE, empty_schema


@pytest.fixture
def clickhouse_client(empty_schema: Schema) -> ClickhouseClient:
    # Return a client without opening connection.
    creds = ClickhouseCredentials()
    return ClickhouseClient(
        empty_schema,
        ClickhouseClientConfiguration(dataset_name=f"test_{uniq_id()}", credentials=creds),
    )


def test_create_table(clickhouse_client: ClickhouseClient) -> None:
    statements = clickhouse_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(statements) == 1
    sql = statements[0]
    print(sql)
    sqlfluff.parse(sql, dialect="clickhouse")

    assert sql.strip().startswith("CREATE TABLE")
    assert "EVENT_TEST_TABLE" in sql
    assert '"COL1" NUMBER(19,0) NOT NULL' in sql
    assert '"COL2" FLOAT NOT NULL' in sql
    assert '"COL3" BOOLEAN NOT NULL' in sql
    assert '"COL4" TIMESTAMP_TZ NOT NULL' in sql
    assert '"COL5" VARCHAR' in sql
    assert '"COL6" NUMBER(38,9) NOT NULL' in sql
    assert '"COL7" BINARY' in sql
    assert '"COL8" NUMBER(38,0)' in sql
    assert '"COL9" VARIANT NOT NULL' in sql
    assert '"COL10" DATE NOT NULL' in sql


def test_alter_table(clickhouse_client: ClickhouseClient) -> None:
    statements = clickhouse_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    assert len(statements) == 1
    sql = statements[0]

    # TODO: sqlfluff doesn't parse clickhouse multi ADD COLUMN clause correctly
    # sqlfluff.parse(sql, dialect='clickhouse')

    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == 1
    assert sql.count("ADD COLUMN") == 1
    assert '"EVENT_TEST_TABLE"' in sql
    assert '"COL1" NUMBER(19,0) NOT NULL' in sql
    assert '"COL2" FLOAT NOT NULL' in sql
    assert '"COL3" BOOLEAN NOT NULL' in sql
    assert '"COL4" TIMESTAMP_TZ NOT NULL' in sql
    assert '"COL5" VARCHAR' in sql
    assert '"COL6" NUMBER(38,9) NOT NULL' in sql
    assert '"COL7" BINARY' in sql
    assert '"COL8" NUMBER(38,0)' in sql
    assert '"COL9" VARIANT NOT NULL' in sql
    assert '"COL10" DATE' in sql

    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = clickhouse_client._get_table_update_sql("event_test_table", mod_table, True)[0]

    assert '"COL1"' not in sql
    assert '"COL2" FLOAT NOT NULL' in sql


def test_create_table_with_partition_and_cluster(clickhouse_client: ClickhouseClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    statements = clickhouse_client._get_table_update_sql("event_test_table", mod_update, False)
    assert len(statements) == 1
    sql = statements[0]

    # TODO: Can't parse cluster by
    # sqlfluff.parse(sql, dialect="clickhouse")

    # clustering must be the last
    assert sql.endswith('CLUSTER BY ("COL2","COL5")')
