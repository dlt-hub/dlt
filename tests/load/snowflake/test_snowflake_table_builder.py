from copy import deepcopy

import pytest
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema
from dlt.destinations.snowflake.snowflake import SnowflakeClient
from dlt.destinations.snowflake.configuration import SnowflakeClientConfiguration, SnowflakeCredentials
from dlt.destinations.exceptions import DestinationSchemaWillNotUpdate

from tests.load.utils import TABLE_UPDATE


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def snowflake_client(schema: Schema) -> SnowflakeClient:
    # return client without opening connection
    creds = SnowflakeCredentials()
    return SnowflakeClient(schema, SnowflakeClientConfiguration(dataset_name="test_" + uniq_id(), credentials=creds))


def test_create_table(snowflake_client: SnowflakeClient) -> None:
    statements = snowflake_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(statements) == 1
    sql = statements[0]
    sqlfluff.parse(sql, dialect='snowflake')

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


def test_alter_table(snowflake_client: SnowflakeClient) -> None:
    statements = snowflake_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    assert len(statements) == 1
    sql = statements[0]

    # TODO: sqlfluff doesn't parse snowflake multi ADD COLUMN clause correctly
    # sqlfluff.parse(sql, dialect='snowflake')

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
    sql = snowflake_client._get_table_update_sql("event_test_table", mod_table, True)[0]

    assert '"COL1"' not in sql
    assert '"COL2" FLOAT NOT NULL' in sql


def test_create_table_with_partition_and_cluster(snowflake_client: SnowflakeClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)
    # timestamp
    mod_update[3]["partition"] = True
    mod_update[4]["cluster"] = True
    mod_update[1]["cluster"] = True
    statements = snowflake_client._get_table_update_sql("event_test_table", mod_update, False)
    assert len(statements) == 1
    sql = statements[0]

    # TODO: Can't parse cluster by
    # sqlfluff.parse(sql, dialect="snowflake")

    # clustering must be the last
    assert sql.endswith('CLUSTER BY ("COL2","COL5")')
