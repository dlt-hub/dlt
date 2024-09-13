from copy import deepcopy

import pytest
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema, utils
from dlt.destinations import snowflake
from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)

from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def cs_client(empty_schema: Schema) -> SnowflakeClient:
    # change normalizer to case sensitive
    empty_schema._normalizers_config["names"] = "tests.common.cases.normalizers.title_case"
    empty_schema.update_normalizers()
    return create_client(empty_schema)


@pytest.fixture
def snowflake_client(empty_schema: Schema) -> SnowflakeClient:
    return create_client(empty_schema)


def create_client(schema: Schema) -> SnowflakeClient:
    # return client without opening connection
    creds = SnowflakeCredentials()
    return snowflake().client(
        schema,
        SnowflakeClientConfiguration(credentials=creds)._bind_dataset_name(
            dataset_name="test_" + uniq_id()
        ),
    )


def test_create_table(snowflake_client: SnowflakeClient) -> None:
    # make sure we are in case insensitive mode
    assert snowflake_client.capabilities.generates_case_sensitive_identifiers() is False
    # check if dataset name is properly folded
    assert (
        snowflake_client.sql_client.fully_qualified_dataset_name(escape=False)
        == snowflake_client.config.dataset_name.upper()
    )
    with snowflake_client.sql_client.with_staging_dataset():
        assert (
            snowflake_client.sql_client.fully_qualified_dataset_name(escape=False)
            == (
                snowflake_client.config.staging_dataset_name_layout
                % snowflake_client.config.dataset_name
            ).upper()
        )

    statements = snowflake_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(statements) == 1
    sql = statements[0]
    sqlfluff.parse(sql, dialect="snowflake")

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


def test_create_table_case_sensitive(cs_client: SnowflakeClient) -> None:
    # did we switch to case sensitive
    assert cs_client.capabilities.generates_case_sensitive_identifiers() is True
    # check dataset names
    assert cs_client.sql_client.dataset_name.startswith("Test")
    with cs_client.with_staging_dataset():
        assert cs_client.sql_client.dataset_name.endswith("staginG")
    assert cs_client.sql_client.staging_dataset_name.endswith("staginG")
    # check tables
    cs_client.schema.update_table(
        utils.new_table("event_test_table", columns=deepcopy(TABLE_UPDATE))
    )
    sql = cs_client._get_table_update_sql(
        "Event_test_tablE",
        list(cs_client.schema.get_table_columns("Event_test_tablE").values()),
        False,
    )[0]
    sqlfluff.parse(sql, dialect="snowflake")
    # everything capitalized
    assert cs_client.sql_client.fully_qualified_dataset_name(escape=False)[0] == "T"  # Test
    # every line starts with "Col"
    for line in sql.split("\n")[1:]:
        assert line.startswith('"Col')


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
