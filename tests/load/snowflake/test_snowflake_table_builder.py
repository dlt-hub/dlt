from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

from copy import deepcopy

import pytest
import sqlfluff

from dlt.common.utils import uniq_id
from dlt.common.schema import Schema, utils
from dlt.destinations import snowflake
from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient, SUPPORTED_HINTS
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
        snowflake_client.sql_client.fully_qualified_dataset_name(quote=False)
        == snowflake_client.config.dataset_name.upper()
    )
    with snowflake_client.sql_client.with_staging_dataset():
        assert (
            snowflake_client.sql_client.fully_qualified_dataset_name(quote=False)
            == (
                snowflake_client.config.staging_dataset_name_layout
                % snowflake_client.config.dataset_name
            ).upper()
        )

    statements = snowflake_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False, [])
    assert len(statements) == 1
    sql = statements[0]
    sqlfluff.parse(sql, dialect="snowflake")

    assert sql.strip().startswith("CREATE TABLE")
    assert "EVENT_TEST_TABLE" in sql
    assert '"COL1" NUMBER(19,0)  NOT NULL' in sql
    assert '"COL2" FLOAT  NOT NULL' in sql
    assert '"COL3" BOOLEAN  NOT NULL' in sql
    assert '"COL4" TIMESTAMP_TZ  NOT NULL' in sql
    assert '"COL5" VARCHAR' in sql
    assert '"COL6" NUMBER(38,9)  NOT NULL' in sql
    assert '"COL7" BINARY' in sql
    assert '"COL8" NUMBER(38,0)' in sql
    assert '"COL9" VARIANT  NOT NULL' in sql
    assert '"COL10" DATE  NOT NULL' in sql


def test_create_table_with_hints(snowflake_client: SnowflakeClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE[:11])
    # mock hints
    snowflake_client.config.create_indexes = True
    snowflake_client.active_hints = SUPPORTED_HINTS

    mod_update[0]["primary_key"] = True
    mod_update[5]["primary_key"] = True

    mod_update[0]["sort"] = True

    # unique constraints are always single columns
    mod_update[1]["unique"] = True
    mod_update[7]["unique"] = True

    mod_update[4]["parent_key"] = True

    sql = ";".join(
        snowflake_client._get_table_update_sql("event_test_table", mod_update, False, [])
    )

    assert sql.strip().startswith("CREATE TABLE")
    assert "EVENT_TEST_TABLE" in sql
    assert '"COL1" NUMBER(19,0)  NOT NULL' in sql
    assert '"COL2" FLOAT UNIQUE NOT NULL' in sql
    assert '"COL3" BOOLEAN  NOT NULL' in sql
    assert '"COL4" TIMESTAMP_TZ  NOT NULL' in sql
    assert '"COL5" VARCHAR' in sql
    assert '"COL6" NUMBER(38,9)  NOT NULL' in sql
    assert '"COL7" BINARY' in sql
    assert '"COL8" NUMBER(38,0) UNIQUE' in sql
    assert '"COL9" VARIANT  NOT NULL' in sql
    assert '"COL10" DATE  NOT NULL' in sql

    # PRIMARY KEY constraint
    assert 'CONSTRAINT "PK_EVENT_TEST_TABLE_' in sql
    assert 'PRIMARY KEY ("COL1", "COL6")' in sql


def test_alter_table(snowflake_client: SnowflakeClient) -> None:
    storage_columns = deepcopy(TABLE_UPDATE[:1])
    new_columns = deepcopy(TABLE_UPDATE[1:10])
    statements = snowflake_client._get_table_update_sql(
        "event_test_table", new_columns, True, storage_columns
    )

    assert len(statements) == 2, "Should have one ADD COLUMN and one DROP CLUSTERING KEY statement"
    add_column_sql = statements[0]

    # TODO: sqlfluff doesn't parse snowflake multi ADD COLUMN clause correctly
    # sqlfluff.parse(add_column_sql, dialect='snowflake')

    assert add_column_sql.startswith("ALTER TABLE")
    assert add_column_sql.count("ALTER TABLE") == 1
    assert add_column_sql.count("ADD COLUMN") == 1
    assert '"EVENT_TEST_TABLE"' in add_column_sql
    assert '"COL1"' not in add_column_sql
    assert '"COL2" FLOAT  NOT NULL' in add_column_sql
    assert '"COL3" BOOLEAN  NOT NULL' in add_column_sql
    assert '"COL4" TIMESTAMP_TZ  NOT NULL' in add_column_sql
    assert '"COL5" VARCHAR' in add_column_sql
    assert '"COL6" NUMBER(38,9)  NOT NULL' in add_column_sql
    assert '"COL7" BINARY' in add_column_sql
    assert '"COL8" NUMBER(38,0)' in add_column_sql
    assert '"COL9" VARIANT  NOT NULL' in add_column_sql
    assert '"COL10" DATE' in add_column_sql


def test_alter_table_with_hints(snowflake_client: SnowflakeClient) -> None:
    # mock hints
    snowflake_client.active_hints = SUPPORTED_HINTS

    # test primary key and unique hints
    new_columns = deepcopy(TABLE_UPDATE[11:])
    new_columns[0]["primary_key"] = True
    new_columns[1]["unique"] = True
    storage_columns = deepcopy(TABLE_UPDATE[:11])
    statements = snowflake_client._get_table_update_sql(
        "event_test_table", new_columns, True, storage_columns
    )

    assert len(statements) == 2, "Should have one ADD COLUMN and one DROP CLUSTERING KEY statement"
    add_column_sql = statements[0]
    assert "PRIMARY KEY" not in add_column_sql  # PK constraint ignored for alter
    assert '"COL2_NULL" FLOAT UNIQUE' in add_column_sql

    # test cluster hint

    # case: drop clustering (always run if no cluster hints in new_columns and storage_columns)
    cluster_by_sql = statements[1]

    assert cluster_by_sql.startswith("ALTER TABLE")
    assert '"EVENT_TEST_TABLE"' in cluster_by_sql
    assert cluster_by_sql.endswith("DROP CLUSTERING KEY")

    # case: add clustering (without clustering -> with clustering)
    storage_columns_without_clustering = deepcopy(TABLE_UPDATE[:1])
    new_columns_with_clustering = deepcopy(TABLE_UPDATE[1:2])
    new_columns_with_clustering[0]["cluster"] = True  # COL2
    statements = snowflake_client._get_table_update_sql(
        "event_test_table", new_columns_with_clustering, True, storage_columns_without_clustering
    )

    assert len(statements) == 2, "Should have one ADD COLUMN and one CLUSTER BY statement"
    cluster_by_sql = statements[1]
    assert cluster_by_sql.startswith("ALTER TABLE")
    assert '"EVENT_TEST_TABLE"' in cluster_by_sql
    assert 'CLUSTER BY ("COL2")' in cluster_by_sql

    # case: modify clustering (extend cluster columns)
    storage_columns_with_clustering = deepcopy(TABLE_UPDATE[:2])
    storage_columns_with_clustering[1]["cluster"] = True  # COL2
    new_columns_with_clustering = deepcopy(TABLE_UPDATE[2:5])
    new_columns_with_clustering[2]["cluster"] = True  # COL5
    statements = snowflake_client._get_table_update_sql(
        "event_test_table", new_columns_with_clustering, True, storage_columns_with_clustering
    )

    assert len(statements) == 2, "Should have one ADD COLUMN and one CLUSTER BY statement"
    cluster_by_sql = statements[1]
    assert cluster_by_sql.count("ALTER TABLE") == 1
    assert cluster_by_sql.count("CLUSTER BY") == 1
    assert 'CLUSTER BY ("COL2","COL5")' in cluster_by_sql

    # case: modify clustering (reorder cluster columns)
    storage_columns_reordered = deepcopy(TABLE_UPDATE[:5])
    storage_columns_reordered[1]["cluster"] = True  # COL2
    storage_columns_reordered[4]["cluster"] = True  # COL5
    storage_columns_reordered[1], storage_columns_reordered[4] = (  # swap order
        storage_columns_reordered[4],
        storage_columns_reordered[1],
    )
    new_columns = deepcopy(TABLE_UPDATE[5:6])
    statements = snowflake_client._get_table_update_sql(
        "event_test_table", new_columns, True, storage_columns_reordered
    )

    assert len(statements) == 2, "Should have one ADD COLUMN and one CLUSTER BY statement"
    cluster_by_sql = statements[1]
    assert 'CLUSTER BY ("COL5","COL2")' in cluster_by_sql  # reordered (COL5 first)


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
    assert cs_client.sql_client.fully_qualified_dataset_name(quote=False)[0] == "T"  # Test
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
