from copy import deepcopy

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.common.utils import custom_environ, digest128
from dlt.common.utils import uniq_id
from dlt.destinations import clickhouse
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from tests.load.utils import TABLE_UPDATE, empty_schema


@pytest.fixture
def clickhouse_client(empty_schema: Schema) -> ClickHouseClient:
    # Return a client without opening connection.
    creds = ClickHouseCredentials()
    return clickhouse().client(
        empty_schema,
        ClickHouseClientConfiguration(credentials=creds)._bind_dataset_name(f"test_{uniq_id()}"),
    )


def test_clickhouse_configuration() -> None:
    # Check names normalized.
    with custom_environ(
        {
            "DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME": "username",
            "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST": "host",
            "DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE": "mydb",
            "DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD": "fuss_do_rah",
        }
    ):
        C = resolve_configuration(ClickHouseCredentials(), sections=("destination", "clickhouse"))
        assert C.database == "mydb"
        assert C.password == "fuss_do_rah"

    # Check fingerprint.
    assert ClickHouseClientConfiguration().fingerprint() == ""
    # Based on host.
    c = resolve_configuration(
        ClickHouseCredentials(),
        explicit_value="clickhouse://user1:pass@host1/db1",
    )
    assert ClickHouseClientConfiguration(credentials=c).fingerprint() == digest128("host1")


def test_clickhouse_create_table(clickhouse_client: ClickHouseClient) -> None:
    statements = clickhouse_client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(statements) == 1
    sql = statements[0]

    # sqlfluff struggles with clickhouse's backtick escape characters.
    # sqlfluff.parse(sql, dialect="clickhouse")

    assert sql.strip().startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` Int64" in sql
    assert "`col2` Float64" in sql
    assert "`col3` Boolean" in sql
    assert "`col4` DateTime64(6,'UTC')" in sql
    assert "`col5` String" in sql
    assert "`col6` Decimal(38,9)" in sql
    assert "`col7` String" in sql
    assert "`col8` Decimal(76,0)" in sql
    assert "`col9` String" in sql
    assert "`col10` Date" in sql
    assert "`col11` String" in sql
    assert "`col1_null` Nullable(Int64)" in sql
    assert "`col2_null` Nullable(Float64)" in sql
    assert "`col3_null` Nullable(Boolean)" in sql
    assert "`col4_null` Nullable(DateTime64(6,'UTC'))" in sql
    assert "`col5_null` Nullable(String)" in sql
    assert "`col6_null` Nullable(Decimal(38,9))" in sql
    assert "`col7_null` Nullable(String)" in sql
    assert "`col8_null` Nullable(Decimal(76,0))" in sql
    assert "`col9_null` Nullable(String)" in sql
    assert "`col10_null` Nullable(Date)" in sql
    assert "`col11_null` Nullable(String)" in sql
    assert "`col1_precision` Int64" in sql
    assert "`col4_precision` DateTime64(3,'UTC')" in sql
    assert "`col5_precision` String" in sql
    assert "`col6_precision` Decimal(6,2)" in sql
    assert "`col7_precision` String" in sql
    assert "`col11_precision` String" in sql


def test_clickhouse_alter_table(clickhouse_client: ClickHouseClient) -> None:
    statements = clickhouse_client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)
    assert len(statements) == 1
    sql = statements[0]

    # sqlfluff struggles with clickhouse's backtick escape characters.
    # sqlfluff.parse(sql, dialect="clickhouse")

    # Alter table statements only accept `Nullable` modifiers.
    assert sql.startswith("ALTER TABLE")
    assert sql.count("ALTER TABLE") == 1
    assert "event_test_table" in sql
    assert "`col1` Int64" in sql
    assert "`col2` Float64" in sql
    assert "`col3` Boolean" in sql
    assert "`col4` DateTime64(6,'UTC')" in sql
    assert "`col5` String" in sql
    assert "`col6` Decimal(38,9)" in sql
    assert "`col7` String" in sql
    assert "`col8` Decimal(76,0)" in sql
    assert "`col9` String" in sql
    assert "`col10` Date" in sql
    assert "`col11` String" in sql
    assert "`col1_null` Nullable(Int64)" in sql
    assert "`col2_null` Nullable(Float64)" in sql
    assert "`col3_null` Nullable(Boolean)" in sql
    assert "`col4_null` Nullable(DateTime64(6,'UTC'))" in sql
    assert "`col5_null` Nullable(String)" in sql
    assert "`col6_null` Nullable(Decimal(38,9))" in sql
    assert "`col7_null` Nullable(String)" in sql
    assert "`col8_null` Nullable(Decimal(76,0))" in sql
    assert "`col9_null` Nullable(String)" in sql
    assert "`col10_null` Nullable(Date)" in sql
    assert "`col11_null` Nullable(String)" in sql
    assert "`col1_precision` Int64" in sql
    assert "`col4_precision` DateTime64(3,'UTC')" in sql
    assert "`col5_precision` String" in sql
    assert "`col6_precision` Decimal(6,2)" in sql
    assert "`col7_precision` String" in sql
    assert "`col11_precision` String" in sql

    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = clickhouse_client._get_table_update_sql("event_test_table", mod_table, True)[0]

    assert "`col1`" not in sql
    assert "`col2` Float64" in sql


@pytest.mark.usefixtures("empty_schema")
def test_clickhouse_create_table_with_primary_keys(
    clickhouse_client: ClickHouseClient,
) -> None:
    mod_update = deepcopy(TABLE_UPDATE)

    mod_update[1]["primary_key"] = True
    mod_update[4]["primary_key"] = True
    statements = clickhouse_client._get_table_update_sql("event_test_table", mod_update, False)
    assert len(statements) == 1
    sql = statements[0]

    assert sql.endswith("PRIMARY KEY (`col2`, `col5`)")


@pytest.mark.skip(
    "Only `primary_key` hint has been implemented so far, which isn't specified inline with the"
    " column definition."
)
def test_clickhouse_create_table_with_hints(client: ClickHouseClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)

    mod_update[0]["primary_key"] = True
    mod_update[0]["sort"] = True
    mod_update[1]["cluster"] = True
    mod_update[4]["cluster"] = True

    sql = client._get_table_update_sql("event_test_table", TABLE_UPDATE, True)

    assert "`col1` bigint SORTKEY NOT NULL" in sql
    assert "`col2` double precision DISTKEY NOT NULL" in sql
    assert "`col5` varchar(max) DISTKEY" in sql
    # No hints.
    assert "`col3` boolean  NOT NULL" in sql
    assert "`col4` timestamp with time zone  NOT NULL" in sql


def test_clickhouse_table_engine_configuration() -> None:
    with custom_environ(
        {
            "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST": "localhost",
            "DESTINATION__CLICKHOUSE__DATASET_NAME": f"test_{uniq_id()}",
        }
    ):
        config = resolve_configuration(
            ClickHouseClientConfiguration(), sections=("destination", "clickhouse")
        )
        assert config.table_engine_type == "merge_tree"

    with custom_environ(
        {
            "DESTINATION__CLICKHOUSE__CREDENTIALS__HOST": "localhost",
            "DESTINATION__CLICKHOUSE__TABLE_ENGINE_TYPE": "replicated_merge_tree",
            "DESTINATION__CLICKHOUSE__DATASET_NAME": f"test_{uniq_id()}",
        }
    ):
        config = resolve_configuration(
            ClickHouseClientConfiguration(), sections=("destination", "clickhouse")
        )
        assert config.table_engine_type == "replicated_merge_tree"
