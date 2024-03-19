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

    # sqlfluff struggles with clickhouse's backtick escape characters.
    # sqlfluff.parse(sql, dialect="clickhouse")

    assert sql.strip().startswith("CREATE TABLE")
    assert "event_test_table" in sql
    assert "`col1` Int64" in sql
    assert "`col2` Float64" in sql
    assert "`col3` Boolean" in sql
    assert "`col4` DateTime('UTC')" in sql
    assert "`col5` String" in sql
    assert "`col6` Decimal(38,9)" in sql
    assert "`col7` String" in sql
    assert "`col8` Decimal(76,0)" in sql
    assert "`col9` String" in sql
    assert "`col10` Date" in sql
    assert "`col11` DateTime" in sql
    assert "`col1_null` Nullable(Int64)" in sql
    assert "`col2_null` Nullable(Float64)" in sql
    assert "`col3_null` Nullable(Boolean)" in sql
    assert "`col4_null` Nullable(DateTime('UTC'))" in sql
    assert "`col5_null` Nullable(String)" in sql
    assert "`col6_null` Nullable(Decimal(38,9))" in sql
    assert "`col7_null` Nullable(String)" in sql
    assert "`col8_null` Nullable(Decimal(76,0))" in sql
    assert "`col9_null` Nullable(String)" in sql
    assert "`col10_null` Nullable(Date)" in sql
    assert "`col11_null` Nullable(DateTime)" in sql
    assert "`col1_precision` Int64" in sql
    assert "`col4_precision` DateTime(3, 'UTC')" in sql
    assert "`col5_precision` String" in sql
    assert "`col6_precision` Decimal(6,2)" in sql
    assert "`col7_precision` String" in sql
    assert "`col11_precision` DateTime" in sql


def test_alter_table(clickhouse_client: ClickhouseClient) -> None:
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
    assert "`col4` DateTime('UTC')" in sql
    assert "`col5` String" in sql
    assert "`col6` Decimal(38,9)" in sql
    assert "`col7` String" in sql
    assert "`col8` Decimal(76,0)" in sql
    assert "`col9` String" in sql
    assert "`col10` Date" in sql
    assert "`col11` DateTime" in sql
    assert "`col1_null` Nullable(Int64)" in sql
    assert "`col2_null` Nullable(Float64)" in sql
    assert "`col3_null` Nullable(Boolean)" in sql
    assert "`col4_null` Nullable(DateTime('UTC'))" in sql
    assert "`col5_null` Nullable(String)" in sql
    assert "`col6_null` Nullable(Decimal(38,9))" in sql
    assert "`col7_null` Nullable(String)" in sql
    assert "`col8_null` Nullable(Decimal(76,0))" in sql
    assert "`col9_null` Nullable(String)" in sql
    assert "`col10_null` Nullable(Date)" in sql
    assert "`col11_null` Nullable(DateTime)" in sql
    assert "`col1_precision` Int64" in sql
    assert "`col4_precision` DateTime(3, 'UTC')" in sql
    assert "`col5_precision` String" in sql
    assert "`col6_precision` Decimal(6,2)" in sql
    assert "`col7_precision` String" in sql
    assert "`col11_precision` DateTime" in sql

    mod_table = deepcopy(TABLE_UPDATE)
    mod_table.pop(0)
    sql = clickhouse_client._get_table_update_sql("event_test_table", mod_table, True)[0]

    assert "`col1`" not in sql
    assert "`col2` Float64" in sql


@pytest.mark.usefixtures("empty_schema")
def test_create_table_with_primary_keys(clickhouse_client: ClickhouseClient) -> None:
    mod_update = deepcopy(TABLE_UPDATE)

    mod_update[1]["primary_key"] = True
    mod_update[4]["primary_key"] = True
    statements = clickhouse_client._get_table_update_sql("event_test_table", mod_update, False)
    assert len(statements) == 1
    sql = statements[0]

    assert sql.endswith("PRIMARY KEY (`col2`, `col5`)")
