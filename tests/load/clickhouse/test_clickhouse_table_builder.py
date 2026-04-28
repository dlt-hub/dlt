from copy import deepcopy
from typing import Callable, Optional, Tuple

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.utils import custom_environ, digest128
from dlt.common.utils import uniq_id
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from dlt.common.schema.utils import new_table, pipeline_state_table
from tests.load.clickhouse.utils import clickhouse_client
from tests.load.utils import TABLE_UPDATE, empty_schema

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


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
    assert "`col1_precision` Int16" in sql
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
    assert "`col1_precision` Int16" in sql
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


@pytest.mark.parametrize(
    "naming,case",
    [
        (None, str.lower),
        ("tests.common.cases.normalizers.sql_upper", str.upper),
    ],
    ids=["snake_case", "sql_upper"],
)
@pytest.mark.parametrize(
    "schema_table_name,expected_columns",
    [
        ("version_table_name", ("schema_name", "inserted_at")),
        ("state_table_name", ("pipeline_name", "_dlt_load_id")),
        ("loads_table_name", ("load_id",)),
    ],
    ids=["version", "pipeline_state", "loads"],
)
def test_clickhouse_internal_metadata_tables_have_sort_keys(
    clickhouse_client: ClickHouseClient,
    schema_table_name: str,
    expected_columns: Tuple[str, ...],
    naming: Optional[str],
    case: Callable[[str], str],
) -> None:
    if naming is not None:
        with custom_environ({"SCHEMA__NAMING": naming}):
            clickhouse_client.schema.update_normalizers()

    if schema_table_name == "state_table_name":
        clickhouse_client.schema.update_table(pipeline_state_table())

    table_name = getattr(clickhouse_client.schema, schema_table_name)
    new_columns = list(clickhouse_client.schema.tables[table_name]["columns"].values())

    sql = clickhouse_client._get_table_update_sql(table_name, new_columns, False)[0]

    # uses casing function to approximate UPPER naming convention
    expected_order_by = "(" + ", ".join(case(c) for c in expected_columns) + ")"
    assert f"ORDER BY {expected_order_by}" in sql


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


@pytest.mark.parametrize(
    "has_dedup_sort,hard_delete,expected_engine",
    [
        (False, False, "ENGINE = ReplacingMergeTree"),
        (False, True, "ENGINE = ReplacingMergeTree"),
        (True, False, "ENGINE = ReplacingMergeTree(`col2`)"),
        (True, True, "ENGINE = ReplacingMergeTree(`col2`, `col3`)"),
    ],
    ids=[
        "no_version",
        "hard_delete_without_version",
        "dedup_sort_only",
        "dedup_sort_and_hard_delete",
    ],
)
def test_clickhouse_replacing_merge_tree(
    clickhouse_client: ClickHouseClient,
    has_dedup_sort: bool,
    hard_delete: bool,
    expected_engine: str,
) -> None:
    columns = deepcopy(TABLE_UPDATE[:3])
    columns[0]["primary_key"] = True
    if has_dedup_sort:
        columns[1]["dedup_sort"] = "desc"
    if hard_delete:
        columns[2]["hard_delete"] = True

    table_name = "rmt_table"
    clickhouse_client.schema.update_table(
        new_table(table_name, write_disposition="append", columns=columns)
    )
    clickhouse_client.schema.tables[table_name]["x-table-engine-type"] = (  # type: ignore[typeddict-unknown-key]
        "replacing_merge_tree"
    )

    sql = clickhouse_client._get_table_update_sql(table_name, columns, False)[0]
    assert expected_engine in sql
    assert "PRIMARY KEY (`col1`)" in sql


def test_clickhouse_replacing_merge_tree_fallback_non_append(
    clickhouse_client: ClickHouseClient,
) -> None:
    columns = deepcopy(TABLE_UPDATE[:3])
    columns[0]["primary_key"] = True
    columns[1]["dedup_sort"] = "desc"

    table_name = "rmt_fallback_table"
    clickhouse_client.schema.update_table(
        new_table(table_name, write_disposition="replace", columns=columns)
    )
    clickhouse_client.schema.tables[table_name]["x-table-engine-type"] = (  # type: ignore[typeddict-unknown-key]
        "replacing_merge_tree"
    )

    sql = clickhouse_client._get_table_update_sql(table_name, columns, False)[0]
    assert "ENGINE = MergeTree" in sql
    assert "ReplacingMergeTree" not in sql
