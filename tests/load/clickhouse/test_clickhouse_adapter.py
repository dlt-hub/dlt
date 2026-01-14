import pytest

from typing import Generator, Dict, Literal, cast

import dlt
from dlt.common.schema.exceptions import UnboundColumnException
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.destinations.adapters import clickhouse_adapter
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.clickhouse_adapter import (
    extract_column_names,
    get_column_names_from_table_hint,
    set_column_hints_from_table_hint,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import (
    CODEC_HINT,
    PARTITION_HINT,
    SETTINGS_HINT,
    SORT_HINT,
    TDeployment,
    TSQLExprOrColumnSeq,
)
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.clickhouse.utils import (
    clickhouse_client,
    get_codecs,
    get_create_table_query,
    get_deployment_type,
    get_partition_key,
    get_sorting_key,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


# this constant relates to `clickhouse_adapter_resource`
SORT_PARTITION_CASES = (
    # `sort`/`partition` input, expected ORDER BY / PARTITION clause, expected sorting/partition key
    # expressions (we do not mix in uppercase names because expressions are not normalized)
    pytest.param("town", "town", "town", id="expr-simple"),
    pytest.param("(town, number % 4)", "(town, number % 4)", "town, number % 4", id="expr-complex"),
    # sequences of column names (we mix in uppercase names to verify normalization)
    pytest.param(["TOWN"], "(town)", "town", id="seq-single"),
    pytest.param(("street", "TOWN"), "(street, town)", "street, town", id="seq-multi"),
)


@pytest.fixture
def clickhouse_adapter_resource() -> DltResource:
    @dlt.resource(
        columns={
            "TOWN": {"data_type": "text"},
            "street": {"data_type": "text"},
            "number": {"data_type": "bigint"},
        }
    )
    def data():
        yield [{"TOWN": "Dubai", "street": "Sheikh Zayed Road", "number": 1}]

    return data()


def test_clickhouse_adapter_table_engine_type() -> None:
    @dlt.resource
    def merge_tree_resource() -> Generator[Dict[str, int], None, None]:
        yield {"field1": 1, "field2": 2}

    # `ReplicatedMergeTree` has been supplanted by `ReplacingMergeTree` on CH Cloud,
    # which is automatically selected even if `MergeTree` is selected.
    # See https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree.

    # The `Log` Family of engines are only supported in self-managed deployments.
    # So can't test in CH Cloud CI.

    @dlt.resource
    def replicated_merge_tree_resource() -> Generator[Dict[str, int], None, None]:
        yield {"field1": 1, "field2": 2}

    @dlt.resource
    def not_annotated_resource() -> Generator[Dict[str, int], None, None]:
        """Non annotated resource will default to `SharedMergeTree` for CH cloud
        and `MergeTree` for self-managed installation."""
        yield {"field1": 1, "field2": 2}

    clickhouse_adapter(merge_tree_resource, table_engine_type="merge_tree")
    clickhouse_adapter(replicated_merge_tree_resource, table_engine_type="replicated_merge_tree")

    pipe = dlt.pipeline(
        pipeline_name="adapter_test",
        destination="clickhouse",
        dev_mode=True,
        dataset_name="adapter_test_ds",
    )

    with pipe.sql_client() as client:
        deployment_type: TDeployment = get_deployment_type(cast(ClickHouseSqlClient, client))

    if deployment_type == "ClickHouseCloud":
        pack = pipe.run(
            [
                merge_tree_resource,
                replicated_merge_tree_resource,
                not_annotated_resource,
            ]
        )
    else:
        # `ReplicatedMergeTree` not supported if only a single node.
        pack = pipe.run([merge_tree_resource, not_annotated_resource])

    assert_load_info(pack)

    with pipe.sql_client() as client:
        # Get a map of table names to full table names.
        tables = {}
        for table in client._list_tables():
            if "resource" in table:
                tables[table.split("___")[1]] = table
        if deployment_type == "ClickHouseCloud":
            assert (len(tables.keys())) == 3
        else:
            assert (len(tables.keys())) == 2

        # Check the table content.
        for full_table_name in tables.values():
            with client.execute_query(f"SELECT * FROM {full_table_name};") as cursor:
                res = cursor.fetchall()
                assert tuple(res[0])[:2] == (1, 2)

        # Check the table engine.
        for table_name, full_table_name in tables.items():
            with client.execute_query(
                "SELECT database, name, engine, engine_full FROM system.tables "
                f"WHERE name = '{full_table_name}';"
            ) as cursor:
                res = cursor.fetchall()
                if table_name in (
                    "merge_tree_resource",
                    "replicated_merge_tree_resource",
                ):
                    if deployment_type == "ClickHouseCloud":
                        assert tuple(res[0])[2] in (
                            "MergeTree",
                            "SharedMergeTree",
                            "ReplicatedMergeTree",
                        )
                    else:
                        assert tuple(res[0])[2] in ("MergeTree",)
                else:
                    # Non annotated resource needs to default to detected installation
                    # type, i.e. cloud or self-managed.
                    # CI runs on CH cloud, so will be `SharedMergeTree`.
                    if deployment_type == "ClickHouseCloud":
                        assert tuple(res[0])[2] == "SharedMergeTree"
                    else:
                        assert tuple(res[0])[2] == "MergeTree"

    # We can check the generated table's SQL, though.
    with pipe.destination_client() as dest_client:
        for table in tables:
            sql = dest_client._get_table_update_sql(  # type: ignore[attr-defined]
                table,
                pipe.default_schema.tables[table]["columns"].values(),
                generate_alter=False,
            )
            if table == "replicated_merge_tree_resource":
                assert "ENGINE = ReplicatedMergeTree" in sql[0]
            else:
                assert "ENGINE = MergeTree" or "ENGINE = SharedMergeTree" in sql[0]


# NOTE: if you update `test_clickhouse_adapter_sort`, check if the equivalent
# `test_clickhouse_adapter_partition` should also be updated
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "sort, expected_order_by_clause, expected_sorting_key", SORT_PARTITION_CASES
)
def test_clickhouse_adapter_sort(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
    sort: TSQLExprOrColumnSeq,
    expected_order_by_clause: str,
    expected_sorting_key: str,
) -> None:
    res = clickhouse_adapter(clickhouse_adapter_resource, sort=sort)
    table_schema = res.compute_table_schema()

    # table hint gets set correctly
    assert table_schema[SORT_HINT] == sort  # type: ignore[typeddict-item]

    # column hints get set correctly
    sort_column_names = get_column_names_from_table_hint(sort)
    for col in table_schema["columns"].values():
        assert col.get("sort") is (True if col["name"] in sort_column_names else None)

    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_sort", dev_mode=True)
    client = cast(ClickHouseClient, pipe.destination_client())

    # clause gets set correctly
    table_schema = client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = client._get_table_update_sql("data", new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert f"ORDER BY {expected_order_by_clause}" in sql

    # key gets set correctly
    pipe.run(res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    sorting_key = get_sorting_key(sql_client, table_name=res.name)
    assert sorting_key == expected_sorting_key


# NOTE: if you update `test_clickhouse_adapter_partition`, check if the equivalent
# `test_clickhouse_adapter_sort` should also be updated
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "partition, expected_partition_by_clause, expected_partition_key",
    SORT_PARTITION_CASES,
)
def test_clickhouse_adapter_partition(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
    partition: TSQLExprOrColumnSeq,
    expected_partition_by_clause: str,
    expected_partition_key: str,
) -> None:
    res = clickhouse_adapter(clickhouse_adapter_resource, partition=partition)
    table_schema = res.compute_table_schema()

    # table hint gets set correctly
    assert table_schema[PARTITION_HINT] == partition  # type: ignore[typeddict-item]

    # column hints get set correctly
    partition_column_names = get_column_names_from_table_hint(partition)
    for col in table_schema["columns"].values():
        assert col.get("partition") is (True if col["name"] in partition_column_names else None)

    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_partition", dev_mode=True)
    client = cast(ClickHouseClient, pipe.destination_client())

    # clause gets set correctly
    table_schema = client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = client._get_table_update_sql("data", new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert f"PARTITION BY {expected_partition_by_clause}" in sql

    # key gets set correctly
    pipe.run(res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    partition_key = get_partition_key(sql_client, table_name=res.name)
    assert partition_key == expected_partition_key


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_adapter_settings(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
) -> None:
    settings = {
        "allow_nullable_key": True,
        "max_suspicious_broken_parts": 500,
        "deduplicate_merge_projection_mode": "ignore",
        "merge_selecting_sleep_slowdown_factor": 1.2,
    }
    expected_settings_clause = (
        "allow_nullable_key = true,"
        " max_suspicious_broken_parts = 500,"
        " deduplicate_merge_projection_mode = 'ignore',"
        " merge_selecting_sleep_slowdown_factor = 1.2"
    )

    # hint gets set correctly
    res = clickhouse_adapter(clickhouse_adapter_resource, settings=settings)  # type: ignore[arg-type]
    table_schema = res.compute_table_schema()
    assert table_schema[SETTINGS_HINT] == settings  # type: ignore[typeddict-item]

    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_settings", dev_mode=True)
    client = cast(ClickHouseClient, pipe.destination_client())

    # clause gets set correctly
    client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = client._get_table_update_sql("data", new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert f"SETTINGS {expected_settings_clause}" in sql

    # settings get set correctly
    pipe.run(res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    create_table_query = get_create_table_query(sql_client, table_name=res.name)
    assert f"SETTINGS {expected_settings_clause}" in create_table_query


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_adapter_codecs(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
) -> None:
    # hints get set correctly
    codecs = {"TOWN": "ZSTD(3)", "number": "Delta, ZSTD(2)"}
    res = clickhouse_adapter(clickhouse_adapter_resource, codecs=codecs)
    table_schema = res.compute_table_schema()
    columns = table_schema["columns"]
    assert columns["TOWN"][CODEC_HINT] == "ZSTD(3)"  # type: ignore[typeddict-item]
    assert columns["number"][CODEC_HINT] == "Delta, ZSTD(2)"  # type: ignore[typeddict-item]

    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_codecs", dev_mode=True)
    client = cast(ClickHouseClient, pipe.destination_client())

    # clauses get set correctly
    client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    stmts = client._get_table_update_sql("data", new_columns, False)
    assert len(stmts) == 1
    sql = stmts[0]
    assert "`TOWN` Nullable(String) CODEC(ZSTD(3))," in sql
    assert "`street` Nullable(String)," in sql  # no codec
    assert "`number` Nullable(Int64) CODEC(Delta, ZSTD(2))" in sql

    # codecs get set correctly
    pipe.run(res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    applied_codecs = get_codecs(sql_client, table_name=res.name)
    assert applied_codecs["town"] == "CODEC(ZSTD(3))"
    assert applied_codecs["street"] == ""
    assert applied_codecs["number"] == "CODEC(Delta(8), ZSTD(2))"


def test_clickhouse_adapter_param_interplay(clickhouse_adapter_resource: DltResource) -> None:
    """Asserts column hints are set correctly when multiple adapter parameters are used together."""

    res = clickhouse_adapter(
        clickhouse_adapter_resource,
        sort=["number"],
        partition=["street"],
        codecs={"TOWN": "ZSTD(3)", "number": "Delta, ZSTD(2)"},
    )
    columns = res.compute_table_schema()["columns"]
    assert columns["number"]["sort"] is True
    assert columns["number"]["nullable"] is False
    assert columns["street"]["partition"] is True
    assert columns["street"]["nullable"] is False
    assert columns["TOWN"][CODEC_HINT] == "ZSTD(3)"  # type: ignore[typeddict-item]
    assert columns["number"][CODEC_HINT] == "Delta, ZSTD(2)"  # type: ignore[typeddict-item]


def test_clickhouse_adapter_type_check() -> None:
    with pytest.raises(TypeError):
        clickhouse_adapter([{"foo": "bar"}], codecs="not_a_dict")  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        clickhouse_adapter([{"foo": "bar"}], sort=False)  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        clickhouse_adapter([{"foo": "bar"}], partition=True)  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        clickhouse_adapter([{"foo": "bar"}], settings="not_a_dict")  # type: ignore[arg-type]


@pytest.mark.parametrize("hint", ("sort", "partition"))
@pytest.mark.parametrize(
    "table_hint,expected_key",
    (
        pytest.param(["c2", "c3"], "(c2, c3)", id="seq"),
        pytest.param("(upper(c2), c3)", "(upper(c2), c3)", id="expr"),
    ),
)
def test_clickhouse_adapter_column_hints(
    clickhouse_client: ClickHouseClient,
    hint: Literal["sort", "partition"],
    table_hint: TSQLExprOrColumnSeq,
    expected_key: str,
) -> None:
    """Tests `sort` and `partition` column hints and their interplay with corresponding table hints."""

    @dlt.resource(
        table_name="foo",
        columns={
            "c1": {"data_type": "text", "nullable": True, hint: True},  # type: ignore[misc]
            "c2": {"data_type": "text", hint: False},  # type: ignore[misc]
            "c3": {"data_type": "text", "nullable": True},
        },
    )
    def res_with_column_hints_only():
        yield [{"c1": "a", "c2": "b", "c3": "c"}]

    table_hint_key = SORT_HINT if hint == "sort" else PARTITION_HINT
    clause_type = "ORDER BY" if hint == "sort" else "PARTITION BY"

    table_schema = res_with_column_hints_only.compute_table_schema()

    # column hints from resource are retained
    columns = table_schema["columns"]
    assert columns["c1"].get(hint) is True
    assert columns["c2"].get(hint) is False
    assert columns["c3"].get(hint) is None

    # table hint is not set
    assert table_hint_key not in table_schema

    # clause is based on column hints
    clickhouse_client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    sql = clickhouse_client._get_table_update_sql("foo", new_columns, False)[0]
    assert f"{clause_type} (c1)" in sql

    # now add table hint
    kwargs = {str(hint): table_hint}
    res_with_table_hint = clickhouse_adapter(res_with_column_hints_only, **kwargs)  # type: ignore[arg-type]

    # table hint is set correctly
    table_schema = res_with_table_hint.compute_table_schema()
    assert table_schema[table_hint_key] == table_hint  # type: ignore[typeddict-item]

    # NOTE: we unit test setting of column hints based on table hint in
    # `test_set_column_hints_from_table_hint`, so we skip that here

    # clause is based on table hint
    clickhouse_client.schema.update_table(table_schema)
    new_columns = list(table_schema["columns"].values())
    sql = clickhouse_client._get_table_update_sql("foo", new_columns, False)[0]
    assert f"{clause_type} {expected_key}" in sql


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("hint", ("sort", "partition"))
@pytest.mark.parametrize(
    "table_hint_val",
    (
        pytest.param(["unbound_col"], id="seq-unbound-col"),
        pytest.param("toYYYYMMDD(unbound_col)", id="expr-unbound-col"),
    ),
)
def test_clickhouse_adapter_unbound_column(
    destination_config: DestinationTestConfiguration,
    hint: Literal["sort", "partition"],
    table_hint_val: TSQLExprOrColumnSeq,
) -> None:
    """Tests that unbound columns in `sort` and `partition` hints lead to UnboundColumnException.

    Error is thrown because unbound columns are added to schema as non-nullable,
    and dlt has generic mechanism in place to raise on unbound non-nullable columns.
    """

    kwargs = {str(hint): table_hint_val}
    res = clickhouse_adapter([{"bound_col": "foo"}], **kwargs)  # type: ignore[arg-type]
    pipe = destination_config.setup_pipeline(
        "test_clickhouse_adapter_unbound_column", dev_mode=True
    )

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipe.run(res, **destination_config.run_kwargs)
    assert isinstance(pip_ex.value.__cause__, UnboundColumnException)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "param",
    ("sort", "partition"),  # adapter params that accept SQL expressions
)
def test_clickhouse_adapter_expr_fails(
    destination_config: DestinationTestConfiguration, param: Literal["sort", "partition"]
) -> None:
    pipe = destination_config.setup_pipeline(
        "test_clickhouse_adapter_partition_sort_fails", dev_mode=True
    )

    # fails when expression uses non-normalized column name
    @dlt.resource(columns={"TIMESTAMP": {"nullable": False}})
    def res_upper():
        yield [{"TIMESTAMP": "2025-12-15T13:32:45Z"}]

    clickhouse_adapter(res_upper, **{str(param): "toYYYYMMDD(TIMESTAMP)"})  # type: ignore[arg-type]
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipe.run(res_upper, **destination_config.run_kwargs)
    cause = pip_ex.value.__cause__
    assert isinstance(cause, DatabaseTerminalException)
    assert str(cause).startswith("Code: 47.")  # UNKNOWN_IDENTIFIER

    # fails when expression uses nullable column (test ClickHouse has `allow_nullable_key` disabled)
    @dlt.resource(columns={"timestamp": {"nullable": True}})
    def res_nullable():
        yield [{"timestamp": "2025-12-15T13:32:45Z"}]

    clickhouse_adapter(res_nullable, **{str(param): "toYYYYMMDD(timestamp)"})  # type: ignore[arg-type]
    pipe.drop_pending_packages()  # clear previous failed run
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipe.run(res_nullable, **destination_config.run_kwargs)
    cause = pip_ex.value.__cause__
    assert isinstance(cause, DatabaseTerminalException)
    assert str(cause).startswith("Code: 44.")  # ILLEGAL_COLUMN


def test_extract_column_names() -> None:
    assert extract_column_names("year") == {"year"}
    assert extract_column_names("(year)") == {"year"}
    assert extract_column_names("(year, month)") == {"year", "month"}
    assert extract_column_names("toYYYYMMDD(timestamp)") == {"timestamp"}
    assert extract_column_names("number % 4") == {"number"}
    assert extract_column_names("(upper(town), street)") == {"town", "street"}


def test_get_column_names_from_hint() -> None:
    # column sequence inputs
    assert get_column_names_from_table_hint(("year",)) == {"year"}
    assert get_column_names_from_table_hint(["year"]) == {"year"}
    assert get_column_names_from_table_hint(["year", "month"]) == {"year", "month"}

    # SQL expression input
    assert get_column_names_from_table_hint("(upper(town), street)") == {"town", "street"}


@pytest.mark.parametrize(
    "table_hint",
    (
        pytest.param(["c2", "c3"], id="seq"),
        pytest.param("(upper(c2), c3)", id="expr"),
    ),
)
def test_set_column_hints_from_table_hint(table_hint: TSQLExprOrColumnSeq) -> None:
    c1: TColumnSchema = {"name": "c1", "data_type": "text", "nullable": True, "sort": True}
    c2: TColumnSchema = {"name": "c2", "data_type": "text", "sort": False}
    c3: TColumnSchema = {"name": "c3", "data_type": "text", "nullable": True}

    columns: TTableSchemaColumns = {"c1": c1, "c2": c2, "c3": c3}

    set_column_hints_from_table_hint(columns, table_hint, hint_name="sort")

    # NOTE: see `set_column_hints` docstring for rules
    assert c1.get("sort") is None  # removed (rule 3)
    assert c2.get("sort") is True  # overridden (rule 1)
    assert c3.get("sort") is True  # set (rule 1)
    assert c1.get("nullable") is True  # retained (rule 4)
    assert c2.get("nullable") is False  # set (rule 2)
    assert c3.get("nullable") is True  # retained, despite being part of table hint (rule 4)
