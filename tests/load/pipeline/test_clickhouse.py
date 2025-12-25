from typing import Any, Iterator, Literal, cast

import pytest

import dlt
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.schema.schema import Schema
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.destinations.adapters import clickhouse_adapter
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseUndefinedRelation
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import TSQLExprOrColumnSeq
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.clickhouse.utils import (
    CLICKHOUSE_ADAPTER_CASES,
    CLICKHOUSE_ADAPTER_SETTINGS_CASE,
    clickhouse_adapter_resource,
    get_create_table_query,
    get_partition_key,
    get_sorting_key,
)
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.utils import TEST_STORAGE_ROOT
from tests.pipeline.utils import assert_load_info, load_table_counts


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_staging_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_destination_append(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"clickhouse_{uniq_id()}", dev_mode=True)

    try:

        @dlt.resource(name="items", write_disposition="append")
        def items() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "sub_items": [
                    {"id": 101, "name": "sub item 101"},
                    {"id": 101, "name": "sub item 102"},
                ],
            }

        pipeline.run(
            items,
            **destination_config.run_kwargs,
            staging=destination_config.staging,
        )

        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
        )
        assert table_counts["items"] == 1
        assert table_counts["items__sub_items"] == 2
        assert table_counts["_dlt_loads"] == 1

        # Load again with schema evolution.
        @dlt.resource(name="items", write_disposition="append")
        def items2() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "new_field": "hello",
                "sub_items": [
                    {
                        "id": 101,
                        "name": "sub item 101",
                        "other_new_field": "hello 101",
                    },
                    {
                        "id": 101,
                        "name": "sub item 102",
                        "other_new_field": "hello 102",
                    },
                ],
            }

        pipeline.run(items2, **destination_config.run_kwargs)
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
        )
        assert table_counts["items"] == 2
        assert table_counts["items__sub_items"] == 4
        assert table_counts["_dlt_loads"] == 2

    except Exception as e:
        raise e

    finally:
        with pipeline.sql_client() as client:
            client.drop_dataset()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_no_dataset_name(destination_config: DestinationTestConfiguration) -> None:
    # add staging to cover staging dataset name that must be present
    destination_config.staging = dlt.destinations.filesystem(TEST_STORAGE_ROOT)
    # create explicitly empty dataset
    # NOTE: we use empty string here but when creating pipeline directly you can just skip
    # the dataset_name argument
    pipeline = destination_config.setup_pipeline("no_dataset_name_items", dataset_name="")
    # run only on localhost because we have a common dataset for all tests that may run in parallel
    dest_client, staging_client = pipeline._get_destination_clients(Schema("test"))

    # make sure staging has dataset name which is pipeline default
    assert isinstance(staging_client.config, DestinationClientDwhConfiguration)
    assert staging_client.config.dataset_name == pipeline._make_dataset_name(None, pipeline.staging)

    assert isinstance(dest_client.config, DestinationClientDwhConfiguration)
    print(dest_client.config.dataset_name)
    assert dest_client.config.dataset_name == ""

    if dest_client.config.credentials.host != "localhost":  # type: ignore[attr-defined]
        pytest.skip("Empty dataset may be tested only on a localhost clickhouse")

    @dlt.resource(name="items", write_disposition="merge", primary_key="id")
    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    info = pipeline.run([items], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    # test drop storage, first create additional table and make sure it was not destroyed
    pipeline_2 = destination_config.setup_pipeline("no_dataset_name_events", dataset_name="")
    info = pipeline_2.run([items.with_name("events")], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts_2 = load_table_counts(
        pipeline_2, *[t["name"] for t in pipeline_2.default_schema._schema_tables.values()]
    )
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2
    assert table_counts_2["_dlt_loads"] == 2

    with pipeline.destination_client() as dest_client:
        assert dest_client.is_storage_initialized()
        dest_client.drop_storage()
        assert not dest_client.is_storage_initialized()

    # events are still there
    table_counts_2 = load_table_counts(
        pipeline_2, *[t for t in pipeline_2.default_schema.data_table_names()]
    )
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2

    # all other tables got dropped
    with pytest.raises(DatabaseUndefinedRelation):
        table_counts_2 = load_table_counts(
            pipeline_2, *[t for t in pipeline_2.default_schema.dlt_table_names()]
        )

    # load again
    info = pipeline_2.run([items.with_name("events")], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts_2 = load_table_counts(
        pipeline_2, *[t["name"] for t in pipeline_2.default_schema._schema_tables.values()]
    )
    # merge
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2
    # table got dropped so we have 1 load
    assert table_counts_2["_dlt_loads"] == 1


# NOTE: if you update `test_clickhouse_adapter_sort_pipe`, check if the equivalent
# `test_clickhouse_adapter_partition_pipe` should also be updated
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "sort, _expected_order_by_clause, expected_sorting_key", CLICKHOUSE_ADAPTER_CASES
)
def test_clickhouse_adapter_sort_pipe(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
    sort: TSQLExprOrColumnSeq,
    _expected_order_by_clause,
    expected_sorting_key: str,
) -> None:
    sorted_res = clickhouse_adapter(clickhouse_adapter_resource, sort=sort)
    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_sort", dev_mode=True)
    pipe.run(sorted_res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    sorting_key = get_sorting_key(sql_client, table_name=sorted_res.name)
    assert sorting_key == expected_sorting_key


# NOTE: if you update `test_clickhouse_adapter_partition_pipe`, check if the equivalent
# `test_clickhouse_adapter_sort_pipe` should also be updated
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "partition, _expected_partition_by_clause, expected_partition_key",
    CLICKHOUSE_ADAPTER_CASES,
)
def test_clickhouse_adapter_partition_pipe(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
    partition: TSQLExprOrColumnSeq,
    _expected_partition_by_clause,
    expected_partition_key: str,
) -> None:
    partitioned_res = clickhouse_adapter(clickhouse_adapter_resource, partition=partition)
    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_partition", dev_mode=True)
    pipe.run(partitioned_res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    partition_key = get_partition_key(sql_client, table_name=partitioned_res.name)
    assert partition_key == expected_partition_key


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_adapter_settings_pipe(
    destination_config: DestinationTestConfiguration,
    clickhouse_adapter_resource: DltResource,
) -> None:
    settings, expected_settings_clause = CLICKHOUSE_ADAPTER_SETTINGS_CASE
    adapted_res = clickhouse_adapter(clickhouse_adapter_resource, settings=settings)
    pipe = destination_config.setup_pipeline("test_clickhouse_adapter_settings", dev_mode=True)
    pipe.run(adapted_res, **destination_config.run_kwargs, refresh="drop_sources")
    sql_client = cast(ClickHouseSqlClient, pipe.sql_client())
    create_table_query = get_create_table_query(sql_client, table_name=adapted_res.name)
    assert f"SETTINGS {expected_settings_clause}" in create_table_query


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

    # fails when expression uses nullable column
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
