import pytest
import datetime  # noqa: I251
from typing import Iterator, Any

import dlt, os
from dlt.common import pendulum
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.cases import table_update_and_row, assert_all_data_types_row
from tests.pipeline.utils import assert_load_info, load_table_counts
from tests.pipeline.utils import load_table_counts
from dlt.destinations.exceptions import CantExtractTablePrefix
from dlt.destinations.adapters import athena_partition, athena_adapter

from tests.load.utils import (
    TEST_FILE_LAYOUTS,
    FILE_LAYOUT_MANY_TABLES_ONE_FOLDER,
    FILE_LAYOUT_CLASSIC,
    FILE_LAYOUT_TABLE_NOT_FIRST,
    destinations_configs,
    DestinationTestConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["athena"]),
    ids=lambda x: x.name,
)
def test_athena_destinations(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("athena_" + uniq_id(), dev_mode=True)

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    pipeline.run(items, **destination_config.run_kwargs)

    # see if we have athena tables with items
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    # load again with schema evloution
    @dlt.resource(name="items", write_disposition="append")
    def items2():
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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["athena"]),
    ids=lambda x: x.name,
)
def test_athena_all_datatypes_and_timestamps(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline("athena_" + uniq_id(), dev_mode=True)

    # TIME is not supported
    column_schemas, data_types = table_update_and_row(exclude_types=["time"])

    # apply the exact columns definitions so we process json and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource() -> Iterator[Any]:
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source() -> Any:
        return my_resource

    info = pipeline.run(my_source(), **destination_config.run_kwargs)
    assert_load_info(info)

    with pipeline.sql_client() as sql_client:
        db_rows = sql_client.execute_sql("SELECT * FROM data_types")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # content must equal
        assert_all_data_types_row(
            db_row[:-2],
            parse_json_strings=True,
            timestamp_precision=sql_client.capabilities.timestamp_precision,
            schema=column_schemas,
        )

        # now let's query the data with timestamps and dates.
        # https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html#engine-versions-reference-0003-timestamp-changes

        # use string representation TIMESTAMP(2)
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = TIMESTAMP '2022-05-23 13:26:45.176'"
        )
        assert len(db_rows) == 10
        # no rows - TIMESTAMP(6) not supported
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = TIMESTAMP '2022-05-23 13:26:45.176145'"
        )
        assert len(db_rows) == 0
        # use pendulum
        # that will pass
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = %s",
            pendulum.datetime(2022, 5, 23, 13, 26, 45, 176000),
        )
        assert len(db_rows) == 10
        # that will return empty list
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = %s",
            pendulum.datetime(2022, 5, 23, 13, 26, 45, 176145),
        )
        assert len(db_rows) == 0

        # use datetime
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = %s",
            datetime.datetime(2022, 5, 23, 13, 26, 45, 176000),
        )
        assert len(db_rows) == 10
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col4 = %s",
            datetime.datetime(2022, 5, 23, 13, 26, 45, 176145),
        )
        assert len(db_rows) == 0

        # check date
        db_rows = sql_client.execute_sql("SELECT * FROM data_types WHERE col10 = DATE '2023-02-27'")
        assert len(db_rows) == 10
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col10 = %s", pendulum.date(2023, 2, 27)
        )
        assert len(db_rows) == 10
        db_rows = sql_client.execute_sql(
            "SELECT * FROM data_types WHERE col10 = %s", datetime.date(2023, 2, 27)
        )
        assert len(db_rows) == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["athena"]),
    ids=lambda x: x.name,
)
def test_athena_blocks_time_column(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("athena_" + uniq_id(), dev_mode=True)

    column_schemas, data_types = table_update_and_row()

    # apply the exact columns definitions so we process json and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource() -> Iterator[Any]:
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source() -> Any:
        return my_resource

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source(), **destination_config.run_kwargs)
    assert isinstance(pip_ex.value.__cause__, UnsupportedDataType)
    assert pip_ex.value.__cause__.data_type == "time"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["athena"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_athena_file_layouts(destination_config: DestinationTestConfiguration, layout) -> None:
    # test wether strange file layouts still work in all staging configs
    pipeline = destination_config.setup_pipeline("athena_file_layout", dev_mode=True)
    os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout

    resources = [
        dlt.resource([1, 2, 3], name="items1"),
        dlt.resource([1, 2, 3, 4, 5, 6, 7], name="items2"),
    ]

    # layouts that should not work should raise exception
    if layout in [
        FILE_LAYOUT_CLASSIC,  # table not in own folder
        FILE_LAYOUT_MANY_TABLES_ONE_FOLDER,  # table not in own folder
        FILE_LAYOUT_TABLE_NOT_FIRST,  # table not the first variable
    ]:
        with pytest.raises(CantExtractTablePrefix):
            pipeline.run(resources, **destination_config.run_kwargs)
        return

    info = pipeline.run(resources, **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts == {"items1": 3, "items2": 7}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["athena"], with_table_format="iceberg"),
    ids=lambda x: x.name,
)
def test_athena_partitioned_iceberg_table(destination_config: DestinationTestConfiguration):
    """Load an iceberg table with partition hints and verify partitions are created correctly."""
    pipeline = destination_config.setup_pipeline("athena_" + uniq_id(), dev_mode=True)

    data_items = [
        (1, "A", datetime.date.fromisoformat("2021-01-01")),
        (2, "A", datetime.date.fromisoformat("2021-01-02")),
        (3, "A", datetime.date.fromisoformat("2021-01-03")),
        (4, "A", datetime.date.fromisoformat("2021-02-01")),
        (5, "A", datetime.date.fromisoformat("2021-02-02")),
        (6, "B", datetime.date.fromisoformat("2021-01-01")),
        (7, "B", datetime.date.fromisoformat("2021-01-02")),
        (8, "B", datetime.date.fromisoformat("2021-01-03")),
        (9, "B", datetime.date.fromisoformat("2021-02-01")),
        (10, "B", datetime.date.fromisoformat("2021-03-02")),
    ]

    @dlt.resource(table_format="iceberg")
    def partitioned_table():
        yield [{"id": i, "category": c, "created_at": d} for i, c, d in data_items]

    athena_adapter(
        partitioned_table,
        partition=[
            "category",
            athena_partition.month("created_at"),
        ],
    )

    info = pipeline.run(partitioned_table, **destination_config.run_kwargs)
    assert_load_info(info)

    # Get partitions from metadata
    with pipeline.sql_client() as sql_client:
        tbl_name = sql_client.make_qualified_table_name("partitioned_table$partitions")
        rows = sql_client.execute_sql(f"SELECT partition FROM {tbl_name}")
        partition_keys = {r[0] for r in rows}

        data_rows = sql_client.execute_sql(
            "SELECT id, category, created_at FROM"
            f" {sql_client.make_qualified_table_name('partitioned_table')}"
        )
        # data_rows = [(i, c, d.toisoformat()) for i, c, d in data_rows]

    # All data is in table
    assert len(data_rows) == len(data_items)
    assert set(data_rows) == set(data_items)

    # Compare with expected partitions
    # Months are number of months since epoch
    expected_partitions = {
        "{category=A, created_at_month=612}",
        "{category=A, created_at_month=613}",
        "{category=B, created_at_month=612}",
        "{category=B, created_at_month=613}",
        "{category=B, created_at_month=614}",
    }

    assert partition_keys == expected_partitions
