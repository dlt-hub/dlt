import pytest
import dlt
import os

from typing import List
from functools import reduce

from tests.load.utils import destinations_configs, DestinationTestConfiguration, AZ_BUCKET
from pandas import DataFrame


@dlt.source()
def source():
    @dlt.resource()
    def items():
        yield from [{"id": i, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(300)]

    @dlt.resource()
    def items2():
        yield from [{"id": i, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(150)]

    return [items, items2]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces_sql(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )

    # run source
    s = source()
    pipeline.run(
        s,
    )

    # get one df
    df = pipeline.dataset.df(table="items", batch_size=5)
    assert len(df.index) == 5
    assert set(df.columns.values) == {"id", "_dlt_load_id", "_dlt_id"}

    # iterate all dataframes
    frames = []
    for df in pipeline.dataset.iter_df(table="items", batch_size=70):
        frames.append(df)

    # check frame amount and items counts
    assert len(frames) == 5
    assert [len(df.index) for df in frames] == [70, 70, 70, 70, 20]

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f["id"].to_list() for f in frames])
    assert set(ids) == set(range(300))

    # basic check of arrow table
    table = pipeline.dataset.arrow(table="items", batch_size=5)
    assert set(table.column_names) == {"id", "_dlt_load_id", "_dlt_id"}
    assert table.num_rows == 5


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[AZ_BUCKET],
    ),  # TODO: make AZ work
    ids=lambda x: x.name,
)
def test_read_interfaces_filesystem(destination_config: DestinationTestConfiguration) -> None:
    # we force multiple files per table, they may only hold 50 items
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "50"

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    # get one df
    df = pipeline.dataset.df(table="items", batch_size=5)
    assert len(df.index) == 5
    assert set(df.columns.values) == {"id", "_dlt_load_id", "_dlt_id"}

    # iterate all dataframes
    frames = []
    for df in pipeline.dataset.iter_df(table="items", batch_size=70):
        frames.append(df)

    # check frame amount and items counts
    assert len(frames) == 5
    assert [len(df.index) for df in frames] == [70, 70, 70, 70, 20]

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f["id"].to_list() for f in frames])
    assert set(ids) == set(range(300))

    # basic check of arrow table
    table = pipeline.dataset.arrow(table="items", batch_size=5)
    assert set(table.column_names) == {"id", "_dlt_load_id", "_dlt_id"}
    assert table.num_rows == 5
