import pytest
import dlt

from typing import List
from functools import reduce

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from pandas import DataFrame


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces(destination_config: DestinationTestConfiguration) -> None:
    # we load a table with child table and check wether ibis works
    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )

    @dlt.source()
    def source():
        @dlt.resource()
        def items():
            yield from [
                {"id": i, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(300)
            ]

        @dlt.resource()
        def items2():
            yield from [
                {"id": i, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(150)
            ]

        return [items, items2]

    # create 300 entries in "items" table
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
    table.num_rows == 5

    # access via resource
    len(s.items.dataset.df().index) == 300
    len(s.items2.dataset.df().index) == 150
