# test the sql insert job loader, works only on duckdb for now

from typing import Any

import dlt

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import make_hints, ModelStr


# TODO: use destination config and only select duckdb for now
def test_simple_model_jobs() -> None:
    # populate a table with 10 items and retrieve dataset
    pipeline = dlt.pipeline(
        pipeline_name="example_pipeline", destination="duckdb", dataset_name="example_dataset"
    )
    pipeline.run([{"a": i} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # create a resource that generates sql statements to create 2 new tables
    # we also need to supply all hints so the table can be created
    @dlt.resource()
    def copied_table() -> Any:
        query = dataset["example_table"].limit(5).query()
        yield dlt.mark.with_hints(ModelStr(query), hints=make_hints(columns=example_table_columns))

    @dlt.resource()
    def copied_table_2() -> Any:
        query = dataset["example_table"].limit(7).query()
        yield dlt.mark.with_hints(ModelStr(query), hints=make_hints(columns=example_table_columns))

    # run sql jobs
    pipeline.run([copied_table(), copied_table_2()])

    # the two tables where created
    assert load_table_counts(pipeline, "copied_table", "copied_table_2", "example_table") == {
        "copied_table": 5,
        "copied_table_2": 7,
        "example_table": 10,
    }

    # we have a table entry for the main table "copied_table"
    assert "copied_table" in pipeline.default_schema.tables
    # and we only have the three columns from the original table
    assert set(pipeline.default_schema.tables["copied_table"]["columns"].keys()) == {
        "a",
        "_dlt_id",
        "_dlt_load_id",
    }


def test_write_dispositions() -> None:
    # TODO: test wether models are written into the correct tables (sometimes staging tables) and
    # wether the merge and replace strategies are applied correctly
    pass


def test_insert_less_columns() -> None:
    # TODO: test what happens if the selected schema is a subset of the target table schema
    # maybe it works, maybe it won't...
    pass


def test_multiple_statments_per_resource() -> None:
    # TODO: test what happens if a resource yields multiple statements
    # they should all be executed and each should produce its own file
    # after extraction
    pass
