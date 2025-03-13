# test the sql insert job loader, works only on duckdb for now

from typing import Any

import dlt

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.pipeline.utils import load_table_counts

from dlt.extract.hints import make_hints


def test_sql_job() -> None:
    # populate a table with 10 items and retrieve dataset
    pipeline = dlt.pipeline(
        pipeline_name="example_pipeline", destination="duckdb", dataset_name="example_dataset"
    )
    pipeline.run([{"a": i} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    # create a resource that generates sql statements to create 2 new tables
    @dlt.resource()
    def copied_table() -> Any:
        query = dataset["example_table"].limit(5).query()
        yield dlt.mark.with_hints(
            f"CREATE OR REPLACE TABLE copied_table AS {query}",
            make_hints(file_format="sql"),
        )

        query = dataset["example_table"].limit(7).query()
        yield dlt.mark.with_hints(
            f"CREATE OR REPLACE TABLE copied_table2 AS {query}",
            make_hints(file_format="sql"),
        )

    # run sql jobs
    pipeline.run(copied_table())

    # the two tables where created
    assert load_table_counts(pipeline, "example_table", "copied_table", "copied_table2") == {
        "example_table": 10,
        "copied_table": 5,
        "copied_table2": 7,
    }

    # we have a table entry for the main table "copied_table"
    assert "copied_table" in pipeline.default_schema.tables
    # but no columns, it's up to the user to provide a schema
    assert len(pipeline.default_schema.tables["copied_table"]["columns"]) == 0
