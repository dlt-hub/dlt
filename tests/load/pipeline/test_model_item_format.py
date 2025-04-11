import os
import io

from typing import Any
from unittest.mock import MagicMock
import pytest
import dlt

from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import make_hints, SqlModel

from tests.load.utils import count_job_types, destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.data_writers.writers import ModelWriter


from dlt.pipeline.exceptions import PipelineStepFailed

DESTINATIONS_SUPPORTING_MODEL = [
    "duckdb",
    # "athena", #TODO: might support with the iceberg table format
    "bigquery",
    "clickhouse",
    "databricks",
    "motherduck",
    "redshift",
    "snowflake",
    "sqlalchemy",
    "mssql",
    "postgres",
    # "dremio",
]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=DESTINATIONS_SUPPORTING_MODEL,
    ),
    ids=lambda x: x.name,
)
def test_simple_incremental(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_model_item_format", dev_mode=False)

    pipeline.run([{"a": i, "b": i + 1} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    select_dialect = pipeline.destination.capabilities().sqlglot_dialect

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # TODO: incremental is not supported for models yet
    @dlt.resource()
    def copied_table(incremental_field=dlt.sources.incremental("a")) -> Any:
        query = dataset["example_table"].limit(8).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

    with pytest.raises(PipelineStepFailed):
        pipeline.run([copied_table()])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=DESTINATIONS_SUPPORTING_MODEL,
    ),
    ids=lambda x: x.name,
)
def test_simple_model_jobs(destination_config: DestinationTestConfiguration) -> None:
    """
    Test creating SQL model jobs for various scenarios:
    - Copying a table without a specific column with a row limit.
    - Reversing the column order in the output table with a row limit.
    - Copying the entire table with a row limit.
    """
    # populate a table with two columns each with 10 items and retrieve dataset
    pipeline = destination_config.setup_pipeline("test_model_item_format", dev_mode=False)

    pipeline.run([{"a": i, "b": i + 1} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    # Retrieve the SQL dialect and schema information
    select_dialect = pipeline.destination.capabilities().sqlglot_dialect
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # Define resources for different SQL model jobs
    # We also need to supply all hints so the table can be created
    # Create a copied table without column "b"
    @dlt.resource()
    def copied_table_no_b() -> Any:
        query = dataset["example_table"][["a", "_dlt_load_id", "_dlt_id"]].limit(5).query()
        sql_model = SqlModel.from_query_string(query=query, dialect=select_dialect)
        yield dlt.mark.with_hints(
            sql_model,
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "b"}),
        )

    # Create a table with reversed column order
    @dlt.resource()
    def reversed_table() -> Any:
        query = dataset["example_table"][["_dlt_id", "_dlt_load_id", "b", "a"]].limit(7).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=dict(reversed(example_table_columns.items()))),
        )

    # Create a copied table with all columns
    @dlt.resource()
    def copied_table() -> Any:
        query = dataset["example_table"].limit(8).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

    # run sql jobs
    pipeline.run([copied_table_no_b(), reversed_table(), copied_table()])

    # Validate row counts for all tables
    assert load_table_counts(
        pipeline, "copied_table_no_b", "reversed_table", "copied_table", "example_table"
    ) == {
        "copied_table_no_b": 5,
        "reversed_table": 7,
        "copied_table": 8,
        "example_table": 10,
    }

    # Validate that all tables were created
    assert "copied_table_no_b" in pipeline.default_schema.tables
    assert "reversed_table" in pipeline.default_schema.tables
    assert "copied_table" in pipeline.default_schema.tables

    # Validate columns for the table without column "b"
    assert set(pipeline.default_schema.tables["copied_table_no_b"]["columns"].keys()) == {
        "a",
        "_dlt_id",
        "_dlt_load_id",
    }

    # Validate column order for the reversed table
    casefolder = pipeline.sql_client().capabilities.casefold_identifier
    reversed_insert_df = dataset["reversed_table"].df()
    expected_columns = [casefolder(key) for key in ["_dlt_id", "_dlt_load_id", "b", "a"]]
    actual_columns = list(reversed_insert_df.columns)
    assert (
        actual_columns == expected_columns
    ), f"Column mismatch: {actual_columns} != {expected_columns}"

    # Validate that the copied table includes all columns
    assert set(pipeline.default_schema.tables["copied_table"]["columns"].keys()) == {
        "a",
        "b",
        "_dlt_id",
        "_dlt_load_id",
    }

    # Validate that each table has exactly one model job
    assert count_job_types(pipeline) == {
        "copied_table_no_b": {"model": 1},
        "reversed_table": {"model": 1},
        "copied_table": {"model": 1},
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=DESTINATIONS_SUPPORTING_MODEL,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "write_disposition",
    ["merge", "replace", "append"],
    ids=lambda x: x,
)
def test_write_dispositions(
    destination_config: DestinationTestConfiguration, write_disposition: TWriteDisposition
) -> None:
    pipeline = destination_config.setup_pipeline("test_write_dispositions", dev_mode=True)

    pipeline.run(
        [{"a": i} for i in range(7)],
        primary_key="a",
        table_name="example_table_1",
        write_disposition=write_disposition,
    )
    pipeline.run(
        [{"a": i} for i in range(10)],
        primary_key="a",
        table_name="example_table_2",
        write_disposition=write_disposition,
    )

    # we now run a select of items 3-10 from example_table_2 into example_table_1
    # each w_d should have a different outcome
    dataset = pipeline.dataset()
    example_table_columns = dataset.schema.tables["example_table_1"]["columns"]
    # In Databricks, Ibis adds a helper column to emulate offset, causing a schema mismatch
    # when the query attempts to insert it. We explicitly select only the expected columns.
    relation = (
        dataset["example_table_2"].order_by("a").limit(7, offset=3)[example_table_columns.keys()]
    )
    query = relation.query()

    select_dialect = pipeline.destination.capabilities().sqlglot_dialect

    @dlt.resource(
        write_disposition=write_disposition, table_name="example_table_1", primary_key="a"
    )
    def copied_table() -> Any:
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

    pipeline.run([copied_table()])

    # Snowflake is typin sensitive
    if destination_config.destination_type == "snowflake":
        result_items = dataset["example_table_1"].df()["A"].tolist()
    else:
        result_items = dataset["example_table_1"].df()["a"].tolist()
    result_items.sort()

    if write_disposition == "merge":
        # tables merged
        assert result_items == list(range(10))
    elif write_disposition == "replace":
        # table fully replaced
        assert result_items == [3, 4, 5, 6, 7, 8, 9]
    elif write_disposition == "append":
        # the middle part is duplicated
        assert result_items == [0, 1, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 8, 9]
    else:
        raise ValueError(f"Unknown write disposition: {write_disposition}")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=DESTINATIONS_SUPPORTING_MODEL,
    ),
    ids=lambda x: x.name,
)
def test_multiple_statements_per_resource(destination_config: DestinationTestConfiguration) -> None:
    # Disable unique indexing for postgres, otherwise there will be a not null constraint error
    # because we're copying from the same table
    if destination_config.destination_type == "postgres":
        os.environ["DESTINATION__POSTGRES__CREATE_INDEXES"] = "false"

    pipeline = destination_config.setup_pipeline(
        "test_multiple_statments_per_resource", dev_mode=False
    )

    pipeline.run([{"a": i} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    select_dialect = pipeline.destination.capabilities().sqlglot_dialect

    # create a resource that generates sql statements to create 2 new tables
    # we also need to supply all hints so the table can be created
    @dlt.resource()
    def copied_table() -> Any:
        query1 = dataset["example_table"].limit(5).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query1, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

        query2 = dataset["example_table"].limit(7).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query2, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

    pipeline.run([copied_table()])

    assert load_table_counts(pipeline, "copied_table", "example_table") == {
        "copied_table": 12,
        "example_table": 10,
    }

    # two model jobs where produced
    assert count_job_types(pipeline) == {
        "copied_table": {"model": 2},
    }


def test_model_writer_without_destination(mocker):
    """
    Test the `ModelWriter` class without passing destination capabilities (`_caps`) to ensure:
    - The `write_data` method processes items correctly.
    - The `items_count` is updated accurately.
    - The writer works fine at the pipeline level without any destination set.
    """
    writer_spy = mocker.spy(ModelWriter, "write_data")

    mock_file = io.StringIO()
    writer = ModelWriter(mock_file)

    mock_item = [
        MagicMock(dialect=None, query="SELECT * FROM test_table"),
        MagicMock(dialect="mysql", query="SELECT id, name FROM users"),
    ]

    writer.write_data(mock_item)

    writer_spy.assert_called_once_with(writer, mock_item)

    written_content = mock_file.getvalue()
    assert "dialect: \n" in written_content
    assert "SELECT * FROM test_table" in written_content
    assert "dialect: mysql" in written_content
    assert "SELECT id, name FROM users" in written_content

    assert writer.items_count == len(mock_item)

    # Test the writer at the pipeline level to ensure it works without destination
    @dlt.resource
    def example_table() -> Any:
        query = 'SELECT * FROM "test_model_writer_without_destination"."example_table"'
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=None),
            hints=make_hints(),
        )

    pipeline = dlt.pipeline(pipeline_name="test_model_writer_without_destination")
    try:
        pipeline.extract(example_table)
    except Exception as e:
        pytest.fail(f"pipeline.extract(example_table) raised an exception: {e}")
