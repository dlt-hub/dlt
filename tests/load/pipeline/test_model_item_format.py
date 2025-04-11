# test the sql insert job loader, works only on duckdb for now
import os

from typing import Any
import pytest
import dlt

from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import make_hints, SqlModel
from tests.load.utils import count_job_types, destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info
from dlt.common.schema.typing import TWriteDisposition

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
    # populate a table with 10 items and retrieve dataset
    pipeline = destination_config.setup_pipeline("test_model_item_format", dev_mode=False)

    pipeline.run([{"a": i, "b": i + 1} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    select_dialect = pipeline.destination.capabilities().sqlglot_dialect

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # create a resource that generates sql statements to create 2 new tables
    # we also need to supply all hints so the table can be created
    @dlt.resource()
    def copied_table() -> Any:
        query = dataset["example_table"][["a", "_dlt_load_id", "_dlt_id"]].limit(5).query()
        sql_model = SqlModel.from_query_string(query=query, dialect=select_dialect)
        yield dlt.mark.with_hints(
            sql_model,
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "b"}),
        )

    @dlt.resource()
    def copied_table_2() -> Any:
        query = dataset["example_table"][["b", "_dlt_load_id", "_dlt_id"]].limit(7).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "a"}),
        )

    @dlt.resource()
    def copied_table_3() -> Any:
        query = dataset["example_table"].limit(8).query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=example_table_columns),
        )

    # run sql jobs
    pipeline.run([copied_table(), copied_table_2(), copied_table_3()])

    # the two tables where created
    assert load_table_counts(
        pipeline, "copied_table", "copied_table_2", "copied_table_3", "example_table"
    ) == {
        "copied_table": 5,
        "copied_table_2": 7,
        "copied_table_3": 8,
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

    # we have a model job each
    assert count_job_types(pipeline) == {
        "copied_table": {"model": 1},
        "copied_table_2": {"model": 1},
        "copied_table_3": {"model": 1},
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
def test_insert_less_or_reversed_columns(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_insert_less_columns", dev_mode=False)

    pipeline.run(
        [{"a": 1, "b": "n", "c": True}, {"a": None, "b": "n", "c": False}],
        table_name="example_table",
    )
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    select_dialect = pipeline.destination.capabilities().sqlglot_dialect

    # Test a model corresponding to a partial table without column "a"
    partial_table_column_keys = [key for key in example_table_columns.keys() if key != "a"]

    @dlt.resource()
    def partial_insert() -> Any:
        relation = dataset["example_table"][partial_table_column_keys]
        query = relation.query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "a"}),
        )

    load_info = pipeline.run([partial_insert()])
    assert_load_info(load_info)

    partial_insert_df = dataset["partial_insert"].df()

    casefolder = pipeline.sql_client().capabilities.casefold_identifier

    expected_columns = [casefolder(key) for key in partial_table_column_keys]
    actual_columns = list(partial_insert_df.columns)

    # Check names and order
    assert (
        actual_columns == expected_columns
    ), f"Column mismatch: {actual_columns} != {expected_columns}"

    # Test a model corresponding to a table without reversed
    reversed_table_column_keys = list(example_table_columns.keys())[::-1]

    @dlt.resource()
    def reversed_insert() -> Any:
        relation = dataset["example_table"][reversed_table_column_keys]
        query = relation.query()
        yield dlt.mark.with_hints(
            SqlModel.from_query_string(query=query, dialect=select_dialect),
            hints=make_hints(columns=dict(reversed(example_table_columns.items()))),
        )

    load_info = pipeline.run([reversed_insert()])
    assert_load_info(load_info)

    reversed_insert_df = dataset["reversed_insert"].df()

    expected_columns = [casefolder(key) for key in reversed_table_column_keys]

    actual_columns = list(reversed_insert_df.columns)

    # Check names and order
    assert (
        actual_columns == expected_columns
    ), f"Column mismatch: {actual_columns} != {expected_columns}"


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
