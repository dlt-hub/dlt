# test the sql insert job loader, works only on duckdb for now

from typing import Any
import pytest
import dlt

from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import make_hints, ModelStr
from tests.load.utils import count_job_types, destinations_configs, DestinationTestConfiguration
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
    "dremio",
]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        exclude=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_simple_model_jobs(destination_config: DestinationTestConfiguration) -> None:
    # populate a table with 10 items and retrieve dataset
    pipeline = destination_config.setup_pipeline("test_model_item_format", dev_mode=False)

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

    # we have a model job each
    assert count_job_types(pipeline) == {
        "copied_table": {"model": 1},
        "copied_table_2": {"model": 1},
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        exclude=["athena"],
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

    @dlt.resource(
        write_disposition=write_disposition, table_name="example_table_1", primary_key="a"
    )
    def copied_table() -> Any:
        yield dlt.mark.with_hints(ModelStr(query), hints=make_hints(columns=example_table_columns))

    pipeline.run([copied_table()])

    # Snowflake is case sensitive
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
        #        subset=["sqlalchemy"],
        exclude=["athena", "mssql", "dremio", "postgres", "synapse"],
    ),
    ids=lambda x: x.name,
)
def test_insert_less_or_reversed_columns(destination_config: DestinationTestConfiguration) -> None:
    # NOTE: at least for duckdb, the column count AND order of the select query must match
    # the target table schema
    pipeline = destination_config.setup_pipeline("test_insert_less_columns", dev_mode=False)

    pipeline.run(
        [{"a": 1, "b": "n", "c": True}, {"a": None, "b": "n", "c": False}],
        table_name="example_table",
    )
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    print(example_table_columns)

    # This should raise a column number mismatch error
    @dlt.resource()
    def partial_insert() -> Any:
        partial_table_column_keys = [key for key in example_table_columns.keys() if key != "a"]
        relation = dataset["example_table"][partial_table_column_keys]
        query = relation.query()
        yield dlt.mark.with_hints(ModelStr(query), hints=make_hints(columns=example_table_columns))

    with pytest.raises(PipelineStepFailed):
        pipeline.run([partial_insert()])

    # This should raise a type mismatch.
    # SQLite (via SQLAlchemy) is lenient with types, so instead of a type error,
    # we get a NOT NULL constraint violation when inserting None from "a" into "_dlt_id".
    @dlt.resource()
    def reversed_insert() -> Any:
        reversed_table_column_keys = list(example_table_columns.keys())[::-1]
        relation = dataset["example_table"][reversed_table_column_keys]
        query = relation.query()
        yield dlt.mark.with_hints(ModelStr(query), hints=make_hints(columns=example_table_columns))

    print(example_table_columns)

    with pytest.raises(PipelineStepFailed):
        pipeline.run([reversed_insert()])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        exclude=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_multiple_statements_per_resource(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "test_multiple_statments_per_resource", dev_mode=False
    )

    pipeline.run([{"a": i} for i in range(10)], table_name="example_table")
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # create a resource that generates sql statements to create 2 new tables
    # we also need to supply all hints so the table can be created
    @dlt.resource()
    def copied_table() -> Any:
        query1 = dataset["example_table"].limit(5).query()
        yield dlt.mark.with_hints(ModelStr(query1), hints=make_hints(columns=example_table_columns))

        query2 = dataset["example_table"].limit(7).query()
        yield dlt.mark.with_hints(ModelStr(query2), hints=make_hints(columns=example_table_columns))

    pipeline.run([copied_table()])

    assert load_table_counts(pipeline, "copied_table", "example_table") == {
        "copied_table": 12,
        "example_table": 10,
    }

    # two model jobs where produced
    assert count_job_types(pipeline) == {
        "copied_table": {"model": 2},
    }
