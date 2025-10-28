import os
import io

from typing import Any, List
from unittest.mock import MagicMock
import pytest
import dlt

from dlt.extract.hints import make_hints

from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.load.exceptions import LoadClientJobException

from dlt.common.data_writers.writers import ModelWriter
from dlt.common.schema.typing import TWriteDisposition, TDataType
from dlt.common.schema.exceptions import DataValidationError
from dlt.common.utils import uniq_id

from tests.cases import table_update_and_row, assert_all_data_types_row

from tests.load.utils import (
    count_job_types,
    destinations_configs,
    DestinationTestConfiguration,
    table_update_and_row_for_destination,
)
from tests.utils import preserve_environ
from tests.pipeline.utils import assert_load_info, load_tables_to_dicts, load_table_counts

import sqlglot

DESTINATIONS_SUPPORTING_MODEL = [
    "duckdb",
    "athena",  # with iceberg table format
    "bigquery",
    "clickhouse",
    "databricks",
    "motherduck",
    "redshift",
    "snowflake",
    "sqlalchemy",
    "mssql",
    "postgres",
    "synapse",
    "dremio",
    "ducklake",
]

# Get config with iceberg table format if supported
destination_configs = [
    config
    for dest in DESTINATIONS_SUPPORTING_MODEL
    for config in (
        destinations_configs(default_sql_configs=True, subset=[dest], with_table_format="iceberg")
        if dest == "athena"
        else destinations_configs(default_sql_configs=True, subset=[dest])
    )
]


UNSUPPORTED_MODEL_QUERIES = [
    "DELETE FROM users WHERE id = 1",
    "INSERT INTO users (id, name) VALUES (1, 'Alice')",
    "UPDATE users SET name = 'Bob' WHERE id = 1",
    "CREATE TABLE users (id INTEGER, name TEXT)",
    "DROP TABLE users",
    "TRUNCATE TABLE users",
]


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_simple_incremental(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_simple_incremental", dev_mode=True)

    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # TODO: incremental is not supported for models yet
    @dlt.resource()
    def copied_table(incremental_field=dlt.sources.incremental("a")) -> Any:
        rel = dataset["example_table"].limit(8)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns=example_table_columns),
        )

    with pytest.raises(PipelineStepFailed):
        pipeline.run([copied_table()])


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_aliased_column(destination_config: DestinationTestConfiguration) -> None:
    """
    Test that a column in a SQL query can be aliased correctly and processed by the pipeline.
    Specifically, this test ensures the resulting table contains the aliased column with the correct data.
    """
    pipeline = destination_config.setup_pipeline("test_aliased_column", dev_mode=True)
    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )

    dataset = pipeline.dataset()
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # Define a resource that aliases column "a" as "b"
    @dlt.resource()
    def copied_table_with_a_as_b() -> Any:
        rel = dataset("SELECT a as b, _dlt_load_id, _dlt_id FROM example_table")
        # parsed = rel._qualified_query

        # # sqlglot.parse_one(query, read=select_dialect)
        # # Get first expression in the SELECT statement (e.g "a")
        # first_expr = parsed.expressions[0]
        # # Clickhouse aliases by default, so special handling is needed
        # if isinstance(first_expr, sqlglot.exp.Alias):
        #     original_expr = first_expr.this
        # else:
        #     original_expr = first_expr
        # # Wrap the first expression with an alias: "a AS b"
        # parsed.expressions[0] = sqlglot.exp.Alias(this=original_expr, alias="b")
        # # Convert back to an SQL
        # query = rel.to_sql()
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "a"}),
        )

    pipeline.run(
        [copied_table_with_a_as_b()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    assert load_table_counts(pipeline, "copied_table_with_a_as_b", "example_table") == {
        "copied_table_with_a_as_b": 10,
        "example_table": 10,
    }

    assert set(pipeline.default_schema.tables["copied_table_with_a_as_b"]["columns"].keys()) == {
        "b",
        "_dlt_id",
        "_dlt_load_id",
    }

    # The sum of "b" should match the sum of the original "a"
    result_df = dataset["copied_table_with_a_as_b"].df()
    assert result_df["b"].sum() == sum(i for i in range(10))


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_simple_model_jobs(
    destination_config: DestinationTestConfiguration,
) -> None:
    """
    Test creating SQL model jobs for various scenarios:
    - Copying a table using a query without a specific column ("b") which will be added as null by the normalizer.
    - Copying a table using a query with reversed select order which will be reordered by the normalizer.
    """
    # populate a table with two columns each with 10 items and retrieve dataset
    pipeline = destination_config.setup_pipeline("test_simple_model_jobs", dev_mode=True)

    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )
    dataset = pipeline.dataset()

    # Retrieve the SQL dialect and schema information
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # Define a resource for a SQL model that excludes column "b" and "_dlt_id" from the query
    # The normalizer will add "b" as null since it is included in the schema hints,
    # without including "_dlt_id" into the schema and insert statement
    # because the addition of the column "_dlt_id" is disabled by default
    @dlt.resource()
    def model_with_no_b() -> Any:
        rel = dataset["example_table"][["a", "_dlt_load_id"]].order_by("a").limit(5)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(
                columns={k: v for k, v in example_table_columns.items() if k != "_dlt_id"}
            ),
        )

    # Define a resource for a SQL model that reverses the column order in the query
    # The normalizer will reorder the columns to match the schema's order,
    # add "_dlt_load_id" into the schema and as a constant value to the insert statement
    # because the addition of the column "_dlt_load_id" is enabled by default
    @dlt.resource()
    def model_reversed_select() -> Any:
        rel = dataset["example_table"][["_dlt_id", "b", "a"]].order_by("a").limit(7)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(
                columns={k: v for k, v in example_table_columns.items() if k != "_dlt_load_id"}
            ),
        )

    # Run model jobs
    load_info = pipeline.run(
        [model_with_no_b(), model_reversed_select()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    # Validate row counts for all tables
    assert load_table_counts(
        pipeline, "model_with_no_b", "model_reversed_select", "example_table"
    ) == {
        "model_with_no_b": 5,
        "model_reversed_select": 7,
        "example_table": 10,
    }

    # Validate that all tables were created
    assert "model_with_no_b" in pipeline.default_schema.tables
    assert "model_reversed_select" in pipeline.default_schema.tables

    # Validate that the table "model_with_no_b" includes all columns in the schema
    # except for "_dlt_id"
    assert set(pipeline.default_schema.tables["model_with_no_b"]["columns"].keys()) == {
        "a",
        "b",
        "_dlt_load_id",
    }

    # Validate that the table "model_reversed_select" includes all columns in the schema
    assert set(pipeline.default_schema.tables["model_reversed_select"]["columns"].keys()) == {
        "a",
        "b",
        "_dlt_id",
        "_dlt_load_id",
    }

    # Validate results in the "model_with_no_b" table,
    # making sure column b is empty
    # and _dlt_id was created anew
    model_with_no_b_df = dataset["model_with_no_b"].df()
    assert set([0, 1, 2, 3, 4]) == set(model_with_no_b_df["a"].to_list())
    assert [] == model_with_no_b_df["b"].dropna().to_list()

    # Validate the column order in the table created with a query with reversed column order,
    # ensuring _dlt_load_id was added and created anew
    model_reversed_select_df = dataset["model_reversed_select"].df()
    expected_columns = ["a", "b", "_dlt_id", "_dlt_load_id"]
    actual_columns = list(model_reversed_select_df.columns)
    assert (
        actual_columns == expected_columns
    ), f"Column mismatch: {actual_columns} != {expected_columns}"
    assert len(set(model_reversed_select_df["_dlt_load_id"])) == 1
    assert set(model_reversed_select_df["_dlt_load_id"]).pop() == load_info.loads_ids[0]

    # Validate that each table has exactly one model job
    if destination_config.destination_type == "athena":
        assert count_job_types(pipeline) == {
            "model_with_no_b": {"model": 1, "sql": 1},
            "model_reversed_select": {"model": 1, "sql": 1},
        }
    else:
        assert count_job_types(pipeline) == {
            "model_with_no_b": {"model": 1},
            "model_reversed_select": {"model": 1},
        }


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_model_from_two_tables(destination_config: DestinationTestConfiguration, preserve_environ):
    # adding dlt id is disabled by default, so we set it to true
    # because here we insert to a pre-existing table "merged_table" for which "_dlt_id" column is present
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(True)

    pipeline = destination_config.setup_pipeline("test_model_from_two_tables", dev_mode=True)

    pipeline.run(
        [{"a": i, "b": i + 10} for i in range(5)],
        table_name="example_table_ab",
        **destination_config.run_kwargs,
    )

    pipeline.run(
        [{"a": i, "c": i + 20} for i in range(5)],
        table_name="example_table_ac",
        **destination_config.run_kwargs,
    )

    pipeline.run(
        [{"a": -1, "b": -1, "c": -1}],  # one dummy row → defines schema
        table_name="merged_table",
        **destination_config.run_kwargs,
    )

    dataset = pipeline.dataset()
    merged_cols = dataset.schema.tables["merged_table"]["columns"]

    @dlt.resource(table_name="merged_table")
    def insert_ab() -> Any:
        rel = dataset["example_table_ab"][["a", "b"]]  # only a,b
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in merged_cols.items() if k in ("a", "b")}),
        )

    @dlt.resource(table_name="merged_table")
    def insert_ac() -> Any:
        rel = dataset["example_table_ac"][["a", "c"]]  # only a,c
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in merged_cols.items() if k in ("a", "c")}),
        )

    pipeline.run(
        [insert_ab(), insert_ac()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    df = dataset["merged_table"].df()

    assert len(df) == 11
    assert sum(df["a"].to_list()) == 19  # -1 + 2 * (0 + 1 + 2 + 3 + 4)
    assert df["b"].dropna().sum() == 59  # -1 + 11 + 12 + 13 + 14 + 15
    assert df["c"].dropna().sum() == 109  # -1 + 21 + 22 + 23 + 24 + 25


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_model_from_two_consecutive_tables(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline("test_model_from_joined_table", dev_mode=True)

    pipeline.run(
        [{"a": i, "b": i + 10} for i in range(5)],
        table_name="example_table_ab",
        **destination_config.run_kwargs,
    )

    pipeline.run(
        [{"a": i, "c": i + 20} for i in range(5)],
        table_name="example_table_ac",
        **destination_config.run_kwargs,
    )

    dataset = pipeline.dataset()

    relation_ab = dataset["example_table_ab"]
    relation_ac = dataset["example_table_ac"]
    ab_cols = dataset.schema.tables["example_table_ab"]["columns"]
    ac_cols = dataset.schema.tables["example_table_ac"]["columns"]

    @dlt.resource(table_name="result_table")
    def insert_ab() -> Any:
        rel = relation_ab[["a", "b"]]
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in ab_cols.items() if k in ["a", "b"]}),
        )

    @dlt.resource(table_name="result_table")
    def insert_ac() -> Any:
        rel = relation_ac[["a", "c"]]
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in ac_cols.items() if k in ["a", "c"]}),
        )

    pipeline.run(
        [insert_ab()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    pipeline.run(
        [insert_ac()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    # Validate columns for the result table
    # Note that the addition of "_dlt_id" is disabled by default
    assert set(pipeline.default_schema.tables["result_table"]["columns"].keys()) == {
        "a",
        "b",
        "c",
        "_dlt_load_id",
    }

    df = dataset["result_table"].df()

    assert len(df) == 10
    assert sum(df["a"].to_list()) == 20  # 2 * (0 + 1 + 2 + 3 + 4)
    assert df["b"].dropna().sum() == 60  # 11 + 12 + 13 + 14 + 15
    assert df["c"].dropna().sum() == 110  # 21 + 22 + 23 + 24 + 25


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "write_disposition",
    ["merge", "replace", "append"],
    ids=lambda x: x,
)
def test_write_dispositions(
    destination_config: DestinationTestConfiguration,
    write_disposition: TWriteDisposition,
    preserve_environ,
) -> None:
    # adding dlt id is disabled by default, so we set it to true
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(True)

    pipeline = destination_config.setup_pipeline("test_write_dispositions", dev_mode=True)

    pipeline.run(
        [{"a": i} for i in range(7)],
        primary_key="a",
        table_name="example_table_1",
        write_disposition=write_disposition,
        **destination_config.run_kwargs,
    )
    pipeline.run(
        [{"a": i + 1} for i in range(10)],
        primary_key="a",
        table_name="example_table_2",
        write_disposition=write_disposition,
        **destination_config.run_kwargs,
    )

    # we now run a select of items 3-10 from example_table_2 into example_table_1
    # each w_d should have a different outcome
    dataset = pipeline.dataset()
    example_table_columns = dataset.schema.tables["example_table_1"]["columns"]
    # In Databricks, Ibis adds a helper column to emulate offset, causing a schema mismatch
    # when the query attempts to insert it. We explicitly select only the expected columns.
    # Note that we also explicitly select "_dlt_id" because its addition is disabled by default
    example_table_2 = dataset.table("example_table_2").to_ibis()
    expression = (
        example_table_2.filter(example_table_2.a >= 3).order_by("a").limit(7)[["a", "_dlt_id"]]
    )
    relation = dataset(expression)

    @dlt.resource(
        write_disposition=write_disposition,
        table_name="example_table_1",
        primary_key="a",
    )
    def copied_table() -> Any:
        yield dlt.mark.with_hints(
            relation,
            hints=make_hints(columns=example_table_columns),
        )

    pipeline.run(
        [copied_table()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    # staging_dataset = dlt.dataset(
    #     pipeline.destination, pipeline.dataset_name + "_staging", schema=pipeline.default_schema
    # )
    # staging_table_1 = staging_dataset.example_table_1.df()
    # print(staging_table_1)

    table_1 = dataset["example_table_1"].df()
    result_items = table_1["a"].tolist()
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
    destination_configs,
    ids=lambda x: x.name,
)
def test_multiple_statements_per_resource(
    destination_config: DestinationTestConfiguration, preserve_environ
) -> None:
    # Disable unique indexing for postgres, otherwise there will be a not null constraint error
    # because we're copying from the same table
    if destination_config.destination_type == "postgres":
        os.environ["DESTINATION__POSTGRES__CREATE_INDEXES"] = "false"

    pipeline = destination_config.setup_pipeline(
        "test_multiple_statments_per_resource", dev_mode=True
    )

    pipeline.run(
        [{"a": i} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )
    dataset = pipeline.dataset()

    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # create a resource that generates sql statements to create 2 new tables
    # we also need to supply all hints so the table can be created,
    # note that we explicitly select "_dlt_id" as its addition is disabled by default
    @dlt.resource()
    def copied_table() -> Any:
        rel1 = dataset["example_table"][["a", "_dlt_id"]].limit(5)
        yield dlt.mark.with_hints(
            rel1,
            hints=make_hints(columns=example_table_columns),
        )

        rel2 = dataset["example_table"][["a", "_dlt_id"]].limit(7)
        yield dlt.mark.with_hints(
            rel2,
            hints=make_hints(columns=example_table_columns),
        )

    pipeline.run(
        [copied_table()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )

    assert load_table_counts(pipeline, "copied_table", "example_table") == {
        "copied_table": 12,
        "example_table": 10,
    }

    # two model jobs where produced
    if destination_config.destination_type == "athena":
        assert count_job_types(pipeline) == {
            "copied_table": {"model": 2, "sql": 1},
        }
    else:
        assert count_job_types(pipeline) == {
            "copied_table": {"model": 2},
        }


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("drop_column", ["_dlt_load_id", "_dlt_id"])
def test_copying_table_with_dropped_column(
    destination_config: DestinationTestConfiguration, drop_column: str, preserve_environ
) -> None:
    """
    Test copying a table while excluding one of the DLT-injected columns (`_dlt_id` or `_dlt_load_id`),
    to verify that:
    - The resulting table contains all expected columns, including dlt ones.
    - Row counts and model job counts are correct.
    - Load id is correct.
    _ dlt ids are unique.
    """
    # adding dlt id is disabled by default, so we set it to true
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(True)

    table_suffix = "no_dlt_id" if drop_column == "_dlt_id" else "dlt_id"
    target_table_name = f"copied_table_{table_suffix}"

    # populate a table with two columns each with 10 items and retrieve dataset
    pipeline = destination_config.setup_pipeline(
        "test_copying_table_with_dropped_column", dev_mode=True
    )

    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )
    dataset = pipeline.dataset()
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    @dlt.resource(name=target_table_name)
    def copied_table() -> Any:
        kept_columns = ["a", "b", "_dlt_load_id", "_dlt_id"]
        kept_columns.remove(drop_column)

        rel = dataset["example_table"][kept_columns].limit(5)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(
                columns={k: v for k, v in example_table_columns.items() if k != drop_column}
            ),
        )

    load_info = pipeline.run(
        [copied_table()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )
    assert_load_info(load_info)

    # Validate row counts for all tables
    assert load_table_counts(pipeline, target_table_name, "example_table") == {
        target_table_name: 5,
        "example_table": 10,
    }

    assert target_table_name in pipeline.default_schema.tables
    assert "example_table" in pipeline.default_schema.tables

    # Validate columns for the table
    assert set(pipeline.default_schema.tables[target_table_name]["columns"].keys()) == {
        "a",
        "b",
        "_dlt_id",
        "_dlt_load_id",
    }

    # Validate that each table has exactly one model job
    if destination_config.destination_type == "athena":
        assert count_job_types(pipeline) == {
            target_table_name: {"model": 1, "sql": 1},
        }
    else:
        assert count_job_types(pipeline) == {
            target_table_name: {"model": 1},
        }

    # Validate load id or dlt id
    load_id = load_info.loads_ids[0]
    result_items = dataset[target_table_name].df()[drop_column].to_list()

    if drop_column == "_dlt_load_id":
        assert all(
            item == load_id for item in result_items
        ), f"All values should match _dlt_load_id={load_id}"
    elif drop_column == "_dlt_id":
        assert len(result_items) == len(set(result_items)), "Values in _dlt_id must be unique"


@pytest.mark.parametrize(
    "destination_config",
    destination_configs,
    ids=lambda x: x.name,
)
def test_load_model_with_all_types(
    destination_config: DestinationTestConfiguration, preserve_environ
) -> None:
    # adding dlt id is disabled by default, so we set it to true
    os.environ["NORMALIZE__MODEL_NORMALIZER__ADD_DLT_ID"] = str(True)

    pipeline = destination_config.setup_pipeline("test_load_model_with_all_types", dev_mode=True)

    with pipeline._maybe_destination_capabilities() as caps:
        pass

    # this is simplistic way to set default file format
    if not destination_config.file_format:
        destination_config.file_format = (
            caps.preferred_loader_file_format or caps.preferred_staging_file_format
        )

    column_schemas, data_types = table_update_and_row_for_destination(destination_config)

    @dlt.resource(table_name="data_types", columns=column_schemas)
    def my_resource() -> Any:
        nonlocal data_types
        yield [data_types] * 10

    pipeline.run([my_resource()], **destination_config.run_kwargs)
    dataset = pipeline.dataset()
    example_table_columns = dataset.schema.tables["data_types"]["columns"]

    @dlt.resource()
    def copied_table() -> Any:
        rel = dataset["data_types"][list(data_types.keys())]
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns=example_table_columns),
        )

    info = pipeline.run(
        [copied_table()],
        loader_file_format="model",
        table_format=destination_config.run_kwargs["table_format"],
    )
    assert_load_info(info)

    rows = load_tables_to_dicts(pipeline, "copied_table", exclude_system_cols=True)["copied_table"]
    assert len(rows) == 10

    assert_all_data_types_row(
        pipeline.destination.capabilities(),
        rows[0],
        schema=column_schemas,
        allow_base64_binary=destination_config.destination_type == "clickhouse",
    )


@pytest.mark.parametrize("tables_contract", ["freeze", "evolve"])
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["duckdb"],
    ),
    ids=lambda x: x.name,
)
def test_data_contract_on_tables(
    destination_config: DestinationTestConfiguration, tables_contract: str
) -> None:
    pipeline = destination_config.setup_pipeline("test_data_contract_on_tables", dev_mode=True)

    # Populate an example table
    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )
    dataset = pipeline.dataset()

    # Retrieve the SQL dialect and schema information
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # Define a resource to create a new copied table
    @dlt.resource(schema_contract={"tables": tables_contract})  # type: ignore
    def copied_table() -> Any:
        rel = dataset["example_table"][["a", "b", "_dlt_id"]].limit(5)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns=example_table_columns),
        )

    if tables_contract == "evolve":
        info = pipeline.run(
            [copied_table()],
            loader_file_format="model",
            table_format=destination_config.run_kwargs["table_format"],
        )
        assert_load_info(info)
    else:
        with pytest.raises(PipelineStepFailed) as py_exc:
            pipeline.run(
                [copied_table()],
                loader_file_format="model",
                table_format=destination_config.run_kwargs["table_format"],
            )
        assert py_exc.value.step == "extract"
        assert isinstance(py_exc.value.__context__, DataValidationError)
        assert py_exc.value.__context__.schema_entity == "tables"
        assert py_exc.value.__context__.contract_mode == "freeze"
        assert py_exc.value.__context__.table_name == "copied_table"


@pytest.mark.parametrize("columns_contract", ["freeze", "evolve", "discard_row", "discard_value"])
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["duckdb"],
    ),
    ids=lambda x: x.name,
)
def test_data_contract_on_columns(
    destination_config: DestinationTestConfiguration, columns_contract: str
) -> None:
    # NOTE: discard_row on columns behaves the same way as discard_value
    pipeline = destination_config.setup_pipeline("test_data_contract_on_columns", dev_mode=True)

    # Populate tables with different column sets and retreve dataset
    pipeline.run(
        [{"a": i} for i in range(10)],
        table_name="copied_table",
        **destination_config.run_kwargs,
    )  # Single column a
    pipeline.run(
        [{"a": i, "b": i + 1} for i in range(10)],
        table_name="example_table",
        **destination_config.run_kwargs,
    )  # Two columns a, b
    dataset = pipeline.dataset()

    # Retrieve the SQL dialect and schema information
    example_table_columns = dataset.schema.tables["example_table"]["columns"]

    # Define a resource to insert a new column into copied_table
    @dlt.resource(schema_contract={"columns": columns_contract})  # type: ignore
    def copied_table() -> Any:
        rel = dataset["example_table"][["b", "_dlt_load_id", "_dlt_id"]].limit(5)
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns=example_table_columns),
        )

    if columns_contract == "evolve":
        info = pipeline.run(
            [copied_table()],
            loader_file_format="model",
            table_format=destination_config.run_kwargs["table_format"],
        )
        assert_load_info(info)
        assert load_table_counts(pipeline, "copied_table", "example_table") == {
            "copied_table": 15,  # 10 original rows + 5 new rows with column "b"
            "example_table": 10,
        }
        # Validate that column "b" was added and contains the correct data
        # The last 5 rows of "b" should match the first 5 rows of "b" from example_table
        result_items = dataset["copied_table"].df()["b"].tolist()
        assert result_items[-5:] == [1, 2, 3, 4, 5]

    elif columns_contract == "freeze":
        with pytest.raises(PipelineStepFailed) as py_exc:
            pipeline.run(
                [copied_table()],
                loader_file_format="model",
                table_format=destination_config.run_kwargs["table_format"],
            )
        assert py_exc.value.step == "extract"
        assert isinstance(py_exc.value.__context__, DataValidationError)
        assert py_exc.value.__context__.schema_entity == "columns"
        assert py_exc.value.__context__.contract_mode == "freeze"
        assert py_exc.value.__context__.table_name == "copied_table"

    elif columns_contract in ["discard_row", "discard_value"]:
        info = pipeline.run(
            [copied_table()],
            loader_file_format="model",
            table_format=destination_config.run_kwargs["table_format"],
        )
        assert_load_info(info)
        assert load_table_counts(pipeline, "copied_table", "example_table") == {
            "copied_table": 15,  # 10 original rows + 5 new rows without column "b"
            "example_table": 10,
        }
        # Validate that column "b" was not added
        assert "b" not in pipeline.default_schema.tables["copied_table"]["columns"].keys()
        # Validate that the original rows in "a" remain unchanged
        result_items = dataset["copied_table"].df()["a"].tolist()
        assert result_items[:10] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


@pytest.mark.parametrize("data_type_contract", ["freeze", "evolve", "discard_row", "discard_value"])
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["duckdb"],
    ),
    ids=lambda x: x.name,
)
def test_data_contract_on_data_type(
    destination_config: DestinationTestConfiguration, data_type_contract: str
) -> None:
    # TODO: data contracts on data type level currently don't work as expected
    pipeline = destination_config.setup_pipeline("test_data_contract_on_data_type", dev_mode=True)

    # Populate tables with different data types and retrieve dataset
    pipeline.run(
        [{"a": i} for i in range(10)],
        table_name="copied_table",
        **destination_config.run_kwargs,
    )  # Integer column
    pipeline.run(
        [{"a": string, "b": i} for i, string in enumerate(["I", "love", "dlt"])],
        table_name="example_table",
        **destination_config.run_kwargs,
    )  # String column
    dataset = pipeline.dataset()

    # Retrieve the SQL dialect and schema information
    example_table_columns = dataset.schema.tables["example_table"]["columns"]
    copied_table_columns = dataset.schema.tables["copied_table"]["columns"]

    # Validate initial data types
    assert copied_table_columns["a"]["data_type"] == "bigint"
    assert example_table_columns["a"]["data_type"] == "text"

    # Define model resource to insert string column into integer column
    @dlt.resource(schema_contract={"data_type": data_type_contract}, table_name="copied_table")  # type: ignore
    def copied_table() -> Any:
        rel = dataset["example_table"][["a", "_dlt_load_id", "_dlt_id"]]
        yield dlt.mark.with_hints(
            rel,
            hints=make_hints(columns={k: v for k, v in example_table_columns.items() if k != "b"}),
        )

    if data_type_contract in ["freeze", "discard_row", "discard_value", "evolve"]:
        with pytest.raises(PipelineStepFailed) as py_exc:
            pipeline.run(
                [copied_table()],
                loader_file_format="model",
                table_format=destination_config.run_kwargs["table_format"],
            )
        assert py_exc.value.step == "load"
        assert isinstance(py_exc.value.__context__, LoadClientJobException)
