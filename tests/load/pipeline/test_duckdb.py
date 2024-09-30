from typing import ClassVar, Optional
import pytest
import os
from datetime import datetime  # noqa: I251

import dlt
from dlt.common import json
from dlt.common.libs.pydantic import DltConfig
from dlt.common.schema.exceptions import SchemaIdentifierNormalizationCollision
from dlt.common.time import ensure_pendulum_datetime, pendulum

from dlt.destinations import duckdb
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.cases import TABLE_UPDATE_ALL_INT_PRECISIONS, TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS
from tests.load.duckdb.test_duckdb_table_builder import add_timezone_false_on_precision
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import airtable_emojis, assert_data_table_counts, load_table_counts

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_duck_case_names(destination_config: DestinationTestConfiguration) -> None:
    # we want to have nice tables
    os.environ["SCHEMA__NAMING"] = "duck_case"
    pipeline = destination_config.setup_pipeline("test_duck_case_names")
    # create tables and columns with emojis and other special characters
    pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"),
        **destination_config.run_kwargs,
    )
    pipeline.run(
        [{"🐾Feet": 2, "1+1": "two", "\nhey": "value"}],
        table_name="🦚Peacocks🦚",
        **destination_config.run_kwargs,
    )
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts == {
        "📆 Schedule": 3,
        "🦚Peacock": 1,
        "🦚Peacock__peacock": 3,
        "🦚Peacocks🦚": 1,
        "🦚WidePeacock": 1,
        "🦚WidePeacock__Peacock": 3,
    }

    # this will fail - duckdb preserves case but is case insensitive when comparing identifiers
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(
            [{"🐾Feet": 2, "1+1": "two", "🐾feet": "value"}],
            table_name="🦚peacocks🦚",
            **destination_config.run_kwargs,
        )
    assert isinstance(pip_ex.value.__context__, SchemaIdentifierNormalizationCollision)
    assert pip_ex.value.__context__.conflict_identifier_name == "🦚Peacocks🦚"
    assert pip_ex.value.__context__.identifier_name == "🦚peacocks🦚"
    assert pip_ex.value.__context__.identifier_type == "table"

    # show tables and columns
    with pipeline.sql_client() as client:
        with client.execute_query("DESCRIBE 🦚peacocks🦚;") as q:
            tables = q.df()
    assert tables["column_name"].tolist() == ["🐾Feet", "1+1", "hey", "_dlt_load_id", "_dlt_id"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_duck_precision_types(destination_config: DestinationTestConfiguration) -> None:
    import pyarrow as pa

    # store timestamps without timezone adjustments
    os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = ""

    now_s = ensure_pendulum_datetime("2022-05-23T13:26:46+01:00")
    now_ms = ensure_pendulum_datetime("2022-05-23T13:26:46.167+01:00")
    now_us = ensure_pendulum_datetime("2022-05-23T13:26:46.167231+01:00")
    now_ns = ensure_pendulum_datetime("2022-05-23T13:26:46.167231+01:00")  # time.time_ns()

    # TODO: we can't really handle integers > 64 bit (so nanoseconds and HUGEINT)
    pipeline = destination_config.setup_pipeline("test_duck_all_precision_types")
    row = [
        {
            "col1_ts": now_s,
            "col2_ts": now_ms,
            "col3_ts": now_us,
            "col4_ts": now_ns,
            "col1_int": -128,
            "col2_int": 16383,
            "col3_int": 2**32 // 2 - 1,
            "col4_int": 2**64 // 2 - 1,
            "col5_int": 2**64 // 2 - 1,
        }
    ]
    pipeline.run(
        row,
        table_name="row",
        **destination_config.run_kwargs,
        columns=add_timezone_false_on_precision(
            TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS + TABLE_UPDATE_ALL_INT_PRECISIONS
        ),
    )

    with pipeline.sql_client() as client:
        table = client.native_connection.sql("SELECT * FROM row").arrow()

    # only us has TZ aware timestamp in duckdb, also we have UTC here
    assert table.schema.field(0).type == pa.timestamp("s")
    assert table.schema.field(1).type == pa.timestamp("ms")
    assert table.schema.field(2).type == pa.timestamp("us")
    assert table.schema.field(3).type == pa.timestamp("ns")

    assert table.schema.field(4).type == pa.int8()
    assert table.schema.field(5).type == pa.int16()
    assert table.schema.field(6).type == pa.int32()
    assert table.schema.field(7).type == pa.int64()
    assert table.schema.field(8).type == pa.decimal128(38, 0)

    table_row = table.to_pylist()[0]
    table_row["col1_ts"] = ensure_pendulum_datetime(table_row["col1_ts"])
    table_row["col2_ts"] = ensure_pendulum_datetime(table_row["col2_ts"])
    table_row["col3_ts"] = ensure_pendulum_datetime(table_row["col3_ts"])
    table_row["col4_ts"] = ensure_pendulum_datetime(table_row["col4_ts"])
    table_row.pop("_dlt_id")
    table_row.pop("_dlt_load_id")
    assert table_row == row[0]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_new_nested_prop_parquet(destination_config: DestinationTestConfiguration) -> None:
    from pydantic import BaseModel

    class EventDetail(BaseModel):
        detail_id: str
        is_complete: bool

    class EventV1(BaseModel):
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

        ver: int
        id: str  # noqa
        details: EventDetail

    duck_factory = duckdb("_storage/test_duck.db")

    pipeline = destination_config.setup_pipeline(
        "test_new_nested_prop_parquet", dataset_name="test_dataset"
    )
    pipeline.destination = duck_factory  # type: ignore

    event = {"ver": 1, "id": "id1", "details": {"detail_id": "detail_1", "is_complete": False}}

    pipeline.run(
        [event],
        table_name="events",
        columns=EventV1,
        loader_file_format="parquet",
        schema_contract="evolve",
    )
    print(pipeline.default_schema.to_pretty_yaml())

    # we will use a different pipeline with a separate schema but writing to the same dataset and to the same table
    # the table schema is identical to the previous one with a single field ("time") added
    # this will create a different order of columns than in the destination database ("time" will map to "_dlt_id")
    # duckdb copies columns by column index so that will fail

    class EventDetailV2(BaseModel):
        detail_id: str
        is_complete: bool
        time: Optional[datetime]

    class EventV2(BaseModel):
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

        ver: int
        id: str  # noqa
        details: EventDetailV2

    event["details"]["time"] = pendulum.now()  # type: ignore

    pipeline = destination_config.setup_pipeline(
        "test_new_nested_prop_parquet_2", dataset_name="test_dataset"
    )
    pipeline.destination = duck_factory  # type: ignore
    pipeline.run(
        [event],
        table_name="events",
        columns=EventV2,
        loader_file_format="parquet",
        schema_contract="evolve",
    )
    print(pipeline.default_schema.to_pretty_yaml())


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_jsonl_reader(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_jsonl_reader")

    data = [{"a": 1, "b": 2}, {"a": 1}]
    pipeline.run(data, table_name="data", loader_file_format="jsonl")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_provoke_parallel_parquet_same_table(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(name="events", file_format="parquet")
    def _get_shuffled_events(repeat: int = 1):
        for _ in range(repeat):
            with open(
                "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
            ) as f:
                issues = json.load(f)
                yield issues

    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "200"
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "200"

    pipeline = destination_config.setup_pipeline("test_provoke_parallel_parquet_same_table")
    pipeline.run(_get_shuffled_events(50), **destination_config.run_kwargs)

    assert_data_table_counts(
        pipeline,
        expected_counts={
            "events": 5000,
            "events__payload__pull_request__base__repo__topics": 14500,
            "events__payload__commits": 3850,
            "events__payload__pull_request__requested_reviewers": 1200,
            "events__payload__pull_request__labels": 1300,
            "events__payload__issue__labels": 150,
            "events__payload__issue__assignees": 50,
        },
    )
    metrics = pipeline.last_trace.last_normalize_info.metrics[
        pipeline.last_trace.last_normalize_info.loads_ids[0]
    ][0]
    event_files = [m for m in metrics["job_metrics"].keys() if m.startswith("events.")]
    assert len(event_files) == 5000 // 200
    assert all(m.endswith("parquet") for m in event_files)
