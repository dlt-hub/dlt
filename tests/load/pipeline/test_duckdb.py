import pytest
import os

from dlt.common.time import ensure_pendulum_datetime
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.cases import TABLE_UPDATE_ALL_INT_PRECISIONS, TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS
from tests.pipeline.utils import airtable_emojis
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    load_table_counts,
)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_duck_case_names(destination_config: DestinationTestConfiguration) -> None:
    # we want to have nice tables
    # dlt.config["schema.naming"] = "duck_case"
    os.environ["SCHEMA__NAMING"] = "duck_case"
    pipeline = destination_config.setup_pipeline("test_duck_case_names")
    # create tables and columns with emojis and other special characters
    info = pipeline.run(
        airtable_emojis().with_resources("📆 Schedule", "🦚Peacock", "🦚WidePeacock"),
        loader_file_format=destination_config.file_format,
    )
    info.raise_on_failed_jobs()
    info = pipeline.run(
        [{"🐾Feet": 2, "1+1": "two", "\nhey": "value"}],
        table_name="🦚Peacocks🦚",
        loader_file_format=destination_config.file_format,
    )
    info.raise_on_failed_jobs()
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    )
    assert table_counts == {
        "📆 Schedule": 3,
        "🦚Peacock": 1,
        "🦚Peacock__peacock": 3,
        "🦚Peacocks🦚": 1,
        "🦚WidePeacock": 1,
        "🦚WidePeacock__peacock": 3,
    }

    # this will fail - duckdb preserves case but is case insensitive when comparing identifiers
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(
            [{"🐾Feet": 2, "1+1": "two", "🐾feet": "value"}],
            table_name="🦚peacocks🦚",
            loader_file_format=destination_config.file_format,
        )
    assert isinstance(pip_ex.value.__context__, DatabaseTerminalException)

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
    info = pipeline.run(
        row,
        table_name="row",
        loader_file_format=destination_config.file_format,
        columns=TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS + TABLE_UPDATE_ALL_INT_PRECISIONS,
    )
    info.raise_on_failed_jobs()

    with pipeline.sql_client() as client:
        table = client.native_connection.sql("SELECT * FROM row").arrow()

    # only us has TZ aware timestamp in duckdb, also we have UTC here
    assert table.schema.field(0).type == pa.timestamp("s")
    assert table.schema.field(1).type == pa.timestamp("ms")
    assert table.schema.field(2).type == pa.timestamp("us", tz="UTC")
    assert table.schema.field(3).type == pa.timestamp("ns")

    assert table.schema.field(4).type == pa.int8()
    assert table.schema.field(5).type == pa.int16()
    assert table.schema.field(6).type == pa.int32()
    assert table.schema.field(7).type == pa.int64()
    assert table.schema.field(8).type == pa.decimal128(38, 0)

    table_row = table.to_pylist()[0]
    table_row["col1_ts"] = ensure_pendulum_datetime(table_row["col1_ts"])
    table_row["col2_ts"] = ensure_pendulum_datetime(table_row["col2_ts"])
    table_row["col4_ts"] = ensure_pendulum_datetime(table_row["col4_ts"])
    table_row.pop("_dlt_id")
    table_row.pop("_dlt_load_id")
    assert table_row == row[0]
