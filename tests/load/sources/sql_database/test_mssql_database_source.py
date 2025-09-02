import pytest

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.exceptions import MissingDependencyException

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.utils import uniq_id

from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import DltResource

from tests.load.sources.sql_database.utils import assert_incremental_chunks
from tests.pipeline.utils import (
    assert_load_info,
    assert_schema_on_data,
    assert_table_counts,
    load_tables_to_dicts,
)

try:
    from dlt.sources.sql_database import (
        ReflectionLevel,
        TableBackend,
        sql_database,
        sql_table,
    )
    from tests.load.sources.sql_database.mssql_source import MSSQLSourceDB
    import sqlalchemy as sa
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


def make_pipeline(destination_name: TDestinationReferenceArg) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database" + uniq_id(),
        destination=destination_name,
        dataset_name="test_sql_pipeline_" + uniq_id(),
        dev_mode=False,
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_all_data_types(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    source = sql_database(
        credentials=mssql_db.credentials,
        schema=mssql_db.schema,
        reflection_level=reflection_level,
        backend=backend,
    )

    pipeline = make_pipeline("duckdb")

    pipeline.extract(source, loader_file_format="parquet")
    pipeline.normalize()
    info = pipeline.load()
    assert_load_info(info)

    schema = pipeline.default_schema
    table = schema.tables["app_user"]

    # check tz-awareness
    assert table["columns"]["some_datetimeoffset"].get("timezone", True) is True
    # timezones are inferred from data or set explicitly, just not on sqlalchemy minimal
    ntz_flag = reflection_level == "minimal"
    assert table["columns"]["some_datetime2"].get("timezone", True) is ntz_flag
    assert table["columns"]["some_smalldatetime"].get("timezone", True) is ntz_flag

    assert_schema_on_data(
        table,
        load_tables_to_dicts(pipeline, "app_user")["app_user"],
        False,
        backend in ["sqlalchemy", "pyarrow"],
    )
    # check duckdb schema
    with pipeline.sql_client() as client:
        import duckdb

        duckdb_conn: duckdb.DuckDBPyConnection = client.native_connection
        columns = {r[0]: r[1] for r in duckdb_conn.sql("DESCRIBE app_user").fetchall()}
        assert columns["some_datetimeoffset"] == "TIMESTAMP WITH TIME ZONE"
        ntz_dt = "TIMESTAMP WITH TIME ZONE" if reflection_level == "minimal" else "TIMESTAMP"
        assert columns["some_datetime2"] == ntz_dt
        assert columns["some_smalldatetime"] == ntz_dt


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_sql_table_incremental_datetime_ntz(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")
    table = sql_table(
        credentials=mssql_db.credentials,
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "some_smalldatetime",
            initial_value=ensure_pendulum_datetime_utc("1999-01-01T00:00:00+00:00").naive(),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = mssql_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(pipeline, table, "some_smalldatetime", timezone=False, row_count=rc)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_sql_table_incremental_datetime_tz(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")
    table = sql_table(
        credentials=mssql_db.credentials,
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "created_at",
            initial_value=ensure_pendulum_datetime_utc("1999-01-01T00:00:00+00:00"),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = mssql_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(pipeline, table, "created_at", timezone=True, row_count=rc)


@pytest.mark.no_load
@pytest.mark.parametrize("timestamp_precision", (6, 7))
def test_sql_table_high_datetime(
    mssql_db: MSSQLSourceDB,
    timestamp_precision: int,
) -> None:
    """Tests datetime that goes beyond arrow nanosecond timestamp"""

    # mock user with high created_at so it is out of range
    user_id = mssql_db.get_random_user_id()
    mssql_db.update_row({"some_datetime2": "2918-08-01 00:00:00.000"}, f"id = {user_id}")

    table = sql_table(
        credentials=mssql_db.credentials,
        table="app_user",
        schema=mssql_db.schema,
        backend="pyarrow",
        reflection_level="full_with_precision",
        incremental=dlt.sources.incremental(
            "some_datetime2", initial_value=ensure_pendulum_datetime_utc("2918-08-01 00:00:00.000")
        ),
    )

    # if we set precision to 7, this will force arrow into nanoseconds and it will fail with overflow
    pipeline = make_pipeline(dlt.destinations.mssql(timestamp_precision=timestamp_precision))

    if timestamp_precision == 7:
        with pytest.raises(PipelineStepFailed):
            pipeline.extract(table)
    else:
        # this will pass
        pipeline.extract(table)
        # now load and check record
        pipeline = make_pipeline("duckdb")
        info = pipeline.run(table)
        assert_load_info(info)
        assert_table_counts(pipeline, {"app_user": 1}, "app_user")
        assert (
            load_tables_to_dicts(pipeline, "app_user")["app_user"][0]["some_datetime2"]
            == ensure_pendulum_datetime_utc("2918-08-01 00:00:00.000").naive()
        )
