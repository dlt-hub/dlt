from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import pytest

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.exceptions import MissingDependencyException

from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import uniq_id

from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from tests.load.sources.sql_database.utils import (
    assert_extracted_uuids_are_strings,
    assert_incremental_chunks,
)
from tests.pipeline.utils import (
    assert_load_info,
    assert_schema_on_data,
    assert_table_counts,
    load_tables_to_dicts,
)
from tests.utils import capture_dlt_logger

try:
    from dlt.sources.sql_database import (
        ReflectionLevel,
        TableBackend,
        sql_database,
        sql_table,
    )
    from tests.load.sources.sql_database.mssql_source import DECIMALS_ROW, MSSQLSourceDB
    import sqlalchemy as sa
except MissingDependencyException:
    pytest.skip("Tests require sql alchemy", allow_module_level=True)


pytestmark = pytest.mark.mssql


PANDAS_MIXED_OFFSETS = "pandas backend yields a DATETIMEOFFSET column with mixed offsets as strings"
CONNECTORX_DATETIMEOFFSET = (
    "connectorx (0.4.5 to 0.4.7a1) returns DATETIMEOFFSET as the instant minus its offset"
)


def make_pipeline(destination_name: TDestinationReferenceArg) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database" + uniq_id(),
        destination=destination_name,
        dataset_name="test_sql_pipeline_" + uniq_id(),
        dev_mode=False,
    )


def backend_credentials(
    mssql_db: MSSQLSourceDB, backend: TableBackend
) -> ConnectionStringCredentials:
    """Swaps in the mssql-python dialect for `mssql_arrow`, the only one exposing an Arrow cursor."""
    if backend != "mssql_arrow":
        return mssql_db.credentials
    url = sa.engine.make_url(mssql_db.credentials.to_native_representation())
    url = url.set(drivername="mssql+mssqlpython").difference_update_query(["driver"])
    try:
        url.get_dialect()
    except sa.exc.NoSuchModuleError:
        pytest.skip("mssql_arrow needs the mssql+mssqlpython dialect from SQLAlchemy 2.1")
    return ConnectionStringCredentials(url.render_as_string(hide_password=False))


@pytest.mark.parametrize(
    "backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx", "mssql_arrow"]
)
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_all_data_types(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    request: pytest.FixtureRequest,
) -> None:
    if backend == "pandas" and reflection_level == "minimal":
        # without a reflected type nothing turns the strings back into timestamps
        request.applymarker(pytest.mark.xfail(strict=True, reason=PANDAS_MIXED_OFFSETS))
    source = sql_database(
        credentials=backend_credentials(mssql_db, backend),
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


@pytest.mark.parametrize(
    "backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx", "mssql_arrow"]
)
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_sql_table_incremental_datetime_ntz(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")
    table = sql_table(
        credentials=backend_credentials(mssql_db, backend),
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "some_smalldatetime",
            initial_value=ensure_pendulum_datetime("1999-01-01T00:00:00+00:00").naive(),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    info = mssql_db.table_infos["app_user"]
    assert_incremental_chunks(
        pipeline,
        table,
        "some_smalldatetime",
        timezone=False,
        row_count=info["row_count"],
        cursor_values=[row["some_smalldatetime"] for row in info["rows"]],
    )


@pytest.mark.parametrize(
    "backend",
    [
        "sqlalchemy",
        "pyarrow",
        pytest.param(
            "pandas",
            marks=pytest.mark.xfail(
                strict=True,
                reason=PANDAS_MIXED_OFFSETS,
            ),
        ),
        pytest.param(
            "connectorx",
            marks=pytest.mark.xfail(
                strict=True,
                reason=CONNECTORX_DATETIMEOFFSET,
            ),
        ),
        "mssql_arrow",
    ],
)
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_sql_table_incremental_datetime_tz(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")
    table = sql_table(
        credentials=backend_credentials(mssql_db, backend),
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "some_datetimeoffset",
            initial_value=datetime(1999, 1, 1, tzinfo=timezone.utc),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    info = mssql_db.table_infos["app_user"]
    assert_incremental_chunks(
        pipeline,
        table,
        "some_datetimeoffset",
        timezone=True,
        row_count=info["row_count"],
        cursor_values=[row["some_datetimeoffset"] for row in info["rows"]],
    )


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
            "some_datetime2", initial_value=ensure_pendulum_datetime("2918-08-01 00:00:00.000")
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
            == ensure_pendulum_datetime("2918-08-01 00:00:00.000").naive()
        )


@pytest.mark.parametrize("backend", ["pyarrow", "sqlalchemy", "pandas", "mssql_arrow"])
def test_uniqueidentifier_data_type(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
) -> None:
    """UNIQUEIDENTIFIER as merge key must not create duplicates due to casing mismatch.

    Reproduces the user case from #3299: initial load followed by incremental merge with
    both updates and inserts must not create duplicate rows.
    """
    import uuid

    pipeline = make_pipeline("duckdb")
    rc = mssql_db.table_infos["app_user"]["row_count"]

    # reuse sql_table resource for both loads
    table = sql_table(
        credentials=backend_credentials(mssql_db, backend),
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level="full",
        write_disposition="merge",
        primary_key="some_uniqueidentifier",
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=ensure_pendulum_datetime("1999-01-01T00:00:00+00:00"),
        ),
    )

    # 1. initial full load with UUID as merge primary key
    info = pipeline.run(table, loader_file_format="parquet")
    assert_load_info(info)

    # schema must have data_type="text" for the UNIQUEIDENTIFIER column
    uid_col = pipeline.default_schema.tables["app_user"]["columns"]["some_uniqueidentifier"]
    assert uid_col["data_type"] == "text"

    rows_after_initial = load_tables_to_dicts(pipeline, "app_user")["app_user"]
    assert len(rows_after_initial) == rc
    for val in (row["some_uniqueidentifier"] for row in rows_after_initial):
        uuid.UUID(val)  # validates well-formed

    # 2. update 3 existing rows' updated_at AND insert 10 new rows, then merge
    # updated_at set far in the future so updated rows enter the incremental window
    future_ts = ensure_pendulum_datetime("2030-01-01T00:00:00+00:00")
    for i, row in enumerate(rows_after_initial[:3]):
        mssql_db.update_row(
            {"updated_at": future_ts.add(seconds=i)},
            f"id = {row['id']}",
        )
    mssql_db.generate_users(n=10)

    info = pipeline.run(table, loader_file_format="parquet")
    assert_load_info(info)

    rows_after_merge = load_tables_to_dicts(pipeline, "app_user")["app_user"]
    # merge must not create duplicates: rc initial + 10 new
    assert len(rows_after_merge) == rc + 10


@pytest.mark.parametrize(
    "backend", ["sqlalchemy", "pyarrow", "pandas", "connectorx", "mssql_arrow"]
)
def test_uniqueidentifier_yields_str(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
) -> None:
    """The resource must yield UNIQUEIDENTIFIER values as Python str, never uuid.UUID objects."""
    import uuid

    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")

    table = sql_table(
        credentials=backend_credentials(mssql_db, backend),
        table="app_user",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level="full",
    )

    all_uuids: List[str] = []
    for item in table:
        all_uuids.extend(assert_extracted_uuids_are_strings("some_uniqueidentifier", item))

    assert len(all_uuids) == mssql_db.table_infos["app_user"]["row_count"]
    for val in all_uuids:
        uuid.UUID(val)  # validates well-formed


@pytest.mark.parametrize(
    "backend,backend_kwargs",
    [
        ("pyarrow", {}),
        ("connectorx", {"return_type": "arrow"}),
        ("connectorx", {"return_type": "arrow_stream"}),
    ],
    ids=["pyarrow", "connectorx-arrow", "connectorx-arrow_stream"],
)
@pytest.mark.usefixtures("mssql_db_with_decimals")
def test_decimal_precision(
    mssql_db: MSSQLSourceDB,
    backend: TableBackend,
    backend_kwargs: Dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Decimals and MONEY keep precision, connectorx warns on columns it rounds or reads as floats."""
    if backend == "connectorx":
        pytest.importorskip("sqlalchemy", minversion="2.0")

    table = sql_table(
        credentials=mssql_db.credentials,
        table="decimals",
        schema=mssql_db.schema,
        backend=backend,
        reflection_level="full",
        backend_kwargs=backend_kwargs,
    )
    pipeline = make_pipeline("duckdb")
    with capture_dlt_logger(caplog) as caplog:
        info = pipeline.run(table)
    assert_load_info(info)

    row = load_tables_to_dicts(pipeline, "decimals")["decimals"][0]
    # does not fit float64: connectorx<0.4.6 returned it as double
    assert row["amount"] == DECIMALS_ROW["amount"]
    assert row["big_amount"] == DECIMALS_ROW["big_amount"]
    balance = pipeline.default_schema.get_table("decimals")["columns"]["balance"]
    assert (balance["data_type"], balance["precision"], balance["scale"]) == ("decimal", 19, 4)

    warnings = [r.message for r in caplog.records if "lower precision" in r.message]
    if backend == "pyarrow":
        assert row["rate"] == DECIMALS_ROW["rate"]
        assert row["balance"] == DECIMALS_ROW["balance"]
        assert warnings == []
    else:
        # connectorx rounds to its fixed scale of 10 and reads MONEY as double
        assert row["rate"] == Decimal("1.1234567891")
        assert len(warnings) == 1
        assert "`rate`" in warnings[0]
        assert "`balance`" in warnings[0]
        assert "`amount`" not in warnings[0]
        assert "`big_amount`" not in warnings[0]


@pytest.mark.usefixtures("mssql_db_with_decimals")
def test_connectorx_decimal_out_of_range(mssql_db: MSSQLSourceDB) -> None:
    """connectorx fails on values above ~7.9e18 even if the column precision allows them."""
    pytest.importorskip("sqlalchemy", minversion="2.0")

    # fits DECIMAL(35,2)
    mssql_db.update_row(
        {"big_amount": Decimal("123456789012345678901.23")}, "id = 1", table="decimals"
    )

    table = sql_table(
        credentials=mssql_db.credentials,
        table="decimals",
        schema=mssql_db.schema,
        backend="connectorx",
        reflection_level="full",
    )
    pipeline = make_pipeline("duckdb")
    # the value cannot be rescaled to the fixed scale of 10 within 96 bits of rust_decimal
    with pytest.raises(PipelineStepFailed, match="decimal scale is not equal to expected scale"):
        pipeline.extract(table)
