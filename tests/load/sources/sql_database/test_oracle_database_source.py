from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, cast
import pytest

from dlt.common.exceptions import DependencyVersionException
from dlt.common.schema import TColumnSchema
from dlt.common.utils import assert_min_pkg_version

try:
    assert_min_pkg_version(pkg_name="sqlalchemy", version="2.0.0")
except DependencyVersionException:
    pytest.skip("Tests require sql alchemy 2.0.0 or higher", allow_module_level=True)


from tests.load.sources.sql_database.test_sql_database_source import (
    add_default_arrow_decimal_precision,
)
from tests.load.sources.sql_database.utils import assert_incremental_chunks
from tests.pipeline.utils import assert_load_info, assert_schema_on_data, load_tables_to_dicts

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.utils import uniq_id
from dlt.common.incremental.typing import TIncrementalRange

try:
    import oracledb
    import sqlalchemy as sa

    from tests.load.sources.sql_database.oracle_source import OracleSourceDB, TZ_PROBE_COLUMNS

    from dlt.sources.sql_database import ReflectionLevel, TableBackend, sql_database, sql_table
except Exception:
    pytest.skip(
        "Oracle tests require sqlalchemy oracle dialect and driver", allow_module_level=True
    )

pytestmark = [pytest.mark.oracle, pytest.mark.serial]


def make_pipeline(destination_name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="sql_database_oracle_" + uniq_id(),
        destination=destination_name,
        dataset_name="test_sql_oracle_" + uniq_id(),
        dev_mode=False,
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_all_data_types(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    # init dialect exclude_tablespaces=tuple()
    # or actually create a new user and work with it
    source = sql_database(
        credentials=oracle_db.credentials,
        schema=oracle_db.schema,
        reflection_level=reflection_level,
        backend=backend,
        table_names=["app_user"],
        # defer_table_reflect=True,
    )

    pipeline = make_pipeline("duckdb")

    pipeline.extract(source, loader_file_format="parquet")
    pipeline.normalize()
    info = pipeline.load()
    assert_load_info(info)

    table = pipeline.default_schema.tables["app_user"]
    # timezone flags: tz and local tz columns should be tz-aware, ntz depends on reflection level
    assert table["columns"]["some_timestamp_tz"].get("timezone", True) is True
    assert table["columns"]["some_timestamp_ltz"].get("timezone", True) is True
    ntz_flag = reflection_level == "minimal"
    assert table["columns"]["some_timestamp_ntz"].get("timezone", True) is ntz_flag

    rows = load_tables_to_dicts(pipeline, "app_user")["app_user"]
    for col in ("some_timestamp_tz", "some_timestamp_ltz", "some_timestamp_ntz"):
        _assert_loaded_source_values(oracle_db, "app_user", rows, col, table["columns"][col])


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
@pytest.mark.parametrize("session_zone", [None, "-07:00"], ids=lambda zone: zone or "dlt_engine")
def test_sql_table_incremental_datetime_ntz(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    session_zone: Optional[str],
) -> None:
    # a plain TIMESTAMP is compared without any time zone, so the session zone must not matter
    table = sql_table(
        credentials=(
            _session_engine(oracle_db, session_zone) if session_zone else oracle_db.credentials
        ),
        table="app_user",
        schema=oracle_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "some_timestamp_ntz",
            initial_value=datetime(1999, 1, 1),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = oracle_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(
        pipeline,
        table,
        "some_timestamp_ntz",
        timezone=False,
        row_count=rc,
        cursor_values=_cursor_values(oracle_db, "some_timestamp_ntz"),
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
@pytest.mark.parametrize("cursor", ["some_timestamp_tz", "some_timestamp_ltz"])
def test_sql_table_incremental_datetime_tz_aware(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
    cursor: str,
) -> None:
    # chunks of 10 rows surface a cursor that does not round trip as skipped or repeated rows
    table = sql_table(
        credentials=oracle_db.credentials,
        table="app_user",
        schema=oracle_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            cursor,
            initial_value=datetime(1999, 1, 1, tzinfo=timezone.utc),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = oracle_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(
        pipeline,
        table,
        cursor,
        timezone=True,
        row_count=rc,
        cursor_values=_cursor_values(oracle_db, cursor),
    )


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("cursor", ["some_timestamp_tz", "some_timestamp_ltz"])
@pytest.mark.parametrize("session_zone", ["-07:00", "+05:00", "Europe/Warsaw"])
def test_sql_table_incremental_datetime_tz_aware_session_zone(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    cursor: str,
    session_zone: str,
) -> None:
    """Incremental on zoned cursors does not depend on the session time zone of a user Engine."""
    table = sql_table(
        credentials=_session_engine(oracle_db, session_zone),
        table="app_user",
        schema=oracle_db.schema,
        backend=backend,
        incremental=dlt.sources.incremental(
            cursor,
            initial_value=datetime(1999, 1, 1, tzinfo=timezone.utc),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = oracle_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(
        pipeline,
        table,
        cursor,
        timezone=True,
        row_count=rc,
        cursor_values=_cursor_values(oracle_db, cursor),
    )


@pytest.mark.parametrize("session_zone", ["+00:00", "-07:00", "Europe/Warsaw"])
@pytest.mark.parametrize("range_start", ["open", "closed"])
def test_sql_table_incremental_zoned_initial_value(
    oracle_db: OracleSourceDB,
    session_zone: str,
    range_start: TIncrementalRange,
) -> None:
    """The bound cursor keeps its microseconds and is compared as a UTC instant."""
    expected = oracle_db.tz_probe_source("ts_tz")
    cutoff = expected[1]
    assert cutoff.microsecond != 0
    table = sql_table(
        credentials=_session_engine(oracle_db, session_zone),
        table="tz_probe",
        schema=oracle_db.schema,
        incremental=dlt.sources.incremental(
            "ts_tz",
            initial_value=cutoff.replace(tzinfo=timezone.utc),
            range_start=range_start,
            on_cursor_value_missing="exclude",
        ),
    )

    pipeline = make_pipeline("duckdb")
    assert_load_info(pipeline.run(table))
    loaded_ids = {int(row["id"]) for row in load_tables_to_dicts(pipeline, "tz_probe")["tz_probe"]}
    expected_ids = {
        id_
        for id_, value in expected.items()
        if value is not None and (value > cutoff or (range_start == "closed" and value == cutoff))
    }
    assert loaded_ids == expected_ids


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_zoned_timestamps_load(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    """Every Oracle datetime type round trips from the Python values the table was written from."""
    table = sql_table(
        credentials=oracle_db.credentials,
        table="tz_probe",
        schema=oracle_db.schema,
        backend=backend,
        reflection_level=reflection_level,
    )

    pipeline = make_pipeline("duckdb")
    assert_load_info(pipeline.run(table))

    columns = pipeline.default_schema.tables["tz_probe"]["columns"]
    if reflection_level != "minimal":
        for col in ("ts_tz", "ts9_tz", "ts_tz_named", "ts_ltz"):
            assert columns[col]["timezone"] is True, col
        for col in ("ts_naive", "d"):
            assert columns[col]["timezone"] is False, col

    rows = load_tables_to_dicts(pipeline, "tz_probe")["tz_probe"]
    for col in TZ_PROBE_COLUMNS:
        # Oracle agrees with Python on what was written
        assert oracle_db.tz_probe_expected(col) == oracle_db.tz_probe_source(col), col
        _assert_loaded_source_values(oracle_db, "tz_probe", rows, col, columns[col])


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize(
    "context_tz", ["UTC", "Europe/Warsaw", "America/Los_Angeles", "Asia/Kolkata"]
)
def test_zoned_timestamps_context_timezone(
    oracle_db: OracleSourceDB, backend: TableBackend, context_tz: str
) -> None:
    """Zoned values keep their instant and naive values their wall clock in any context timezone."""
    with Container().injectable_context(TimezoneContext(context_tz)):
        pipeline = make_pipeline("duckdb")
        table = sql_table(
            credentials=oracle_db.credentials,
            table="tz_probe",
            schema=oracle_db.schema,
            backend=backend,
        )
        assert_load_info(pipeline.run(table))
        rows = load_tables_to_dicts(pipeline, "tz_probe")["tz_probe"]
        columns = pipeline.default_schema.tables["tz_probe"]["columns"]
        for col in TZ_PROBE_COLUMNS:
            _assert_loaded_source_values(oracle_db, "tz_probe", rows, col, columns[col])

        cursor = "some_timestamp_tz"
        app_user = sql_table(
            credentials=oracle_db.credentials,
            table="app_user",
            schema=oracle_db.schema,
            backend=backend,
            incremental=dlt.sources.incremental(cursor, row_order="asc", range_start="open"),
            chunk_size=10,
        )
        assert_incremental_chunks(
            make_pipeline("duckdb"),
            app_user,
            cursor,
            timezone=True,
            row_count=oracle_db.table_infos["app_user"]["row_count"],
            cursor_values=_cursor_values(oracle_db, cursor),
        )


@pytest.mark.parametrize(
    "session_zone",
    [None, "+00:00", "-07:00", "+05:00", "UTC", "Europe/Warsaw"],
    ids=lambda zone: zone or "client_default",
)
def test_oracledb_zoned_reads(oracle_db: OracleSourceDB, session_zone: Optional[str]) -> None:
    """Pins the `oracledb` behavior that the Oracle notes in the troubleshooting docs describe."""
    tz_probe = oracle_db.tz_probe_table
    expected_wall = oracle_db.tz_probe_expected("ts_naive")
    expected_utc = oracle_db.tz_probe_expected("ts_tz")
    offset = _fixed_offset(session_zone)

    with _session_engine(oracle_db, session_zone).connect() as conn:

        def select(expr: str) -> Dict[int, Any]:
            return {
                int(id_): value
                for id_, value in conn.execute(sa.text(f"SELECT id, {expr} FROM {tz_probe}"))
            }

        assert conn.execute(sa.text("SELECT DBTIMEZONE FROM DUAL")).scalar() == "+05:00"

        # SYS_EXTRACT_UTC returns the instant for every zoned type in any session zone
        for col in ("ts_tz", "ts9_tz", "ts_tz_named", "ts_ltz"):
            assert select(f"SYS_EXTRACT_UTC({col})") == oracle_db.tz_probe_expected(col), col

        # read directly, WITH TIME ZONE drops the offset and keeps the wall clock
        assert select("ts_tz") == expected_wall
        # WITH LOCAL TIME ZONE arrives in the database time zone, not in the session one
        assert select("ts_ltz") == {
            id_: None if value is None else value + timedelta(hours=5)
            for id_, value in expected_utc.items()
        }
        # a value stored with a region name cannot be read at all
        with pytest.raises(sa.exc.DBAPIError, match="DPY-3022"):
            select("ts_tz_named")

        # SYS_EXTRACT_UTC on a plain TIMESTAMP shifts it by the session offset, DATE is rejected
        if offset is not None:
            assert select("SYS_EXTRACT_UTC(ts_naive)") == {
                id_: None if value is None else value - offset
                for id_, value in expected_wall.items()
            }
        with pytest.raises(sa.exc.DBAPIError, match="ORA-30175"):
            select("SYS_EXTRACT_UTC(d)")

        # a bound naive datetime is read in the session time zone, FROM_TZ anchors it to UTC
        def ids_equal_to(bind_expr: str, as_timestamp: bool = True) -> Set[int]:
            cursor = cast(oracledb.Cursor, conn.connection.cursor())
            # a datetime is bound as DATE unless told otherwise, which drops the fraction
            if as_timestamp:
                cursor.setinputsizes(p=oracledb.DB_TYPE_TIMESTAMP)
            cursor.execute(
                f"SELECT id FROM {tz_probe} WHERE ts_tz = {bind_expr}", p=expected_utc[1]
            )
            return {int(r[0]) for r in cursor}

        anchored = "FROM_TZ(CAST(:p AS TIMESTAMP), '+00:00')"
        assert ids_equal_to(anchored) == {1}
        assert ids_equal_to(anchored, as_timestamp=False) == set()
        if offset is not None:
            assert ids_equal_to(":p") == ({1} if offset == timedelta(0) else set())

        # a region name as session zone breaks only values that carry it
        current_timestamp = sa.text("SELECT CURRENT_TIMESTAMP FROM DUAL")
        if session_zone in ("UTC", "Europe/Warsaw"):
            with pytest.raises(sa.exc.DBAPIError, match="DPY-3022"):
                conn.execute(current_timestamp).fetchall()
        else:
            conn.execute(current_timestamp).fetchall()


def _assert_loaded_source_values(
    oracle_db: OracleSourceDB,
    table: str,
    rows: List[Dict[str, Any]],
    col: str,
    column: TColumnSchema,
) -> None:
    """Compares loaded `col` to the source values without normalizing them.

    Aware values compare by instant, naive by wall clock and naive never equals aware. A naive
    source value is expected aware in UTC only when the loaded column has `timezone`.
    """
    source = oracle_db.table_infos[table]["rows"]
    aware = column.get("timezone", True)
    for row in rows:
        expected = source[int(row["id"])][col]
        if expected is not None and expected.tzinfo is None and aware:
            expected = expected.replace(tzinfo=timezone.utc)
        assert row[col] == expected, (col, row["id"], row[col], expected)


def _cursor_values(oracle_db: OracleSourceDB, cursor: str) -> List[datetime]:
    return [row[cursor] for row in oracle_db.table_infos["app_user"]["rows"].values()]


def _fixed_offset(session_zone: Optional[str]) -> Optional[timedelta]:
    if session_zone is None or session_zone[0] not in "+-":
        return None
    sign = -1 if session_zone[0] == "-" else 1
    hours, minutes = session_zone[1:].split(":")
    return sign * timedelta(hours=int(hours), minutes=int(minutes))


def _session_engine(oracle_db: OracleSourceDB, session_zone: Optional[str]) -> sa.engine.Engine:
    """A user supplied Engine whose sessions run in `session_zone`, `None` keeps the client one."""
    engine = sa.create_engine(oracle_db.database_url, poolclass=sa.pool.NullPool)
    if session_zone:

        @sa.event.listens_for(engine, "connect")
        def _set_session_zone(dbapi_connection: Any, connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute(f"ALTER SESSION SET TIME_ZONE = '{session_zone}'")
            cursor.close()

    return engine


def _assert_decimal_columns(data: Any, backend: str) -> Any:
    """Verify that Oracle NUMBER columns are returned as Python Decimal (not float).

    This checks the raw data from the source before loading to confirm the
    Oracle dialect listener is correctly setting asdecimal=True.
    """
    # Columns that should be Decimal (NUMBER types without BINARY_FLOAT/BINARY_DOUBLE)
    decimal_columns = ["some_number", "some_number_precision", "some_number_precision_scale"]

    if backend == "sqlalchemy":
        # Data is a list of dicts
        for col in decimal_columns:
            value = data.get(col)
            if value is not None:
                assert isinstance(
                    value, Decimal
                ), f"Column {col} should be Decimal but got {type(value).__name__}: {value}"
    else:
        # For pyarrow/pandas backends, check the arrow/pandas types
        import pyarrow as pa

        if isinstance(data, pa.Table):
            for col in decimal_columns:
                if col in data.column_names:
                    col_type = data.schema.field(col).type
                    assert pa.types.is_decimal(
                        col_type
                    ), f"Column {col} should be decimal type but got {col_type}"
        # pandas DataFrame case - check for object dtype (Decimal) or decimal128
        elif hasattr(data, "dtypes"):  # pandas DataFrame
            # panda frames are always double, do not use panda frames for decimal data!
            pass

    return data


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_numeric_types(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    expected_columns = deepcopy(NUMERIC_COLUMNS)
    if backend == "pyarrow":
        add_default_arrow_decimal_precision(expected_columns)

    source = sql_database(
        credentials=oracle_db.credentials,
        schema=oracle_db.schema,
        reflection_level=reflection_level,
        backend=backend,
        defer_table_reflect=True,
        table_names=["app_user"],
    )

    # Add map to verify decimal types at extraction time
    source.resources["app_user"].add_map(lambda data: _assert_decimal_columns(data, backend))

    pipeline = make_pipeline("duckdb")
    info = pipeline.run(source, loader_file_format="parquet")
    assert_load_info(info)

    schema = pipeline.default_schema
    table = schema.tables["app_user"]
    assert_schema_on_data(
        table,
        load_tables_to_dicts(pipeline, "app_user")["app_user"],
        False,
        True,
    )

    for expected_column in expected_columns:
        assert expected_column["name"] in table["columns"]
        actual_column = table["columns"][expected_column["name"]]
        if reflection_level != "minimal":
            assert actual_column["data_type"] == expected_column["data_type"]
            if "precision" in expected_column:
                assert actual_column["precision"] == expected_column["precision"]
            else:
                assert "precision" not in actual_column
            if "scale" in expected_column:
                assert actual_column["scale"] == expected_column["scale"]
            else:
                assert "scale" not in actual_column


NUMERIC_COLUMNS: List[TColumnSchema] = [
    {
        "name": "some_number",
        "nullable": True,
        "data_type": "decimal",
    },
    {
        "name": "some_number_precision",
        "nullable": True,
        "data_type": "decimal",
        "precision": 10,
        "scale": (
            0
        ),  # even though column is defined as NUMBER(N), it's inferred as NUMBER(N, 0) by SQLAlchemy2
    },
    {
        "name": "some_number_precision_scale",
        "nullable": True,
        "data_type": "decimal",
        "precision": 10,
        "scale": 2,
    },
    {
        "name": "some_float",
        "nullable": True,
        "data_type": "double",
    },
    {
        "name": "some_binary_float",
        "nullable": True,
        "data_type": "double",
    },
    {
        "name": "some_binary_double",
        "nullable": True,
        "data_type": "double",
    },
]
