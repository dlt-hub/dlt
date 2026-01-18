from copy import deepcopy
from decimal import Decimal
from typing import Any, List
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
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.utils import uniq_id

try:
    from tests.load.sources.sql_database.oracle_source import OracleSourceDB

    from dlt.sources.sql_database import ReflectionLevel, TableBackend, sql_database, sql_table
except Exception:
    pytest.skip(
        "Oracle tests require sqlalchemy oracle dialect and driver", allow_module_level=True
    )

pytestmark = pytest.mark.oracle


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
    # timezone flags: tz column should be tz-aware, ntz depends on reflection level
    assert table["columns"]["some_timestamp_tz"].get("timezone", True) is True
    ntz_flag = reflection_level == "minimal"
    assert table["columns"]["some_timestamp_ntz"].get("timezone", True) is ntz_flag


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow", "pandas"])
@pytest.mark.parametrize("reflection_level", ["minimal", "full", "full_with_precision"])
def test_sql_table_incremental_datetime_ntz(
    oracle_db: OracleSourceDB,
    backend: TableBackend,
    reflection_level: ReflectionLevel,
) -> None:
    table = sql_table(
        credentials=oracle_db.credentials,
        table="app_user",
        schema=oracle_db.schema,
        backend=backend,
        reflection_level=reflection_level,
        incremental=dlt.sources.incremental(
            "some_timestamp_ntz",
            initial_value=ensure_pendulum_datetime_utc("1999-01-01T00:00:00+00:00").naive(),
            row_order="asc",
            range_start="open",
        ),
        chunk_size=10,
    )

    pipeline = make_pipeline("duckdb")
    rc = oracle_db.table_infos["app_user"]["row_count"]
    assert_incremental_chunks(pipeline, table, "some_timestamp_ntz", timezone=False, row_count=rc)


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
