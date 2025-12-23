import pytest

from dlt.common.exceptions import DependencyVersionException
from dlt.common.utils import assert_min_pkg_version

try:
    assert_min_pkg_version(pkg_name="sqlalchemy", version="2.0.0")
except DependencyVersionException:
    pytest.skip("Tests require sql alchemy 2.0.0 or higher", allow_module_level=True)


from tests.load.sources.sql_database.utils import assert_incremental_chunks
from tests.pipeline.utils import assert_load_info

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
