import os
import pytest

import dlt

from tests.pipeline.utils import load_table_counts
from tests.utils import preserve_environ
from tests.cases import table_update_and_row
from tests.load.pipeline.utils import get_load_package_jobs
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


# NOTE: you need to install ADBC drivers to run this tests using dbc
# dbc install: postgresql, mysql, mssql, sqlite


@pytest.fixture(autouse=True)
def enable_adbc(preserve_environ) -> None:
    os.environ["DISABLE_ADBC_DETECTION"] = "0"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "mssql", "sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_adbc_detection(destination_config: DestinationTestConfiguration) -> None:
    from dlt.destinations._adbc_jobs import has_adbc_driver

    driver = destination_config.destination_name or destination_config.destination_type
    if driver == "postgres":
        from dlt.destinations.impl.postgres.factory import get_adbc_driver_location

        driver = get_adbc_driver_location()
    elif driver == "sqlalchemy_sqlite":
        driver = "sqlite"
    elif driver == "sqlalchemy_mysql":
        driver = "mysql"

    assert has_adbc_driver(driver)[0] is True


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "mssql", "sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_adbc_parquet_loading(destination_config: DestinationTestConfiguration) -> None:
    if destination_config.destination_name == "sqlalchemy_sqlite":
        pytest.skip(
            "ADBC disabled for SQLite due to WAL mmap conflicts between sqlite3 and"
            " adbc_driver_sqlite"
        )
    column_schemas, data_ = table_update_and_row()

    pipeline = destination_config.setup_pipeline("pipeline_adbc", dev_mode=True)

    if destination_config.destination_type in ("postgres", "mssql"):
        del column_schemas["col11_precision"]  # TIME(3) not supported
        if destination_config.destination_type == "postgres":
            del column_schemas["col6_precision"]  # adbc cannot process decimal(6,2)
        else:
            del column_schemas["col7_precision"]  # adbc cannot process fixed binary

    if destination_config.destination_name == "sqlalchemy_sqlite":
        for k, v in column_schemas.items():
            # decimals not supported
            if v["data_type"] in ("decimal", "wei", "time"):
                data_[k] = str(data_[k])
                column_schemas[k]["data_type"] = "text"

    @dlt.resource(
        file_format="parquet", columns=column_schemas, write_disposition="merge", primary_key="col1"
    )
    def complex_resource():
        # add child table
        data_["child"] = [1, 2, 3]
        assert len([data_] * 10) == 10
        yield [data_] * 10

    info = pipeline.run(complex_resource())
    jobs = get_load_package_jobs(
        info.load_packages[0], "completed_jobs", "complex_resource", ".parquet"
    )
    # there must be a parquet job or adbc is not installed so we fall back to other job type
    assert len(jobs) == 1
    # verify row count and selected column values (int and string)
    rows = pipeline.dataset().complex_resource.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == data_["col1"]  # col1 (bigint)
    assert rows[0][4] == data_["col5"]  # col5 (text)
    # verify child table values
    child_rows = pipeline.dataset().complex_resource__child.fetchall()
    assert len(child_rows) == 3
    assert set(row[0] for row in child_rows) == {1, 2, 3}  # value column
    # verify pipeline state table values
    state_rows = pipeline.dataset()._dlt_pipeline_state.fetchall()
    assert len(state_rows) == 1
    assert state_rows[0][2] == "pipeline_adbc"  # pipeline_name column

    # load again and make sure we still have 1 record
    pipeline.run(complex_resource())
    assert load_table_counts(pipeline) == {"complex_resource": 1, "complex_resource__child": 3}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "mssql", "sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_adbc_parquet_with_dlt_load_id(
    destination_config: DestinationTestConfiguration, preserve_environ
) -> None:
    """Test that ADBC loading works with _dlt_load_id column.

    This test verifies the fix for https://github.com/dlt-hub/dlt/issues/3551
    where ADBC drivers (especially MSSQL) fail on dictionary-encoded Arrow arrays.
    """
    if destination_config.destination_name == "sqlalchemy_sqlite":
        pytest.skip(
            "ADBC disabled for SQLite due to WAL mmap conflicts between sqlite3 and"
            " adbc_driver_sqlite"
        )
    import pyarrow as pa

    # Enable add_dlt_load_id for parquet normalizer in extract step
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"

    pipeline = destination_config.setup_pipeline("pipeline_adbc_load_id", dev_mode=True)

    @dlt.resource(file_format="parquet")  # write_disposition="merge", primary_key="id"
    def test_data():
        # Yield Arrow table to test the dictionary encoding fix
        yield [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    info = pipeline.run(test_data(), loader_file_format="parquet")

    # Verify load succeeded
    assert info.loads_ids is not None
    load_id = info.loads_ids[0]

    # Verify parquet jobs were used
    jobs = get_load_package_jobs(info.load_packages[0], "completed_jobs", "test_data", ".parquet")
    assert len(jobs) == 1, "Expected parquet job for ADBC loading"

    # Verify data was loaded with _dlt_load_id column
    rows = pipeline.dataset().table("test_data").fetchall()
    assert len(rows) == 2

    # Check that _dlt_load_id column exists and has correct value
    df = pipeline.dataset().test_data.df()
    assert "_dlt_load_id" in df.columns
    assert all(df["_dlt_load_id"] == load_id)
