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
    # if destination_config.destination_name == "sqlalchemy_sqlite":
    #     pytest.skip("skip generic ADBC test for sqlite because just a few data types are supported")
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
        yield data_

    info = pipeline.run(complex_resource())
    jobs = get_load_package_jobs(
        info.load_packages[0], "completed_jobs", "complex_resource", ".parquet"
    )
    # there must be a parquet job or adbc is not installed so we fall back to other job type
    assert len(jobs) == 1
    # make sure we can read data back. TODO: verify data types
    rows = pipeline.dataset().table("complex_resource").fetchall()
    assert len(rows) == 1
    rows = pipeline.dataset().table("complex_resource__child").fetchall()
    assert len(rows) == 3

    # load again and make sure we still have 1 record
    pipeline.run(complex_resource())
    assert load_table_counts(pipeline) == {"complex_resource": 1, "complex_resource__child": 3}
