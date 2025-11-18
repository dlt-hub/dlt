import pytest

import dlt

from tests.cases import table_update_and_row
from tests.load.pipeline.utils import get_load_package_jobs
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


# def test_adbc_detection() -> None:
#     from adbc_driver_manager import dbapi, ProgrammingError
#     import adbc_driver_manager as dm

#     try:
#         db = dm.AdbcDatabase(driver="mssqll")
#         db.close()
#     # try:
#     #     dbapi.connect(driver="postgresql", db_kwargs={"uri": "server"})
#     except ProgrammingError as pr_ex:
#         print(str(pr_ex))
#         print(pr_ex.sqlstate)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "mssql"]),
    ids=lambda x: x.name,
)
def test_adbc_parquet_loading(destination_config: DestinationTestConfiguration) -> None:
    column_schemas, data_types = table_update_and_row()

    pipeline = destination_config.setup_pipeline("pipeline_adbc", dev_mode=True)

    # postgres
    del column_schemas["col6_precision"]  # adbc cannot process decimal(6,2)
    # mssql
    del column_schemas["col7_precision"]  # adbc cannot process fixed binary

    # both
    del column_schemas["col11_precision"]  # TIME(3) not supported

    @dlt.resource(file_format="parquet", columns=column_schemas, max_table_nesting=0)
    def complex_resource():
        yield data_types

    info = pipeline.run(complex_resource())
    jobs = get_load_package_jobs(
        info.load_packages[0], "completed_jobs", "complex_resource", ".parquet"
    )
    # there must be a parquet job or adbc is not installed so we fall back to other job type
    assert len(jobs) == 1
    print(pipeline.dataset().table("complex_resource").fetchall())
