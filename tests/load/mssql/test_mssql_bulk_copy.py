"""End-to-end parquet loading through the native Arrow bulk copy.

The server-free tests for the job itself live in `tests/destinations/test_mssql_bulk_copy.py`.
These need a reachable SQL Server and skip when none is configured.
"""

from typing import Any

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import uniq_id
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials

from tests.pipeline.utils import assert_load_info, load_table_counts

# mark all tests as essential, do not remove
pytestmark = [pytest.mark.essential, pytest.mark.mssql]


@pytest.fixture
def mssql_server() -> None:
    """Skips when no mssql destination credentials are configured."""
    try:
        resolve_configuration(MsSqlCredentials(), sections=("destination", "mssql"))
    except ConfigurationException as conf_ex:
        pytest.skip(f"no mssql destination configured: {conf_ex}")


def test_bulk_copy_loads_parquet_end_to_end(mssql_server: None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mssql_bulk_copy_" + uniq_id(),
        destination=mssql(),
        dataset_name="bulk_copy_data",
        dev_mode=True,
    )

    info = pipeline.run(
        [{"id": i, "name": f"n{i}", "maybe": None if i % 2 else i} for i in range(1000)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1000

    # append a second load file into the same table
    info = pipeline.run(
        [{"id": i, "name": f"n{i}", "maybe": None} for i in range(1000, 1500)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1500


@pytest.mark.parametrize(
    "write_disposition", ["replace", "merge"], ids=["replace", "merge_staging"]
)
def test_bulk_copy_staged_write_dispositions(
    mssql_server: None, write_disposition: TWriteDisposition
) -> None:
    """Both flows land the parquet job in the staging dataset before the followup job runs."""
    pipeline = dlt.pipeline(
        pipeline_name="mssql_bulk_copy_staged_" + uniq_id(),
        destination=mssql(),
        dataset_name="bulk_copy_data",
        dev_mode=True,
    )

    @dlt.resource(name="items", primary_key="id", write_disposition=write_disposition)
    def items(offset: int) -> Any:
        yield from ({"id": i, "name": f"n{i}"} for i in range(offset, offset + 100))

    assert_load_info(pipeline.run(items(0), loader_file_format="parquet"))
    assert load_table_counts(pipeline)["items"] == 100

    assert_load_info(pipeline.run(items(50), loader_file_format="parquet"))
    expected = 100 if write_disposition == "replace" else 150
    assert load_table_counts(pipeline)["items"] == expected
