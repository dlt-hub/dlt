import os
from typing import Any, Callable, Sequence

import dlt
import pytest

from dlt.common import sleep
from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.adapters import databricks_adapter
from dlt.destinations.impl.databricks.factory import DatabricksTypeMapper
from tests.cases import assert_all_data_types_row, table_update_and_row
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info


pytestmark = pytest.mark.essential


def query_rows_eventually(
    dataset: dlt.Dataset,
    query: str,
    rows_ready: Callable[[Sequence[Sequence[Any]]], bool],
    max_attempts: int = 24,
    poll_interval_seconds: int = 5,
) -> list[tuple[Any, ...]]:
    """Run query repeatedly until rows satisfy readiness check or we exhaust attempts.

    Useful for Databricks Zerobus, which provides eventual consistency.
    """

    rows: list[tuple[Any, ...]] = []
    for _ in range(max_attempts):
        rows = dataset(query).fetchall()
        if rows_ready(rows):
            return rows
        sleep(poll_interval_seconds)
    assert rows_ready(rows), f"Timed out waiting for rows to satisfy readiness check: {query}"
    return rows


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(include_cids="databricks_zerobus"),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("file_format", ("parquet", "jsonl"))
def test_databricks_zerobus_data_types(
    destination_config: DestinationTestConfiguration,
    file_format: TLoaderFileFormat,
) -> None:
    """Tests all data types `dlt` supports for `zerobus` insert API."""

    columns, data_row = table_update_and_row(
        exclude_types=tuple(DatabricksTypeMapper.UNSUPPORTED_TYPES[("zerobus", file_format)]),
        # `col12` is `timestamp` without timezone, which is not supported with `jsonl`
        exclude_columns=("col12",) if file_format == "jsonl" else (),
    )

    @dlt.resource(
        write_disposition="append",
        columns=columns,
        file_format=file_format,
    )
    def data_types():
        yield data_row

    databricks_adapter(data_types, insert_api="zerobus")

    # insert row with all supported data types
    pipe = destination_config.setup_pipeline(
        f"test_databricks_zerobus_data_types_{file_format}", dev_mode=True
    )
    info = pipe.run(data_types, **destination_config.run_kwargs)
    assert_load_info(info)

    # assert inserted row has expected values
    selected_columns = ", ".join(columns.keys())
    rows = query_rows_eventually(
        dataset=pipe.dataset(),
        query=f"SELECT {selected_columns} FROM data_types ORDER BY col1 LIMIT 1",
        # "greater than" because Zerobus can deliver duplicates (at-least-once guarantee)
        rows_ready=lambda rows: len(rows) > 0,
    )
    assert_all_data_types_row(
        pipe.destination_client().capabilities,
        rows[0],
        expected_row=data_row,
        schema=columns,
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(include_cids="databricks_zerobus"),
    ids=lambda x: x.name,
)
def test_databricks_zerobus_concurrent_streams(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Asserts multiple Zerobus jobs can stream concurrently into the same table."""

    # NOTE: run this test with `-s` to see Zerobus SDK logs, which show the lifecycle of the
    # multiple streams nicely

    n_rows = 3
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "1"  # force 1 row per file to create multiple jobs
    os.environ["LOAD__WORKERS"] = str(n_rows)  # allow concurrent loads for all rows

    rows = [{"id": i} for i in range(n_rows)]

    @dlt.resource(write_disposition="append")
    def my_resource():
        yield rows

    databricks_adapter(my_resource, insert_api="zerobus")

    pipe = destination_config.setup_pipeline(
        "test_databricks_zerobus_concurrent_streams", dev_mode=True
    )
    info = pipe.run(my_resource)
    assert_load_info(info)

    # pipeline used one job (stream) per row, and all jobs completed successfully
    completed_jobs = [
        job
        for job in info.load_packages[0].jobs["completed_jobs"]
        if job.job_file_info.table_name == my_resource.table_name
    ]
    assert len(completed_jobs) == len(rows)

    # all expected rows are eventually available in the table — we use set comparison because
    # Zerobus can deliver duplicates (at-least-once guarantee)
    expected_rows = [(row["id"],) for row in rows]
    observed_rows = query_rows_eventually(
        dataset=pipe.dataset(),
        query=f"SELECT id FROM {my_resource.table_name} ORDER BY id",
        rows_ready=lambda rows: set(rows) == set(expected_rows),
    )
    assert set(observed_rows) == set(expected_rows)
