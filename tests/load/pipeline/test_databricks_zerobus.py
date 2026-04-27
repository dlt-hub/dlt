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
        rows_ready=lambda rows: len(rows) > 0,
    )
    assert_all_data_types_row(
        pipe.destination_client().capabilities,
        rows[0],
        expected_row=data_row,
        schema=columns,
    )
