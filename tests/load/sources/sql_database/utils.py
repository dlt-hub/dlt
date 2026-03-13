from typing import List

import dlt
from dlt.common.typing import TDataItem
from dlt.extract.resource import DltResource
from tests.pipeline.utils import assert_load_info


def assert_incremental_chunks(
    pipeline: dlt.Pipeline, table: DltResource, cursor: str, timezone: bool, row_count: int
) -> None:
    # number of user must be multiply of 10
    assert row_count % 10 == 0
    for _ in range(row_count // 10):
        info = pipeline.run(table.add_limit(1))
        assert_load_info(info)
        assert pipeline.last_trace.last_normalize_info.row_counts[table.name] == 10
        tzinfo = table.state["incremental"][cursor]["last_value"].tzinfo
        if timezone:
            assert tzinfo is not None
        else:
            assert tzinfo is None
    # load but that will be empty
    pipeline.run(table)
    r_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert table.name not in r_counts or r_counts[table.name] == 0


def assert_extracted_uuids_are_strings(column_name: str, item: TDataItem) -> List[str]:
    """Assert that UUID column values are Python str in a yielded data item.

    Works with all backends: sqlalchemy (dicts), pyarrow (Tables), pandas (DataFrames).
    Returns the extracted string values.
    """
    import pyarrow as pa

    if isinstance(item, pa.Table):
        col = item.column(column_name)
        assert pa.types.is_string(col.type) or pa.types.is_large_string(
            col.type
        ), f"Expected string arrow type for {column_name}, got {col.type}"
        return col.to_pylist()
    elif hasattr(item, "iterrows"):
        # pandas DataFrame
        vals = list(item[column_name])
        for val in vals:
            assert isinstance(val, str), f"Expected str, got {type(val).__name__}"
        return vals
    else:
        # sqlalchemy backend yields dicts
        val = item[column_name]
        assert isinstance(val, str), f"Expected str, got {type(val).__name__}"
        return [val]
