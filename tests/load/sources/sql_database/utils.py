from datetime import datetime, timedelta, timezone as dt_timezone, tzinfo
from typing import List, Optional, Sequence
from zoneinfo import ZoneInfo

import dlt
from dlt.common.time import ensure_datetime_utc
from dlt.common.typing import TDataItem
from dlt.extract.resource import DltResource
from tests.pipeline.utils import assert_load_info, load_tables_to_dicts

CURSOR_START = datetime(2024, 6, 15, 10, 0)
CYCLING_ZONES: List[tzinfo] = [
    dt_timezone(timedelta(hours=2)),
    dt_timezone(-timedelta(hours=5)),
    dt_timezone(timedelta(hours=5, minutes=45)),
    ZoneInfo("Europe/Warsaw"),
    dt_timezone.utc,
]


def cursor_datetime(i: int, zoned: bool) -> datetime:
    """Returns a datetime that increases with `i` and has sub-millisecond digits.

    With `zoned`, the instant is stored in a zone that cycles with `i`, so wall clock order
    differs from instant order and a dropped offset skips or repeats rows.
    """
    value = CURSOR_START + timedelta(minutes=i, microseconds=(i * 7919) % 1_000_000)
    if not zoned:
        return value
    return value.replace(tzinfo=dt_timezone.utc).astimezone(CYCLING_ZONES[i % len(CYCLING_ZONES)])


def assert_incremental_chunks(
    pipeline: dlt.Pipeline,
    table: DltResource,
    cursor: str,
    timezone: bool,
    row_count: int,
    cursor_values: Optional[Sequence[datetime]] = None,
) -> None:
    """Loads `table` in chunks of 10 rows ordered by `cursor` and checks nothing is skipped.

    If `cursor_values` (source values of all rows) are given, the incremental `last_value` is
    compared to them after each chunk, and the loaded cursor column at the end. Naive values
    are UTC.
    """
    # number of user must be multiply of 10
    assert row_count % 10 == 0
    expected = sorted(ensure_datetime_utc(value) for value in cursor_values or [])
    for chunk in range(row_count // 10):
        info = pipeline.run(table.add_limit(1))
        assert_load_info(info)
        assert pipeline.last_trace.last_normalize_info.row_counts[table.name] == 10
        if expected:
            last_value = table.state["incremental"][cursor]["last_value"]
            assert ensure_datetime_utc(last_value) == expected[chunk * 10 + 9]
    # load but that will be empty
    pipeline.run(table)
    r_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert table.name not in r_counts or r_counts[table.name] == 0
    if expected:
        rows = load_tables_to_dicts(pipeline, table.name)[table.name]
        assert sorted(ensure_datetime_utc(row[cursor]) for row in rows) == expected
    # checked last so that rows skipped or repeated above surface first
    tzinfo = table.state["incremental"][cursor]["last_value"].tzinfo
    if timezone:
        assert tzinfo is not None
    else:
        assert tzinfo is None


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
