"""Loads datetime values under a hash-based `_dlt_id`, so an upgrade must not re-key the rows.

`scd2` hashes the whole row, `upsert` hashes the primary key alone - here a datetime column. Both
run before the PUA markers are decoded, so the `isoformat()` string feeds them. Rendering it
differently re-keys every row: scd2 duplicates it, upsert inserts a copy instead of updating.

`incremental` hashes the rows on the cursor boundary at extract time, from the Python objects
through `json.dumps`. Rendering a UTC datetime differently invalidates the hashes kept in state,
so the next run loads the boundary rows again.

Usage: datetime_key_pipeline.py [scd2|upsert|incremental]
"""
import sys
from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Any

import dlt

STRATEGIES: Any = {
    "scd2": ({"disposition": "merge", "strategy": "scd2"}, "id", None),
    "upsert": ({"disposition": "merge", "strategy": "upsert"}, "occurred_at", None),
    "incremental": ("append", None, "occurred_at"),
}

# aware UTC, aware with an offset and naive: each renders differently in the hash
ROWS = [
    {"id": 1, "occurred_at": datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)},
    {"id": 2, "occurred_at": datetime(2024, 3, 15, 23, 30, tzinfo=timezone(timedelta(hours=2)))},
    {"id": 3, "occurred_at": datetime(2024, 5, 15, 23, 30)},
]

# all aware UTC, two rows share the max cursor so both sit on the boundary the next run dedups
INCREMENTAL_ROWS = [
    {"id": 1, "occurred_at": datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)},
    {"id": 2, "occurred_at": datetime(2024, 1, 16, 23, 30, tzinfo=timezone.utc)},
    {"id": 3, "occurred_at": datetime(2024, 1, 16, 23, 30, tzinfo=timezone.utc)},
]


@dlt.source(name="datetime_key")
def datetime_key_source(strategy: str) -> Any:
    write_disposition, primary_key, cursor = STRATEGIES[strategy]

    @dlt.resource(name="events", primary_key=primary_key, write_disposition=write_disposition)
    def events(occurred_at: Any = dlt.sources.incremental(cursor) if cursor else None) -> Any:
        yield INCREMENTAL_ROWS if cursor else ROWS

    return events


if __name__ == "__main__":
    strategy = sys.argv[1] if len(sys.argv) > 1 else "scd2"
    pipeline = dlt.pipeline(
        pipeline_name="datetime_key",
        destination="duckdb",
        dataset_name="datetime_key_data",
    )
    print(pipeline.run(datetime_key_source(strategy)))
