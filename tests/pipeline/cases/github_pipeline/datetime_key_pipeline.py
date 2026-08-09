"""Loads datetime values under a hash-based `_dlt_id`, so an upgrade must not re-key the rows.

`scd2` hashes the whole row, `upsert` hashes the primary key alone - here a datetime column. Both
run before the PUA markers are decoded, so the `isoformat()` string feeds them. Rendering it
differently re-keys every row: scd2 duplicates it, upsert inserts a copy instead of updating.

Usage: datetime_key_pipeline.py [scd2|upsert]
"""
import sys
from datetime import datetime, timedelta, timezone  # noqa: I251
from typing import Any

import dlt

STRATEGIES: Any = {
    "scd2": ({"disposition": "merge", "strategy": "scd2"}, "id"),
    "upsert": ({"disposition": "merge", "strategy": "upsert"}, "occurred_at"),
}

# aware UTC, aware with an offset and naive: each renders differently in the hash
ROWS = [
    {"id": 1, "occurred_at": datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc)},
    {"id": 2, "occurred_at": datetime(2024, 3, 15, 23, 30, tzinfo=timezone(timedelta(hours=2)))},
    {"id": 3, "occurred_at": datetime(2024, 5, 15, 23, 30)},
]


@dlt.source(name="datetime_key")
def datetime_key_source(strategy: str) -> Any:
    write_disposition, primary_key = STRATEGIES[strategy]

    @dlt.resource(name="events", primary_key=primary_key, write_disposition=write_disposition)
    def events() -> Any:
        yield ROWS

    return events


if __name__ == "__main__":
    strategy = sys.argv[1] if len(sys.argv) > 1 else "scd2"
    pipeline = dlt.pipeline(
        pipeline_name="datetime_key",
        destination="duckdb",
        dataset_name="datetime_key_data",
    )
    print(pipeline.run(datetime_key_source(strategy)))
