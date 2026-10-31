"""Reproduces a case where very old schema engine (8) added "seen-data" hint to all tables, also
those including incomplete primary key columns. such tables never received any data. now when
pipeline runs and there's still no data - the run shoud succeed. later when data comes - pipeline
should recompute hints and recover.

Usage: legacy_upsert_pipeline.py [old|current|load]
"""
import sys
from typing import Any

import dlt

# nullable primary key declared on `id`; it is the customer's reflected key that never got a type
COLUMNS: Any = {"id": {"primary_key": True, "nullable": True}}
UPSERT: Any = {"disposition": "merge", "strategy": "upsert"}


@dlt.source(name="legacy_upsert")
def legacy_source(phase: str):
    if phase == "old":
        # old dlt (engine 8): data has no `id`, so the declared primary key stays incomplete
        @dlt.resource(name="renamed_store", columns=COLUMNS, write_disposition="merge")
        def store():
            yield [{"ts": 1, "val": "a"}, {"ts": 2, "val": "b"}]

    elif phase == "load":
        # current dlt: a row inside the incremental window (ts >= 100) is loaded and completes `id`
        @dlt.resource(name="renamed_store", columns=COLUMNS, write_disposition=UPSERT)
        def store(updated=dlt.sources.incremental("ts", initial_value=100)):
            yield [{"id": 1, "ts": 200, "val": "c"}]

    else:
        # current dlt ("current"): upsert with an incremental whose window (ts >= 100) excludes every
        # yielded row, so the resource yields nothing past the cursor - the empty incremental run
        @dlt.resource(name="renamed_store", columns=COLUMNS, write_disposition=UPSERT)
        def store(updated=dlt.sources.incremental("ts", initial_value=100)):
            yield [{"ts": 1, "val": "a"}, {"ts": 2, "val": "b"}]

    return store


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "old"
    pipeline = dlt.pipeline(
        pipeline_name="legacy_upsert",
        destination="duckdb",
        dataset_name="legacy_data",
    )
    info = pipeline.run(legacy_source(phase))
    print(info)
