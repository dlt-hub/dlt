"""Reads and writes the trace of pipeline `trace_compat` with the dlt version of the venv.

- "read": prints the transaction id of the last trace, None when the trace cannot be loaded
- "run": loads one row and prints the transaction id of the new trace

Usage: trace_compat_pipeline.py [read|run]
"""
import sys

import dlt


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "read"
    pipeline = dlt.pipeline(
        pipeline_name="trace_compat", destination="duckdb", dataset_name="trace_data"
    )
    if phase == "run":
        print(pipeline.run([{"id": 2}], table_name="items"))
    trace = pipeline.last_trace
    print("TRACE:", trace.transaction_id if trace else None)
