"""Creates a load package with refresh on an old dlt and loads it with the current dlt.

- "old" (old dlt): run 1 loads `items`. run 2 extracts with refresh="drop_sources" and normalizes
  but does not load - the package with `dropped_tables` in its state is left in normalized.
- "load" (current dlt): loads the pending package and prints refresh and dropped tables.

Usage: refresh_normalized_pipeline.py [old|load]
"""
import sys

import dlt


@dlt.resource(write_disposition="append")
def items():
    yield [{"id": 1, "val": "a"}]


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "old"
    pipeline = dlt.pipeline(
        pipeline_name="refresh_normalized",
        destination="duckdb",
        dataset_name="refresh_data",
    )
    if phase == "old":
        print(pipeline.run(items()))
        # leave a normalized package with refresh drops, do not load it
        pipeline.extract(items(), refresh="drop_sources")
        pipeline.normalize()
        print(pipeline.list_normalized_load_packages())
    else:
        info = pipeline.load()
        print(info)
        package = info.load_packages[0]
        print("REFRESH:", package.refresh)
        print("DROPPED:", sorted(package.dropped_tables or []))
        print("TRUNCATED:", sorted(package.truncated_tables or []))
