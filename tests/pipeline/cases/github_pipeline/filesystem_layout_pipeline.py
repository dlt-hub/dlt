"""Creates a filesystem load package on an old dlt and loads it with the current dlt.

- "old" (old dlt): extracts and normalizes `items` but does not load. The package is left in
  normalized. Prints the package creation time from the package state, in milliseconds.
- "load" (current dlt): loads the pending package and prints the paths of the `items` files.

The layout renders the date placeholders and the load package timestamps from the creation time
in the package state written by the old dlt.

Usage: filesystem_layout_pipeline.py [old|load]
"""
import sys

import dlt
from dlt.destinations import filesystem

LAYOUT = (
    "{table_name}/{YYYY}-{MM}-{DD}/{load_package_timestamp}/{load_package_timestamp_ms}/"
    "{load_id}.{file_id}.{ext}"
)


@dlt.resource
def items():
    yield [{"id": 1}]


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "old"
    pipeline = dlt.pipeline(
        pipeline_name="filesystem_layout",
        destination=filesystem("fs_layout_data", layout=LAYOUT),
        dataset_name="layout_data",
    )
    if phase == "old":
        pipeline.extract(items())
        pipeline.normalize()
        load_id = pipeline.list_normalized_load_packages()[0]
        created_at = pipeline.get_load_package_state(load_id)["created_at"]
        print("CREATED_AT_MS:", int(created_at.timestamp() * 1000))
    else:
        print(pipeline.load())
        for path in sorted(pipeline._fs_client().list_table_files("items")):
            print("FILE:", path)
