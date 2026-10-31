"""Loads filesystem packages with a layout of all date placeholders on dlt 1.25 and the current dlt.

- "old" (old dlt): loads one package of `items`, then extracts and normalizes a second package
  and leaves it in normalized.
- "load" (current dlt): loads the pending package.

Both phases run two pipelines: `filesystem_layout` renders the date placeholders from the load
package timestamp, `filesystem_layout_fixed` from a naive `current_datetime` string in the
config, which is UTC. Each phase prints the paths of the `items` files as `FILE:<pipeline>:<path>`.

Usage: filesystem_layout_pipeline.py [old|load]
"""
import os
import sys

import dlt
from dlt.destinations import filesystem

LAYOUT = (
    "{table_name}/{YYYY}-{YY}-{Y}/{MMMM}-{MMM}-{MM}-{M}/{DD}-{D}/{HH}-{H}/{mm}-{m}/{ss}-{s}/"
    "{dddd}-{ddd}-{dd}-{d}/{SSSS}-{SSS}-{SS}-{S}/{Q}/{curr_date}/{timestamp}-{timestamp_ms}/"
    "{load_package_timestamp}-{load_package_timestamp_ms}/{load_id}.{file_id}.{ext}"
)
CURRENT_DATETIME = "2024-04-14T13:14:15.123456"
PIPELINES = {"filesystem_layout": None, "filesystem_layout_fixed": CURRENT_DATETIME}


@dlt.resource
def items():
    yield [{"id": 1}]


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "old"
    for pipeline_name, current_datetime in PIPELINES.items():
        if current_datetime:
            os.environ["DESTINATION__FILESYSTEM__CURRENT_DATETIME"] = current_datetime
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=filesystem("fs_layout_data", layout=LAYOUT),
            dataset_name=pipeline_name,
        )
        if phase == "old":
            pipeline.run(items())
            pipeline.extract(items())
            pipeline.normalize()
        else:
            pipeline.load()
        for path in sorted(pipeline._fs_client().list_table_files("items")):
            print(f"FILE:{pipeline_name}:{path}")
