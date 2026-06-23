"""Runs a single pipeline step (normalize or load) in a polling loop."""

import sys
import time

import dlt

if __name__ == "__main__":
    pipeline_name = sys.argv[1]
    step = sys.argv[2]
    deadline_seconds = float(sys.argv[3])

    # re-construct the pipeline each iteration to pick up state changes
    # (new schema, new packages) written by other processes
    deadline = time.time() + deadline_seconds
    completed = []
    while time.time() < deadline:
        pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
        info = pipeline.normalize() if step == "normalize" else pipeline.load()
        if info is not None and not info.is_empty:
            completed.extend(info.loads_ids)
            break
        time.sleep(0.3)
    # comminicate status back to the calling test
    sys.stdout.write(",".join(completed) if completed else "timeout")
