"""
---
title: Example Template
description: Add desciption here
keywords: [example]
---

This is a template for a new example. This text will show up in the docs.

With this example you will learn to:

* One
* two
* Three

"""

import dlt

if __name__ == "__main__":
    # run a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="example_pipeline", destination="duckdb", dataset_name="example_data"
    )
    # Extract, normalize, and load the data
    load_info = pipeline.run([1, 2, 3], table_name="player")
    print(load_info)
