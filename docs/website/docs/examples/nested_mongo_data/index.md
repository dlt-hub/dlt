---
title: Control nested MongoDB data
description: Learn how
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to"
    slug="nested_mongo_data"
    run_file="mongodb" />

## Control nested MongoDB data

In this example, you'll find a Python script that

We'll learn:


### Loading code

<!--@@@DLT_SNIPPET_START ./code/mongodb-snippets.py::markdown_source-->
```py
from typing import Iterator, Optional, Dict, Any, Tuple

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem, TDataItems, TAnyDateTime
from dlt.extract.source import DltResource
from dlt.sources.helpers.requests import client

@dlt.source
def mongodb_source(
    credentials: Dict[str, str]=dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = pendulum.datetime(year=2000, month=1, day=1),  # noqa: B008
    end_date: Optional[TAnyDateTime] = None,
):
    pass
```
<!--@@@DLT_SNIPPET_END ./code/mongodb-snippets.py::markdown_source-->

Run the pipeline:


<!--@@@DLT_SNIPPET_START ./code/mongodb-snippets.py::markdown_pipeline-->
```py
if __name__ == "__main__":
    # create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_name", destination="duckdb", dataset_name="dataset_name"
    )

    load_info = pipeline.run(mongodb_source())
    print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/mongodb-snippets.py::markdown_pipeline-->

