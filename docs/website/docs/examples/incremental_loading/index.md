---
title: Load Zendesk tickets incrementally
description: Learn how do incremental loading in consecutive runs
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to do incremental loading in consecutive runs with dlt.
    The state of your incremental loads will be persisted in
    your selected destination and restored and used on each new load,
    making it very easy to keep your loaded dataset up to date with the source."
    slug="incremental_loading"
    run_file="zendesk" />

## Incremental loading with the Zendesk API

In this example, you'll find a Python script that interacts with the Zendesk Support API to extract ticket events data.

We'll learn:

- How to pass [credentials](../../general-usage/credentials) as dict and how to type the `@dlt.source` function arguments.
- How to set [the nesting level](../../general-usage/source#reduce-the-nesting-level-of-generated-tables).
- How to enable [incremental loading](../../general-usage/incremental-loading) for efficient data extraction.
- How to specify [the start and end dates](../../general-usage/incremental-loading#using-dltsourcesincremental-for-backfill) for the data loading and how to [opt-in to Airflow scheduler](../../general-usage/incremental-loading#using-airflow-schedule-for-backfill-and-incremental-loading) by setting `allow_external_schedulers` to `True`.
- How to work with timestamps, specifically converting them to Unix timestamps for incremental data extraction.
- How to use the `start_time` parameter in API requests to retrieve data starting from a specific timestamp.


### Loading code

<!--@@@DLT_SNIPPET_START ./code/zendesk-snippets.py::markdown_source-->
```py
from typing import Optional, Dict, Any, Tuple

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TAnyDateTime
from dlt.sources.helpers.requests import client

@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: Dict[str, str] = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = pendulum.datetime(  # noqa: B008
        year=2000, month=1, day=1
    ),
    end_date: Optional[TAnyDateTime] = None,
):
    """
    Retrieves data from Zendesk Support for tickets events.

    Args:
        credentials: Zendesk credentials (default: dlt.secrets.value)
        start_date: Start date for data extraction (default: 2000-01-01)
        end_date: End date for data extraction (default: None).
            If end time is not provided, the incremental loading will be
            enabled, and after the initial run, only new data will be retrieved.

    Returns:
        DltResource.
    """
    # Convert start_date and end_date to Pendulum datetime objects
    start_date_obj = ensure_pendulum_datetime(start_date)
    end_date_obj = ensure_pendulum_datetime(end_date) if end_date else None

    # Convert Pendulum datetime objects to Unix timestamps
    start_date_ts = start_date_obj.int_timestamp
    end_date_ts: Optional[int] = None
    if end_date_obj:
        end_date_ts = end_date_obj.int_timestamp

    # Extract credentials from secrets dictionary
    auth = (credentials["email"], credentials["password"])
    subdomain = credentials["subdomain"]
    url = f"https://{subdomain}.zendesk.com"

    # we use `append` write disposition, because objects in ticket_events endpoint are never updated
    #  so we do not need to merge
    # we set primary_key so allow deduplication of events by the `incremental` below in the rare case
    #  when two events have the same timestamp
    @dlt.resource(primary_key="id", write_disposition="append")
    def ticket_events(
        timestamp: dlt.sources.incremental[int] = dlt.sources.incremental(
            "timestamp",
            initial_value=start_date_ts,
            end_value=end_date_ts,
            allow_external_schedulers=True,
        ),
    ):
        # URL For ticket events
        # 'https://d3v-dlthub.zendesk.com/api/v2/incremental/ticket_events.json?start_time=946684800'
        event_pages = get_pages(
            url=url,
            endpoint="/api/v2/incremental/ticket_events.json",
            auth=auth,
            data_point_name="ticket_events",
            params={"start_time": timestamp.last_value},
        )
        for page in event_pages:
            yield page
            # stop loading when using end_value and end is reached.
            # unfortunately, Zendesk API does not have the "end_time" parameter, so we stop iterating ourselves
            if timestamp.end_out_of_range:
                return

    return ticket_events
```
<!--@@@DLT_SNIPPET_END ./code/zendesk-snippets.py::markdown_source-->

Run the pipeline:


<!--@@@DLT_SNIPPET_START ./code/zendesk-snippets.py::markdown_pipeline-->
```py
if __name__ == "__main__":
    # create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="zendesk", destination="duckdb", dataset_name="zendesk_data"
    )

    load_info = pipeline.run(zendesk_support())
    print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/zendesk-snippets.py::markdown_pipeline-->

