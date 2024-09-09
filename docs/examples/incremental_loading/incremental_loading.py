"""
---
title: Load Zendesk tickets incrementally
description: Learn how do incremental loading in consecutive runs
keywords: [incremental loading, example]
---

In this example, you'll find a Python script that interacts with the Zendesk Support API to extract ticket events data.

We'll learn:

- How to pass [credentials](../general-usage/credentials) as dict and how to type the `@dlt.source` function arguments.
- How to set [the nesting level](../general-usage/source#reduce-the-nesting-level-of-generated-tables).
- How to enable [incremental loading](../general-usage/incremental-loading) for efficient data extraction.
- How to specify [the start and end dates](../general-usage/incremental-loading#using-dltsourcesincremental-for-backfill) for the data loading and how to [opt-in to Airflow scheduler](../general-usage/incremental-loading#using-airflow-schedule-for-backfill-and-incremental-loading) by setting `allow_external_schedulers` to `True`.
- How to work with timestamps, specifically converting them to Unix timestamps for incremental data extraction.
- How to use the `start_time` parameter in API requests to retrieve data starting from a specific timestamp.
"""

# NOTE: this line is only for dlt CI purposes, you may delete it if you are using this example
__source_name__ = "zendesk"

from typing import Optional, Dict, Any, Tuple

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TAnyDateTime
from dlt.sources.helpers.requests import client


@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials: Dict[str, str] = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = pendulum.datetime(year=2000, month=1, day=1),  # noqa: B008
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


def get_pages(
    url: str,
    endpoint: str,
    auth: Tuple[str, str],
    data_point_name: str,
    params: Optional[Dict[str, Any]] = None,
):
    """
    Makes a request to a paginated endpoint and returns a generator of data items per page.

    Args:
        url: The base URL.
        endpoint: The url to the endpoint, e.g. /api/v2/calls
        auth: Credentials for authentication.
        data_point_name: The key which data items are nested under in the response object (e.g. calls)
        params: Optional dict of query params to include in the request.

    Returns:
        Generator of pages, each page is a list of dict data items.
    """
    # update the page size to enable cursor pagination
    params = params or {}
    params["per_page"] = 1000
    headers = None

    # make request and keep looping until there is no next page
    get_url = f"{url}{endpoint}"
    while get_url:
        response = client.get(get_url, headers=headers, auth=auth, params=params)
        response.raise_for_status()
        response_json = response.json()
        result = response_json[data_point_name]
        yield result

        get_url = None
        # See https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#json-format
        if not response_json["end_of_stream"]:
            get_url = response_json["next_page"]


if __name__ == "__main__":
    # create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="zendesk", destination="duckdb", dataset_name="zendesk_data"
    )

    load_info = pipeline.run(zendesk_support())
    print(load_info)

    # check that stuff was loaded
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    assert row_counts["ticket_events"] == 17
