---
title: Incremental Loading
description: Learn how do incremental loading in consecutive runs
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to do incremental loading in consecutive runs with dlt.
    The state of your incremental loads will be persisted in
    your selected destination and restored and used on each new load,
    making it very easy to keep your loaded dataset up to date with the source."
    slug="incremental_loading" />

## Doing incremental loads with the Zendesk API

In this example, you'll find a Python script that interacts with the Zendesk Support API to extract ticket events data. Along the way, we'll cover the following key concepts:

- We'll learn how to create a source function for Zendesk Support data extraction.

- We'll understand how to enable incremental loading for efficient data extraction.

- We'll explore how to work with timestamps, specifically converting them to Unix timestamps for incremental data extraction.

- We'll see how to use the `start_time` parameter in API requests to retrieve data starting from a specific timestamp.


### Loading code

<!--@@@DLT_SNIPPET_START ./code/run-snippets.py::example-->
```py
from typing import Iterator, Optional, Dict, Any, Tuple

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TDataItem, TDataItems, TAnyDateTime
from dlt.extract.source import DltResource
from dlt.sources.helpers.requests import client


@dlt.source(max_table_nesting=2)
def zendesk_support(
    credentials=dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = pendulum.datetime(year=2000, month=1, day=1),  # noqa: B008
    end_date: Optional[TAnyDateTime] = None,
) -> DltResource:
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

    @dlt.resource(primary_key="id", write_disposition="append")
    def ticket_events(
        timestamp: dlt.sources.incremental[int] = dlt.sources.incremental(
            "timestamp",
            initial_value=start_date_ts,
            end_value=end_date_ts,
            allow_external_schedulers=True,
        ),
    ) -> Iterator[TDataItem]:
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
            if timestamp.end_out_of_range:
                return

    return ticket_events


def get_pages(
    url: str,
    endpoint: str,
    auth: Tuple[str, str],
    data_point_name: str,
    params: Optional[Dict[str, Any]] = None,
) -> Iterator[TDataItems]:
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
        response = client.get(
            get_url, headers=headers, auth=auth, params=params
        )
        response.raise_for_status()
        response_json = response.json()
        result = response_json[data_point_name]
        yield result

        get_url = None
        # See https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/#json-format
        if not response_json["end_of_stream"]:
            get_url = response_json["next_page"]


# build duckdb pipeline
pipeline = dlt.pipeline(
    pipeline_name="zendesk", destination="duckdb", dataset_name="zendesk_data"
)

load_info = pipeline.run(zendesk_support())
print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/run-snippets.py::example-->


## Example of Zendesk data:

```python
# https://d3v-dlthub.zendesk.com/api/v2/incremental/ticket_events.json?start_time=946684800
{'child_events': [
    {
        'id': 18868153669009,
        'via': 'Web form',
        'via_reference_id': None,
        'subject': 'Another ticket',
        'event_type': 'Change',
        'previous_value': 'Another ticket 1'
    }
],
    'id': 18868153668881,
    'ticket_id': 5,
    'timestamp': 1695680609,
    'created_at': '2023-09-25T22:23:29Z',
    'updater_id': 12765072569105,
    'via': 'Web form',
    'system': {
        'client': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'location': 'Frankfurt am Main, HE, Germany',
        'latitude': 50.1169,
        'longitude': 8.6837
    },
    'event_type': 'Audit'
}]
```
