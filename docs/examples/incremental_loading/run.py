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