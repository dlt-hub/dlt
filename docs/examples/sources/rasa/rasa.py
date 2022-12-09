from typing import Any, Iterator

import dlt
from dlt.common.typing import StrAny, TDataItem, TDataItems
from dlt.common.time import timestamp_within
from dlt.extract.source import DltResource


@dlt.source
def rasa(
    data_from: DltResource,
    /,
    source_env: str = None,
    initial_timestamp: float = None,
    end_timestamp: float = None,
    store_last_timestamp: bool = True
) -> Any:
    """Transforms the base resource provided in `data_from` into a rasa tracker store raw dataset where each event type get it's own table.
    The resource is a stream resource and it generates tables dynamically from data. The source uses `rasa.schema.yaml` file to initialize the schema

    Args:
        data_from (DltResource): the base resource with raw events data
        source_env (str, optional): the "environment" field value. Defaults to None.
        initial_timestamp (float, optional): filters in only the events past the timestamp. Defaults to None.
        end_timestamp (float, optional): filters in only the events before the timestamp. Defaults to None.
        store_last_timestamp (bool, optional): tells the source if it should preserve last processed timestamp value and resume when it is run again. Defaults to True.

    Returns:
        DltSource: a source with single resource called "events"
    """

    # the data_from can be a file, database table (see sql_query.py), kafka topic, rabbitmq queue etc.
    @dlt.transformer(data_from=data_from)
    def events(source_events: TDataItems) -> Iterator[TDataItem]:
        # recover start_timestamp from state if given
        if store_last_timestamp:
            start_timestamp = max(initial_timestamp or 0, dlt.state().get("start_timestamp", 0))
        # we expect tracker store events here
        last_timestamp: int = None

        def _proc_event(source_event: TDataItem) -> Iterator[TDataItem]:
            nonlocal last_timestamp

            # must be a dict
            assert isinstance(source_event, dict)
            # filter out events
            if timestamp_within(source_event["timestamp"], start_timestamp, end_timestamp):
                # yield tracker table with all-event index
                event_type = source_event["event"]
                last_timestamp = source_event["timestamp"]
                event = {
                    "sender_id": source_event["sender_id"],
                    "timestamp": last_timestamp,
                    "event": event_type
                }
                if source_env:
                    event["source"] = source_env
                if "model_id" in source_event:
                    event["model_id"] = source_event["model_id"]

                yield dlt.with_table_name(event, "event")
                # yield original event
                yield dlt.with_table_name(source_event, "event_" + event_type)

        # events may be a single item or list of items
        if isinstance(source_events, list):
            for source_event in source_events:
                yield from _proc_event(source_event)
        else:
            yield from _proc_event(source_events)

        # write state so we can restart
        if store_last_timestamp and last_timestamp:
            dlt.state()["start_timestamp"] = last_timestamp

    return events
