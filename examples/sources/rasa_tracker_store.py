from typing import Iterator
from dlt.common.sources import with_table_name

from dlt.common.typing import DictStrAny
from dlt.common.time import timestamp_within


def get_source(base_source: Iterator[DictStrAny], source_env: str = None, start_timestamp: float = None, end_timestamp: float = None, state: DictStrAny = None) -> Iterator[DictStrAny]:
    # the base source can be a file, database table (see sql_query.py), kafka topic, rabbitmq queue etc.
    # recover start_timestamp from state if given
    if state:
        start_timestamp = max(start_timestamp or 0, state.get("rasa_tracker_store", 0))
    # we expect tracker store events here
    last_timestamp: int = None
    for source_event in base_source:
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
            yield with_table_name(event, "event")
            # yield original event
            yield with_table_name(source_event, "event_" + event_type)

    # write state so we can restart
    if state and last_timestamp:
        state["rasa_tracker_store"] = last_timestamp
