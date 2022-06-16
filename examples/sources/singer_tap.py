

from typing import Iterator, TypedDict, cast
from dlt.common.sources import with_table_name
from dlt.common.typing import DictStrAny

# from dlt.pipeline.exceptions import MissingDependencyException


class SingerMessage(TypedDict):
    type: str


class SingerRecord(SingerMessage):
    record: DictStrAny
    stream: str


class SingerState(SingerMessage):
    value: DictStrAny

# try:
#     from singer import parse_message_from_obj, Message, RecordMessage, StateMessage
# except ImportError:
#     raise MissingDependencyException("Singer Source", ["python-dlt-singer"], "Singer runtime compatible with DLT")


# pip install ../singer/singer-python
# https://github.com/datamill-co/singer-runner/tree/master/singer_runner
# https://techgaun.github.io/active-forks/index.html#singer-io/singer-python
def get_source(singer_messages: Iterator[SingerMessage], state: DictStrAny = None) -> Iterator[DictStrAny]:
    last_state = {}
    for msg in singer_messages:
        if msg["type"] == "RECORD":
            # yield record
            msg = cast(SingerRecord, msg)
            yield with_table_name(msg["record"], msg["stream"])
        if msg["type"] == "STATE":
            msg = cast(SingerState, msg)
            last_state = msg["value"]
    if state:
        state["singer"] = last_state
