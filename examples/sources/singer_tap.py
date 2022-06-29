import os
import tempfile
from typing import Iterator, TypedDict, cast, Union

from dlt.common import json
from dlt.common.runners.venv import Venv
from dlt.common.sources import with_table_name
from dlt.common.typing import DictStrAny, StrAny, StrOrBytesPath

from examples.sources.stdout import get_source as get_singer_pipe

# from dlt.pipeline.exceptions import MissingDependencyException
FilePathOrDict = Union[StrAny, StrOrBytesPath]


class SingerMessage(TypedDict):
    type: str  # noqa: A003


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
def get_source_from_stream(singer_messages: Iterator[SingerMessage], state: DictStrAny = None) -> Iterator[DictStrAny]:
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


def get_source(venv: Venv, tap_name: str, config_file: FilePathOrDict, catalog_file: FilePathOrDict, state: DictStrAny = None) -> Iterator[DictStrAny]:
    # write config dictionary to temp file in virtual environment if passed as dict
    config_file_path: StrOrBytesPath = None
    if type(config_file) is dict:
        fd, tmp_name = tempfile.mkstemp(dir=venv.context.env_dir)
        with os.fdopen(fd, "w") as f:
            json.dump(config_file, f)
            config_file_path = tmp_name
    else:
        config_file_path = config_file  # type: ignore

    # process catalog like config
    catalog_file_path: StrOrBytesPath = None
    if type(catalog_file) is dict:
        fd, tmp_name = tempfile.mkstemp(dir=venv.context.env_dir)
        with os.fdopen(fd, "w") as f:
            json.dump(catalog_file, f)
            catalog_file_path = tmp_name
    else:
        catalog_file_path = catalog_file  # type: ignore

    pipe_iterator = get_singer_pipe(venv,
                                    tap_name,
                                    "--config",
                                    os.path.abspath(config_file_path),
                                    "--catalog",
                                    os.path.abspath(catalog_file_path)
                                    )
    yield from get_source_from_stream(pipe_iterator, state)  # type: ignore
