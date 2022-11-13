import os
import tempfile
from typing import Any, Iterator, TypedDict, cast, Union

import dlt
from dlt.common import json
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.runners.venv import Venv
from dlt.common.typing import DictStrAny, StrAny, StrOrBytesPath, TDataItem, TDataItems

from examples.sources.stdout import json_stdout as singer_process_pipe

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
def get_source_from_stream(singer_messages: Iterator[SingerMessage], state: DictStrAny = None) -> Iterator[TDataItem]:
    last_state = {}
    for msg in singer_messages:
        if msg["type"] == "RECORD":
            # yield record
            msg = cast(SingerRecord, msg)
            yield dlt.with_table_name(msg["record"], msg["stream"])
        if msg["type"] == "STATE":
            msg = cast(SingerState, msg)
            last_state = msg["value"]
    if state is not None:
        state["singer"] = last_state


@dlt.transformer()
def singer_raw_stream(singer_messages: TDataItems, use_state: bool = True) -> Iterator[TDataItem]:
    if use_state:
        state = dlt.state()
    else:
        state = None
    yield from get_source_from_stream(cast(Iterator[SingerMessage], singer_messages), state)


@dlt.source(spec=BaseConfiguration)  # use BaseConfiguration spec to prevent injections
def tap(venv: Venv, tap_name: str, config_file: FilePathOrDict, catalog_file: FilePathOrDict, use_state: bool = True) -> Any:
    # TODO: generate append/replace dispositions and some table/column hints from catalog files

    def as_config_file(config: FilePathOrDict) -> StrOrBytesPath:
        if type(config) is dict:
            fd, tmp_name = tempfile.mkstemp(dir=venv.context.env_dir)
            with os.fdopen(fd, "w") as f:
                json.dump(config, f)
            return cast(str, tmp_name)
        else:
            return config  # type: ignore

    # write config dictionary to temp file in virtual environment if passed as dict
    config_file_path = as_config_file(config_file)

    # process catalog like config
    catalog_file_path = as_config_file(catalog_file)

    @dlt.resource(name=tap_name)
    def singer_messages() -> Iterator[TDataItem]:
        # possibly pass state
        if use_state:
            state = dlt.state()
        else:
            state = None
        if state is not None and state.get("singer"):
            state_params = ("--state", as_config_file(dlt.state()["singer"]))
        else:
            state_params = ()  # type: ignore

        pipe_iterator = singer_process_pipe(venv,
                                        tap_name,
                                        "--config",
                                        os.path.abspath(config_file_path),
                                        "--catalog",
                                        os.path.abspath(catalog_file_path),
                                        *state_params
                                        )
        yield from get_source_from_stream(pipe_iterator, state)  # type: ignore

    return singer_messages
