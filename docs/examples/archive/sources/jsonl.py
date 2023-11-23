import itertools
from typing import Iterator, List, Sequence, Union

import dlt
from dlt.common import json
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import StrAny, StrOrBytesPath


def chunk_jsonl(
    path: StrOrBytesPath, chunk_size: int = 20
) -> Union[Iterator[StrAny], Iterator[List[StrAny]]]:
    with open(path, "rb") as f:

        def _iter() -> Iterator[StrAny]:
            yield from map(json.loadb, f)

        if chunk_size == 1:
            yield from _iter()
        else:
            while True:
                chunk = list(itertools.islice(_iter(), chunk_size))
                if chunk:
                    yield chunk
                else:
                    break


jsonl_file = dlt.resource(chunk_jsonl, name="jsonl", spec=BaseConfiguration)


@dlt.resource(name="jsonl")
def jsonl_files(
    paths: Sequence[StrOrBytesPath], chunk_size: int = 20
) -> Union[Iterator[StrAny], Iterator[List[StrAny]]]:
    for path in paths:
        yield from chunk_jsonl(path, chunk_size)
