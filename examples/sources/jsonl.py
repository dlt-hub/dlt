import jsonlines
import itertools
from typing import Iterator, List, Sequence

import dlt
from dlt.common import json
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.typing import StrAny, StrOrBytesPath


def chunk_jsonl(path: StrOrBytesPath, chunk_size: int = 20) -> Iterator[List[StrAny]]:
    print(path)
    with open(path, "r", encoding="utf-8") as f:
        _iter = jsonlines.Reader(f, loads=json.loads)
        if chunk_size == 1:
            yield from _iter
        else:
            while True:
                chunk = list(itertools.islice(_iter, chunk_size))
                if chunk:
                    yield chunk
                else:
                    break

# TODO: explicit values must take precedence
jsonl_file = dlt.resource(chunk_jsonl, name="jsonl", spec=BaseConfiguration)

@dlt.resource(name="jsonl")
def jsonl_files(paths: Sequence[StrOrBytesPath], chunk_size: int = 20) -> Iterator[List[StrAny]]:
    for path in paths:
        yield from chunk_jsonl(path, chunk_size)
