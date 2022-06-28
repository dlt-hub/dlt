import jsonlines
from typing import Iterator

from dlt.common import json
from dlt.common.typing import DictStrAny, StrOrBytesPath


def get_source(path: StrOrBytesPath) -> Iterator[DictStrAny]:
    with open(path, "r", encoding="utf-8") as f:
        yield from jsonlines.Reader(f, loads=json.loads)
