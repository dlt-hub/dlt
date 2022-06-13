import jsonlines
from typing import Iterator, TYPE_CHECKING, Any
if TYPE_CHECKING:
    from _typeshed import StrOrBytesPath
else:
    StrOrBytesPath = Any

from dlt.common import json
from dlt.common.typing import DictStrAny


def get_source(path: StrOrBytesPath) -> Iterator[DictStrAny]:
    with open(path, "r") as f:
        yield from jsonlines.Reader(f, loads=json.loads)
