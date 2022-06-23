
from typing import Iterator, Tuple, Callable

from dlt.common.typing import TEvent, StrAny
from dlt.common.schema import Schema

# type definitions for json normalization function

# iterator of form (table_name, dict) must be returned from normalization function
TUnpackedRowIterator = Iterator[Tuple[str, StrAny]]

# normalization function signature
TNormalizeJSONFunc = Callable[[Schema, TEvent, str], TUnpackedRowIterator]

