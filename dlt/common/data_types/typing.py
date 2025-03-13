from typing import Literal, Set

from dlt.common.typing import get_args


TDataType = Literal[
    "text",
    "double",
    "bool",
    "timestamp",
    "bigint",
    "binary",
    "json",
    "decimal",
    "wei",
    "date",
    "time",
]
DATA_TYPES: Set[TDataType] = set(get_args(TDataType))
