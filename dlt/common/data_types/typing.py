from typing import Literal, Set, get_args


TDataType = Literal["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei", "date", "time"]
DATA_TYPES: Set[TDataType] = set(get_args(TDataType))
