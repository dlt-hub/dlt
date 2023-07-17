import typing as t

TDataType = t.Literal[
    "text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei", "date"
]
DATA_TYPES: t.Set[TDataType] = set(t.get_args(TDataType))
