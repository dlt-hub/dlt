from typing import Any, Callable, Dict, List, Literal, Optional, Set, Type, TypedDict, NewType

from dlt.common.typing import StrAny


TDataType = Literal["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"]
THintType = Literal["not_null", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TColumnProp = Literal["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TWriteDisposition = Literal["skip", "append", "replace", "merge"]
TTypeDetections = Literal["timestamp", "iso_timestamp"]
TTypeDetectionFunc = Callable[[Type[Any], Any], Optional[TDataType]]

DATA_TYPES: Set[TDataType] = set(["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"])
COLUMN_PROPS: Set[TColumnProp] = set(["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"])
COLUMN_HINTS: Set[THintType] = set(["partition", "cluster", "primary_key", "foreign_key", "sort", "unique"])
WRITE_DISPOSITIONS: Set[TWriteDisposition] = set(["skip", "append", "replace", "merge"])


class TColumnBase(TypedDict, total=True):
    name: Optional[str]
    data_type: TDataType
    nullable: bool


class TColumn(TColumnBase, total=False):
    description: Optional[str]
    partition: Optional[bool]
    cluster: Optional[bool]
    unique: Optional[bool]
    sort: Optional[bool]
    primary_key: Optional[bool]
    foreign_key: Optional[bool]


TTableColumns = Dict[str, TColumn]
TSimpleRegex = NewType("TSimpleRegex", str)
TColumnName = NewType("TColumnName", str)
SIMPLE_REGEX_PREFIX = "re:"


class TRowFilters(TypedDict, total=True):
    excludes: Optional[List[TSimpleRegex]]
    includes: Optional[List[TSimpleRegex]]


class TTable(TypedDict, total=False):
    name: Optional[str]
    description: Optional[str]
    write_disposition: Optional[TWriteDisposition]
    table_sealed: Optional[bool]
    parent: Optional[str]
    filters: Optional[TRowFilters]
    columns: TTableColumns


class TPartialTable(TTable):
    pass


TSchemaTables = Dict[str, TTable]
TSchemaUpdate = Dict[str, List[TPartialTable]]


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is consumed by `module`


class TNormalizersConfig(TypedDict, total=True):
    names: str
    detections: Optional[List[TTypeDetections]]
    json: TJSONNormalizer


class TSchemaSettings(TypedDict, total=False):
    schema_sealed: Optional[bool]
    default_hints: Optional[Dict[THintType, List[TSimpleRegex]]]
    preferred_types: Optional[Dict[TSimpleRegex, TDataType]]


class TStoredSchema(TypedDict, total=True):
    version: int
    engine_version: int
    name: str
    settings: Optional[TSchemaSettings]
    tables: TSchemaTables
    normalizers: TNormalizersConfig
