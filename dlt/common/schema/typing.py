from typing import Dict, List, Literal, Optional, Set, TypedDict

from dlt.common.typing import StrAny


TDataType = Literal["text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"]
THintType = Literal["not_null", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TColumnProp = Literal["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique"]
TWriteDisposition = Literal["skip", "append", "replace", "merge"]

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


class TRowFilters(TypedDict, total=True):
    excludes: Optional[List[str]]
    includes: Optional[List[str]]


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
    json: TJSONNormalizer


class TSchemaSettings(TypedDict, total=False):
    schema_sealed: Optional[bool]
    default_hints: Optional[Dict[THintType, List[str]]]
    preferred_types: Optional[Dict[str, TDataType]]


class TStoredSchema(TypedDict, total=True):
    version: int
    engine_version: int
    name: str
    settings: Optional[TSchemaSettings]
    tables: TSchemaTables
    normalizers: TNormalizersConfig
