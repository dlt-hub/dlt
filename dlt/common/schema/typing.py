from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Set, Type, TypedDict, NewType, Union, get_args
from typing_extensions import Never

from dlt.common.data_types import TDataType
from dlt.common.normalizers.typing import TNormalizersConfig

try:
    from pydantic import BaseModel as _PydanticBaseModel
except ImportError:
    _PydanticBaseModel = Never  # type: ignore[assignment, misc]


# current version of schema engine
SCHEMA_ENGINE_VERSION = 6

# dlt tables
VERSION_TABLE_NAME = "_dlt_version"
LOADS_TABLE_NAME = "_dlt_loads"
STATE_TABLE_NAME = "_dlt_pipeline_state"

TColumnProp = Literal["name", "data_type", "nullable", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique", "merge_key", "root_key"]
"""Known properties and hints of the column"""
# TODO: merge TColumnHint with TColumnProp
TColumnHint = Literal["not_null", "partition", "cluster", "primary_key", "foreign_key", "sort", "unique", "root_key", "merge_key"]
"""Known hints of a column used to declare hint regexes."""
TWriteDisposition = Literal["skip", "append", "replace", "merge"]
TTypeDetections = Literal["timestamp", "iso_timestamp", "large_integer", "hexbytes_to_text", "wei_to_double"]
TTypeDetectionFunc = Callable[[Type[Any], Any], Optional[TDataType]]
TColumnNames = Union[str, Sequence[str]]
"""A string representing a column name or a list of"""

COLUMN_PROPS: Set[TColumnProp] = set(get_args(TColumnProp))
COLUMN_HINTS: Set[TColumnHint] = set(["partition", "cluster", "primary_key", "foreign_key", "sort", "unique", "merge_key", "root_key"])
WRITE_DISPOSITIONS: Set[TWriteDisposition] = set(get_args(TWriteDisposition))


class TColumnType(TypedDict, total=False):
    data_type: Optional[TDataType]
    precision: Optional[int]
    scale: Optional[int]


class TColumnSchemaBase(TColumnType, total=False):
    """TypedDict that defines basic properties of a column: name, data type and nullable"""
    name: Optional[str]
    nullable: Optional[bool]


class TColumnSchema(TColumnSchemaBase, total=False):
    """TypedDict that defines additional column hints"""
    description: Optional[str]
    partition: Optional[bool]
    cluster: Optional[bool]
    unique: Optional[bool]
    sort: Optional[bool]
    primary_key: Optional[bool]
    foreign_key: Optional[bool]
    root_key: Optional[bool]
    merge_key: Optional[bool]
    variant: Optional[bool]


TTableSchemaColumns = Dict[str, TColumnSchema]
"""A mapping from column name to column schema, typically part of a table schema"""


TAnySchemaColumns = Union[TTableSchemaColumns, Sequence[TColumnSchema], _PydanticBaseModel, Type[_PydanticBaseModel]]

TSimpleRegex = NewType("TSimpleRegex", str)
TColumnName = NewType("TColumnName", str)
SIMPLE_REGEX_PREFIX = "re:"


class TRowFilters(TypedDict, total=True):
    excludes: Optional[List[TSimpleRegex]]
    includes: Optional[List[TSimpleRegex]]


class TTableSchema(TypedDict, total=False):
    """TypedDict that defines properties of a table"""
    name: Optional[str]
    description: Optional[str]
    write_disposition: Optional[TWriteDisposition]
    table_sealed: Optional[bool]
    parent: Optional[str]
    filters: Optional[TRowFilters]
    columns: TTableSchemaColumns
    resource: Optional[str]


class TPartialTableSchema(TTableSchema):
    pass


TSchemaTables = Dict[str, TTableSchema]
TSchemaUpdate = Dict[str, List[TPartialTableSchema]]

class TSchemaSettings(TypedDict, total=False):
    schema_sealed: Optional[bool]
    detections: Optional[List[TTypeDetections]]
    default_hints: Optional[Dict[TColumnHint, List[TSimpleRegex]]]
    preferred_types: Optional[Dict[TSimpleRegex, TDataType]]


class TStoredSchema(TypedDict, total=False):
    """TypeDict defining the schema representation in storage"""
    version: int
    version_hash: str
    imported_version_hash: Optional[str]
    engine_version: int
    name: str
    description: Optional[str]
    settings: Optional[TSchemaSettings]
    tables: TSchemaTables
    normalizers: TNormalizersConfig
