from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Type,
    TypedDict,
    NewType,
    Union,
    get_args,
)
from typing_extensions import Never

from dlt.common.data_types import TDataType
from dlt.common.normalizers.typing import TNormalizersConfig
from dlt.common.typing import TSortOrder, TAnyDateTime, TLoaderFileFormat

try:
    from pydantic import BaseModel as _PydanticBaseModel
except ImportError:
    _PydanticBaseModel = Never  # type: ignore[assignment, misc]


# current version of schema engine
SCHEMA_ENGINE_VERSION = 9

# dlt tables
VERSION_TABLE_NAME = "_dlt_version"
LOADS_TABLE_NAME = "_dlt_loads"
PIPELINE_STATE_TABLE_NAME = "_dlt_pipeline_state"
DLT_NAME_PREFIX = "_dlt"

TColumnProp = Literal[
    "name",
    "data_type",
    "nullable",
    "partition",
    "cluster",
    "primary_key",
    "foreign_key",
    "sort",
    "unique",
    "merge_key",
    "root_key",
    "hard_delete",
    "dedup_sort",
]
"""Known properties and hints of the column"""
# TODO: merge TColumnHint with TColumnProp
TColumnHint = Literal[
    "not_null",
    "partition",
    "cluster",
    "primary_key",
    "foreign_key",
    "sort",
    "unique",
    "merge_key",
    "root_key",
    "hard_delete",
    "dedup_sort",
]
"""Known hints of a column used to declare hint regexes."""

TTableFormat = Literal["iceberg", "delta"]
TFileFormat = Literal[Literal["preferred"], TLoaderFileFormat]
TTypeDetections = Literal[
    "timestamp", "iso_timestamp", "iso_date", "large_integer", "hexbytes_to_text", "wei_to_double"
]
TTypeDetectionFunc = Callable[[Type[Any], Any], Optional[TDataType]]
TColumnNames = Union[str, Sequence[str]]
"""A string representing a column name or a list of"""

# COLUMN_PROPS: Set[TColumnProp] = set(get_args(TColumnProp))
COLUMN_HINTS: Set[TColumnHint] = set(
    [
        "partition",
        "cluster",
        "primary_key",
        "foreign_key",
        "sort",
        "unique",
        "merge_key",
        "root_key",
    ]
)


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
    hard_delete: Optional[bool]
    dedup_sort: Optional[TSortOrder]


TTableSchemaColumns = Dict[str, TColumnSchema]
"""A mapping from column name to column schema, typically part of a table schema"""


TAnySchemaColumns = Union[
    TTableSchemaColumns, Sequence[TColumnSchema], _PydanticBaseModel, Type[_PydanticBaseModel]
]

TSimpleRegex = NewType("TSimpleRegex", str)
TColumnName = NewType("TColumnName", str)
SIMPLE_REGEX_PREFIX = "re:"

TSchemaEvolutionMode = Literal["evolve", "discard_value", "freeze", "discard_row"]
TSchemaContractEntities = Literal["tables", "columns", "data_type"]


class TSchemaContractDict(TypedDict, total=False):
    """TypedDict defining the schema update settings"""

    tables: Optional[TSchemaEvolutionMode]
    columns: Optional[TSchemaEvolutionMode]
    data_type: Optional[TSchemaEvolutionMode]


TSchemaContract = Union[TSchemaEvolutionMode, TSchemaContractDict]


class TRowFilters(TypedDict, total=True):
    excludes: Optional[List[TSimpleRegex]]
    includes: Optional[List[TSimpleRegex]]


class NormalizerInfo(TypedDict, total=True):
    new_table: bool


# Part of Table containing processing hints added by pipeline stages
TTableProcessingHints = TypedDict(
    "TTableProcessingHints",
    {
        "x-normalizer": Optional[Dict[str, Any]],
        "x-loader": Optional[Dict[str, Any]],
        "x-extractor": Optional[Dict[str, Any]],
    },
    total=False,
)


TWriteDisposition = Literal["skip", "append", "replace", "merge"]
TLoaderMergeStrategy = Literal["delete-insert", "scd2", "upsert"]


WRITE_DISPOSITIONS: Set[TWriteDisposition] = set(get_args(TWriteDisposition))
MERGE_STRATEGIES: Set[TLoaderMergeStrategy] = set(get_args(TLoaderMergeStrategy))

DEFAULT_VALIDITY_COLUMN_NAMES = ["_dlt_valid_from", "_dlt_valid_to"]
"""Default values for validity column names used in `scd2` merge strategy."""


class TWriteDispositionDict(TypedDict):
    disposition: TWriteDisposition


class TMergeDispositionDict(TWriteDispositionDict, total=False):
    strategy: Optional[TLoaderMergeStrategy]
    validity_column_names: Optional[List[str]]
    active_record_timestamp: Optional[TAnyDateTime]
    boundary_timestamp: Optional[TAnyDateTime]
    row_version_column_name: Optional[str]


TWriteDispositionConfig = Union[TWriteDisposition, TWriteDispositionDict, TMergeDispositionDict]


# TypedDict that defines properties of a table
class TTableSchema(TTableProcessingHints, total=False):
    """TypedDict that defines properties of a table"""

    name: Optional[str]
    description: Optional[str]
    write_disposition: Optional[TWriteDisposition]
    schema_contract: Optional[TSchemaContract]
    table_sealed: Optional[bool]
    parent: Optional[str]
    filters: Optional[TRowFilters]
    columns: TTableSchemaColumns
    resource: Optional[str]
    table_format: Optional[TTableFormat]
    file_format: Optional[TFileFormat]


class TPartialTableSchema(TTableSchema):
    pass


TSchemaTables = Dict[str, TTableSchema]
TSchemaUpdate = Dict[str, List[TPartialTableSchema]]


class TSchemaSettings(TypedDict, total=False):
    schema_contract: Optional[TSchemaContract]
    detections: Optional[List[TTypeDetections]]
    default_hints: Optional[Dict[TColumnHint, List[TSimpleRegex]]]
    preferred_types: Optional[Dict[TSimpleRegex, TDataType]]


class TStoredSchema(TypedDict, total=False):
    """TypeDict defining the schema representation in storage"""

    version: int
    version_hash: str
    previous_hashes: List[str]
    imported_version_hash: Optional[str]
    engine_version: int
    name: str
    description: Optional[str]
    settings: Optional[TSchemaSettings]
    tables: TSchemaTables
    normalizers: TNormalizersConfig
