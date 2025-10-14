from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    NewType,
    Union,
)
from typing_extensions import Never, NotRequired, Required

from dlt.common.data_types import TDataType
from dlt.common.normalizers.typing import TNormalizersConfig
from dlt.common.typing import (
    TSortOrder,
    TAnyDateTime,
    TLoaderFileFormat,
    TColumnNames,
    TypedDict,
    get_args,
)

try:
    from pydantic import BaseModel as _PydanticBaseModel
except ImportError:
    _PydanticBaseModel = Never  # type: ignore[assignment, misc]


# current version of schema engine
SCHEMA_ENGINE_VERSION = 11

# dlt tables
VERSION_TABLE_NAME = "_dlt_version"
LOADS_TABLE_NAME = "_dlt_loads"
PIPELINE_STATE_TABLE_NAME = "_dlt_pipeline_state"
DLT_NAME_PREFIX = "_dlt"

# default dlt columns
C_DLT_ID = "_dlt_id"
"""unique id of current row"""
C_DLT_LOAD_ID = "_dlt_load_id"
"""load id to identify records loaded in a single load package"""
# NOTE C_DLT_LOAD_ID != C_DLT_LOADS_TABLE_LOAD_ID but they refer to the exact same entity / value.
# They differ for backwards compatiblity reasons
# TODO add schema migration to use `_dlt_load_id` in `_dlt_loads` table
C_DLT_LOADS_TABLE_LOAD_ID = "load_id"
"""load id column in the table {LOADS_TABLE_NAME}. Meant to be joined with {C_DLT_LOAD_ID} of data tables"""
C_CHILD_PARENT_REF_LABEL = "_dlt_parent"
"""Label of the implicit `TTableReference` between a child table and its parent table"""
C_DESCENDANT_ROOT_REF_LABEL = "_dlt_root"
"""Label of the implicit `TTableReference` between a descendant table and its root table"""
C_ROOT_LOAD_REF_LABEL = "_dlt_load"
"""Label of the implicit `TTableReference` between a root table and the _dlt_loads table"""

TColumnProp = Literal[
    "name",
    # data type
    "data_type",
    "precision",
    "scale",
    "timezone",
    "nullable",
    "variant",
    # hints
    "partition",
    "cluster",
    "primary_key",
    "sort",
    "unique",
    "merge_key",
    "row_key",
    "parent_key",
    "root_key",
    "hard_delete",
    "dedup_sort",
]
"""All known properties of the column, including name, data type info and hints"""
COLUMN_PROPS: Set[TColumnProp] = set(get_args(TColumnProp))

TColumnHint = Literal[
    "nullable",
    "partition",
    "cluster",
    "primary_key",
    "sort",
    "unique",
    "merge_key",
    "row_key",
    "parent_key",
    "root_key",
    "hard_delete",
    "dedup_sort",
]
"""Known hints of a column"""
COLUMN_HINTS: Set[TColumnHint] = set(get_args(TColumnHint))


class TColumnPropInfo(NamedTuple):
    name: Union[TColumnProp, str]
    defaults: Tuple[Any, ...] = (None,)
    is_hint: bool = False


_ColumnPropInfos = [
    TColumnPropInfo("name"),
    TColumnPropInfo("data_type"),
    TColumnPropInfo("precision"),
    TColumnPropInfo("scale"),
    TColumnPropInfo("timezone", (True, None)),
    TColumnPropInfo("nullable", (True, None)),
    TColumnPropInfo("variant", (False, None)),
    TColumnPropInfo("partition", (False, None)),
    TColumnPropInfo("cluster", (False, None)),
    TColumnPropInfo("primary_key", (False, None)),
    TColumnPropInfo("sort", (False, None)),
    TColumnPropInfo("unique", (False, None)),
    TColumnPropInfo("merge_key", (False, None)),
    TColumnPropInfo("row_key", (False, None)),
    TColumnPropInfo("parent_key", (False, None)),
    TColumnPropInfo("root_key", (False, None)),
    TColumnPropInfo("hard_delete", (False, None)),
    TColumnPropInfo("dedup_sort", (False, None)),
    # any x- hint with special settings ie. defaults
    TColumnPropInfo("x-active-record-timestamp", (), is_hint=True),  # no default values
]

ColumnPropInfos: Dict[Union[TColumnProp, str], TColumnPropInfo] = {
    info.name: info for info in _ColumnPropInfos
}
# verify column props and column hints infos
for hint in COLUMN_HINTS:
    assert hint in COLUMN_PROPS, f"Hint {hint} must be a column prop"

for prop in COLUMN_PROPS:
    assert prop in ColumnPropInfos, f"Column {prop} has no info, please define"
    if prop in COLUMN_HINTS:
        ColumnPropInfos[prop] = ColumnPropInfos[prop]._replace(is_hint=True)

TTableFormat = Literal["iceberg", "delta", "hive", "native"]
TFileFormat = Literal[Literal["preferred"], TLoaderFileFormat]
TTypeDetections = Literal[
    "timestamp", "iso_timestamp", "iso_date", "large_integer", "hexbytes_to_text", "wei_to_double"
]
TTypeDetectionFunc = Callable[[Type[Any], Any], Optional[TDataType]]


class TColumnType(TypedDict, total=False):
    data_type: Optional[TDataType]
    nullable: Optional[bool]
    precision: Optional[int]
    scale: Optional[int]
    timezone: Optional[bool]


# Part of Column containing processing hints added by the normalize stage
TColumnProcessingHints = TypedDict(
    "TColumnProcessingHints",
    {
        "x-normalizer": Optional[Dict[str, Any]],
    },
    total=False,
)


class TColumnSchemaBase(TColumnType, TColumnProcessingHints, total=False):
    """TypedDict that defines basic properties of a column: name, data type and nullable"""

    name: Optional[str]


class TColumnSchema(TColumnSchemaBase, total=False):
    """TypedDict that defines additional column hints"""

    description: Optional[str]
    partition: Optional[bool]
    cluster: Optional[bool]
    unique: Optional[bool]
    sort: Optional[bool]
    primary_key: Optional[bool]
    row_key: Optional[bool]
    parent_key: Optional[bool]
    root_key: Optional[bool]
    merge_key: Optional[bool]
    variant: Optional[bool]
    hard_delete: Optional[bool]
    dedup_sort: Optional[TSortOrder]
    incremental: Optional[bool]


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
TLoaderReplaceStrategy = Literal["truncate-and-insert", "insert-from-staging", "staging-optimized"]


WRITE_DISPOSITIONS: Set[TWriteDisposition] = set(get_args(TWriteDisposition))
MERGE_STRATEGIES: Set[TLoaderMergeStrategy] = set(get_args(TLoaderMergeStrategy))
REPLACE_STRATEGIES: Set[TLoaderReplaceStrategy] = set(get_args(TLoaderReplaceStrategy))

DEFAULT_VALIDITY_COLUMN_NAMES = ["_dlt_valid_from", "_dlt_valid_to"]
"""Default values for validity column names used in `scd2` merge strategy."""


class TWriteDispositionDict(TypedDict):
    disposition: TWriteDisposition


class TMergeDispositionDict(TWriteDispositionDict):
    strategy: Optional[TLoaderMergeStrategy]


class TDeleteInsertStrategyDict(TMergeDispositionDict):
    deduplicated: Optional[bool]


class TScd2StrategyDict(TMergeDispositionDict, total=False):
    validity_column_names: Optional[List[str]]
    active_record_timestamp: Optional[TAnyDateTime]
    boundary_timestamp: Optional[TAnyDateTime]
    row_version_column_name: Optional[str]


TWriteDispositionConfig = Union[
    TWriteDisposition,
    TWriteDispositionDict,
    TMergeDispositionDict,
    TScd2StrategyDict,
    TDeleteInsertStrategyDict,
]


TReferenceCardinality = Literal[
    "zero_to_one",
    "one_to_zero",
    "zero_to_many",
    "many_to_zero",
    "one_to_many",
    "many_to_one",
    "one_to_one",
    "many_to_many",
]
"""Represents cardinality between `column` (left) and `referenced_column` (right)

Note that cardinality is not symmetric. For example:
- `Author, 0 to many, Book` an author can have 0 to many book
- `Book, 1 to 1, Author` a book must have exactly 1 author

The statement (Author, 0 to many, Book) doesn't imply (Book, many to 0, Author).
"""


class _TTableReferenceBase(TypedDict, total=False):
    """Describes a reference to another table's columns.
    `columns` corresponds to the `referenced_columns` in the referenced table and their order should match.
    """

    label: Optional[str]
    """Text providing semantic information about the reference.

    For example, the label "liked" describe the relationship between `user` and `post` (user.id, "liked", post.id)
    """

    cardinality: Optional[TReferenceCardinality]
    """Cardinality of the relationship between `table.column` (left) and `referenced_table.referenced_column` (right)."""

    columns: Sequence[str]
    """Name of the column(s) from `table`"""

    referenced_table: str
    """Name of the referenced table"""

    referenced_columns: Sequence[str]
    """Name of the columns(s) from `referenced_table`"""


class TTableReferenceInline(_TTableReferenceBase, TypedDict, total=False):
    table: Optional[str]
    """Name of the table.
    When `TTableReference` is defined on a `TTableSchema` (i.e., "inline reference"), the `table`
    value is determined by `TTableSchema["name"]`
    """


# Keep for backwards compatibility
TTableReference = TTableReferenceInline


# Compared to `TTableReference` or `TInlineTableReference`, `table` is required
class TTableReferenceStandalone(_TTableReferenceBase, TypedDict, total=False):
    table: str
    """Name of the table.
    When `TTableReference` is defined on a `TTableSchema` (i.e., "inline reference"), the `table`
    value is determined by `TTableSchema["name"]`
    """


TTableReferenceParam = Sequence[TTableReference]


class _TTableSchemaBase(TTableProcessingHints, total=False):
    name: Optional[str]
    description: Optional[str]
    schema_contract: Optional[TSchemaContract]
    table_sealed: Optional[bool]
    parent: Optional[str]
    filters: Optional[TRowFilters]
    columns: TTableSchemaColumns
    resource: Optional[str]
    table_format: Optional[TTableFormat]
    file_format: Optional[TFileFormat]


class TTableSchema(_TTableSchemaBase, total=False):
    """TypedDict that defines properties of a table"""

    write_disposition: Optional[TWriteDisposition]
    references: Optional[TTableReferenceParam]


class TPartialTableSchema(TTableSchema):
    pass


TSchemaTables = Dict[str, TTableSchema]
TSchemaUpdate = Dict[str, List[TPartialTableSchema]]
TColumnDefaultHint = Literal["not_null", TColumnHint]
"""Allows using not_null in default hints setting section"""


class TSchemaSettings(TypedDict, total=False):
    schema_contract: Optional[TSchemaContract]
    detections: Optional[List[TTypeDetections]]
    default_hints: Optional[Dict[TColumnDefaultHint, List[TSimpleRegex]]]
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
