import base64
import gzip
from datetime import datetime, date  # noqa: I251
from pendulum.tz import UTC
from typing import (
    Any,
    Dict,
    Mapping,
    Tuple,
    Optional,
    Union,
    Callable,
    Iterable,
    Iterator,
    Sequence,
    List,
)

from dlt import version
from dlt.common.exceptions import MissingDependencyException, DltException
from dlt.common.schema.typing import C_DLT_ID, C_DLT_LOAD_ID, TColumnSchema, TTableSchemaColumns
from dlt.common import logger
from dlt.common.json import json, custom_encode, map_nested_in_place
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import is_nullable_column, dlt_load_id_column
from dlt.common.typing import StrStr, TFileOrPath, TDataItems
from dlt.common.normalizers.naming import NamingConvention

try:
    import pyarrow
    import pyarrow.parquet
    import pyarrow.compute
    import pyarrow.dataset
    from pyarrow.parquet import ParquetFile
    from pyarrow import Table
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyarrow helpers",
        [f"{version.DLT_PKG_NAME}[parquet]"],
        "Install pyarrow to be allow to load arrow tables, panda frames and to use parquet files.",
    )

import ctypes

TAnyArrowItem = Union[pyarrow.Table, pyarrow.RecordBatch]

ARROW_DECIMAL_MAX_PRECISION = 76


class UnsupportedArrowTypeException(DltException):
    """Exception raised when Arrow type conversion failed.

    The setters are used to update the exception with more context
    such as the relevant field and tablea it is caught downstream.
    """

    def __init__(
        self,
        arrow_type: pyarrow.DataType,
        field_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> None:
        self.arrow_type = arrow_type
        self._field_name = field_name if field_name else ""
        self._table_name = table_name if table_name else ""

        msg = self.generate_message(self.arrow_type, self._field_name, self._table_name)
        super().__init__(msg)

    @staticmethod
    def generate_message(arrow_type: pyarrow.DataType, field_name: str, table_name: str) -> str:
        msg = f"Arrow type `{arrow_type}`"
        if field_name:
            msg += f" for field `{field_name}`"
        if table_name:
            msg += f" in table `{table_name}`"

        msg += (
            " is unsupported by dlt. See documentation:"
            " https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas#supported-arrow-data-types"
        )
        return msg

    def _update_message(self) -> None:
        """Modify the `Exception.args` tuple to update message."""
        msg = self.generate_message(self.arrow_type, self.field_name, self.table_name)
        self.args = (msg,)  # must be a tuple

    @property
    def field_name(self) -> str:
        return self._field_name

    @field_name.setter
    def field_name(self, value: str) -> None:
        self._field_name = value
        self._update_message()

    @property
    def table_name(self) -> str:
        return self._table_name

    @table_name.setter
    def table_name(self, value: str) -> None:
        self._table_name = value
        self._update_message()


class PyToArrowConversionException(DltException):
    """Exception raised when converting data to Arrow based on a TableSchema"""

    def __init__(
        self,
        data_type: Optional[str],
        inferred_arrow_type: Optional[pyarrow.DataType] = None,
        field_name: Optional[str] = None,
        table_name: Optional[str] = None,
        details: Optional[str] = None,
    ) -> None:
        self.data_type = data_type
        self.inferred_arrow_type = inferred_arrow_type
        self._field_name = field_name if field_name else ""
        self._table_name = table_name if table_name else ""
        self._details = details if details else ""

        super().__init__()
        self._update_message()

    @staticmethod
    def generate_message(
        data_type: Optional[str],
        inferred_arrow_type: Optional[pyarrow.DataType],
        field_name: str,
        table_name: str,
        details: str,
    ) -> str:
        msg = "Conversion to arrow failed"
        if field_name:
            msg += f" for field `{field_name}`"
        if table_name:
            msg += f" in table `{table_name}`"

        msg += f" with dlt hint `{data_type=:}` and `{inferred_arrow_type=:}`"
        msg += " " + details
        return msg

    def _update_message(self) -> None:
        """Modify the `Exception.args` tuple to update message."""
        msg = self.generate_message(
            self.data_type,
            self.inferred_arrow_type,
            self.field_name,
            self.table_name,
            self._details,
        )
        self.args = (msg,)  # must be a tuple

    @property
    def field_name(self) -> str:
        return self._field_name

    @field_name.setter
    def field_name(self, value: str) -> None:
        self._field_name = value
        self._update_message()

    @property
    def table_name(self) -> str:
        return self._table_name

    @table_name.setter
    def table_name(self, value: str) -> None:
        self._table_name = value
        self._update_message()

    @property
    def details(self) -> str:
        return self._details

    @details.setter
    def details(self, value: str) -> None:
        self._details = value
        self._update_message()


def get_py_arrow_datatype(
    column: TColumnType,
    caps: DestinationCapabilitiesContext,
    tz: str,
) -> Any:
    column_type = column["data_type"]
    if column_type == "text":
        return pyarrow.string()
    elif column_type == "double":
        return pyarrow.float64()
    elif column_type == "bool":
        return pyarrow.bool_()
    elif column_type == "timestamp":
        # sets timezone to None when timezone hint is false
        timezone = tz if column.get("timezone", True) else None
        precision = column.get("precision")
        if precision is None:
            precision = caps.timestamp_precision
        return get_py_arrow_timestamp(precision, timezone)
    elif column_type == "bigint":
        return get_pyarrow_int(column.get("precision"))
    elif column_type == "binary":
        return pyarrow.binary(column.get("precision") or -1)
    elif column_type == "json":
        if (nested_type := column.get("x-nested-type")) and caps.supports_nested_types:
            return deserialize_type(nested_type)  # type: ignore[arg-type]
        else:
            return pyarrow.string()
    elif column_type == "decimal":
        precision, scale = column.get("precision"), column.get("scale")
        if (precision is None) and (scale is None):
            precision_tuple = caps.decimal_precision
        elif precision is None:
            precision_tuple = caps.decimal_precision
            logger.warning(
                f"Received decimal column hint `scale={scale}`, but `precision` not set. Will"
                " assume default destination capability `(precision, scale) ="
                f" {caps.decimal_precision}`"
            )
        elif scale is None:
            # setting scale to 0 when unspecified is a common practice across databases
            precision_tuple = (precision, 0)
            logger.warning(
                f"Received decimal column hint `precision={precision}`, but `scale` not set. "
                "Will assume default destination capability `scale=0`"
            )
        else:
            precision_tuple = (precision, scale)

        return get_py_arrow_numeric(precision_tuple)
    elif column_type == "wei":
        return get_py_arrow_numeric(caps.wei_precision)
    elif column_type == "date":
        return pyarrow.date32()
    elif column_type == "time":
        precision = column.get("precision")
        if precision is None:
            precision = caps.timestamp_precision
        return get_py_arrow_time(precision)
    else:
        raise ValueError(column_type)


def get_py_arrow_timestamp(precision: int, tz: str) -> Any:
    tz = tz if tz else None
    if precision == 0:
        return pyarrow.timestamp("s", tz=tz)
    if precision <= 3:
        return pyarrow.timestamp("ms", tz=tz)
    if precision <= 6:
        return pyarrow.timestamp("us", tz=tz)
    return pyarrow.timestamp("ns", tz=tz)


def get_py_arrow_time(precision: int) -> Any:
    if precision == 0:
        return pyarrow.time32("s")
    elif precision <= 3:
        return pyarrow.time32("ms")
    elif precision <= 6:
        return pyarrow.time64("us")
    return pyarrow.time64("ns")


def get_py_arrow_numeric(precision: Tuple[int, int]) -> Any:
    if precision[0] <= 38:
        return pyarrow.decimal128(*precision)
    if precision[0] <= 76:
        return pyarrow.decimal256(*precision)
    # for higher precision use max precision and trim scale to leave the most significant part
    return pyarrow.decimal256(76, max(0, 76 - (precision[0] - precision[1])))


def get_pyarrow_int(precision: Optional[int]) -> Any:
    if precision is None:
        return pyarrow.int64()
    if precision <= 8:
        return pyarrow.int8()
    elif precision <= 16:
        return pyarrow.int16()
    elif precision <= 32:
        return pyarrow.int32()
    return pyarrow.int64()


def get_column_type_from_py_arrow(
    dtype: pyarrow.DataType, caps: DestinationCapabilitiesContext
) -> TColumnType:
    """Returns (data_type, precision, scale) tuple from pyarrow.DataType"""
    if pyarrow.types.is_string(dtype) or pyarrow.types.is_large_string(dtype):
        return dict(data_type="text")
    elif pyarrow.types.is_floating(dtype):
        return dict(data_type="double")
    elif pyarrow.types.is_boolean(dtype):
        return dict(data_type="bool")
    elif pyarrow.types.is_timestamp(dtype):
        if dtype.unit == "s":
            precision = 0
        elif dtype.unit == "ms":
            precision = 3
        elif dtype.unit == "us":
            precision = 6
        else:
            precision = 9

        if dtype.tz is None:
            return dict(data_type="timestamp", precision=precision, timezone=False)

        return dict(data_type="timestamp", precision=precision)
    elif pyarrow.types.is_date(dtype):
        return dict(data_type="date")
    elif pyarrow.types.is_time(dtype):
        # Time fields in schema are `DataType` instead of `Time64Type` or `Time32Type`
        if dtype == pyarrow.time32("s"):
            precision = 0
        elif dtype == pyarrow.time32("ms"):
            precision = 3
        elif dtype == pyarrow.time64("us"):
            precision = 6
        else:
            precision = 9
        return dict(data_type="time", precision=precision)
    elif pyarrow.types.is_integer(dtype):
        result: TColumnType = dict(data_type="bigint")
        if dtype.bit_width != 64:  # 64bit is a default bigint
            result["precision"] = dtype.bit_width
        return result
    elif pyarrow.types.is_fixed_size_binary(dtype):
        return dict(data_type="binary", precision=dtype.byte_width)
    elif pyarrow.types.is_binary(dtype) or pyarrow.types.is_large_binary(dtype):
        return dict(data_type="binary")
    elif pyarrow.types.is_decimal(dtype):
        return dict(data_type="decimal", precision=dtype.precision, scale=dtype.scale)
    elif pyarrow.types.is_nested(dtype):
        return (
            get_nested_column_type_from_py_arrow(dtype)
            if caps.supports_nested_types
            else dict(data_type="json")
        )
    elif pyarrow.types.is_dictionary(dtype):
        # Dictionary types are essentially categorical encodings. The underlying value_type
        # dictates the "logical" type. We simply delegate to the underlying value_type.
        return get_column_type_from_py_arrow(dtype.value_type, caps)
    elif pyarrow.types.is_null(dtype):
        return {"x-normalizer": {"seen-null-first": True}}  # type: ignore[typeddict-unknown-key]
    else:
        raise UnsupportedArrowTypeException(arrow_type=dtype)


def get_nested_column_type_from_py_arrow(dtype: pyarrow.DataType) -> TColumnType:
    """Creates `json` dlt data type with nested type structure in `x-nested-type` hint.
    Currently the only recognized nested type format is arrow-ipc
    """
    return {"data_type": "json", "x-nested-type": serialize_type(dtype)}  # type: ignore[typeddict-unknown-key]


def serialize_type(dtype: pyarrow.DataType) -> str:
    """Serializes arrow type via arrow ipc as base64 str"""
    schema = pyarrow.schema([pyarrow.field("c", dtype)])
    return "arrow-ipc:" + base64.b64encode(gzip.compress(schema.serialize().to_pybytes())).decode(
        "ascii"
    )


def deserialize_type(type_str: str) -> pyarrow.DataType:
    if type_str.startswith("arrow-ipc:"):
        decompressed = gzip.decompress(base64.b64decode(type_str[10:]))
        schema = pyarrow.ipc.read_schema(pyarrow.BufferReader(decompressed))
        return schema.field(0).type
    else:
        raise TypeError("Cannot deserialize pyarrow type, only arrow-ipc is supported")


def remove_null_columns(item: TAnyArrowItem) -> TAnyArrowItem:
    """Remove all columns of datatype pyarrow.null() from the table or record batch"""
    return remove_columns(
        item, [field.name for field in item.schema if pyarrow.types.is_null(field.type)]
    )


def remove_null_columns_from_schema(schema: pyarrow.Schema) -> Tuple[pyarrow.Schema, bool]:
    """Remove all columns of datatype pyarrow.null() from the schema"""
    fields: List[pyarrow.field] = []
    contains_null: bool = False
    for field in schema:
        if pyarrow.types.is_null(field.type):
            contains_null = True
        else:
            fields.append(field)
    return pyarrow.schema(fields), contains_null


def remove_columns(item: TAnyArrowItem, columns: Sequence[str]) -> TAnyArrowItem:
    """Remove `columns` from Arrow `item`"""
    if not columns:
        return item

    if isinstance(item, pyarrow.Table):
        return item.drop(columns)
    elif isinstance(item, pyarrow.RecordBatch):
        # NOTE: select is available in pyarrow 12 an up
        return item.select([n for n in item.schema.names if n not in columns])  # reverse selection
    else:
        raise ValueError(item)


def append_column(item: TAnyArrowItem, name: str, data: Any) -> TAnyArrowItem:
    """Appends new column to Table or RecordBatch"""
    if isinstance(item, pyarrow.Table):
        return item.append_column(name, data)
    elif isinstance(item, pyarrow.RecordBatch):
        new_field = pyarrow.field(name, data.type)
        return pyarrow.RecordBatch.from_arrays(
            item.columns + [data], schema=item.schema.append(new_field)
        )
    else:
        raise ValueError(item)


def rename_columns(item: TAnyArrowItem, new_column_names: Sequence[str]) -> TAnyArrowItem:
    """Rename arrow columns on Table or RecordBatch, returns same data but with renamed schema"""

    if list(item.schema.names) == list(new_column_names):
        # No need to rename
        return item

    if isinstance(item, pyarrow.Table):
        return item.rename_columns(new_column_names)
    elif isinstance(item, pyarrow.RecordBatch):
        new_fields = [
            field.with_name(new_name) for new_name, field in zip(new_column_names, item.schema)
        ]
        return pyarrow.RecordBatch.from_arrays(item.columns, schema=pyarrow.schema(new_fields))
    else:
        raise TypeError(f"Unsupported data item type: `{type(item)}`")


def fill_empty_source_column_values_with_placeholder(
    table: pyarrow.Table, source_columns: List[str], placeholder: str
) -> pyarrow.Table:
    """
    Replaces empty strings and null values in the specified source columns of an Arrow table with a placeholder string.

    Args:
        table (pa.Table): The input Arrow table.
        source_columns (List[str]): A list of column names to replace empty strings and null values in.
        placeholder (str): The placeholder string to use for replacement.

    Returns:
        pyarrow.Table: The modified Arrow table with empty strings and null values replaced in the specified columns.
    """
    for col_name in source_columns:
        column = table[col_name]
        filled_column = pyarrow.compute.fill_null(column, fill_value=placeholder)
        new_column = pyarrow.compute.replace_substring_regex(
            filled_column, pattern=r"^$", replacement=placeholder
        )
        table = table.set_column(table.column_names.index(col_name), col_name, new_column)
    return table


def should_normalize_arrow_schema(
    schema: pyarrow.Schema,
    columns: TTableSchemaColumns,
    naming: NamingConvention,
) -> Tuple[bool, Mapping[str, str], Dict[str, str], Dict[str, bool], TTableSchemaColumns]:
    """Figure out if any of the normalization steps must be executed. This prevents
    from rewriting arrow tables when no changes are needed. Refer to `normalize_py_arrow_item`
    for a list of normalizations. Note that `column` must be already normalized.
    """
    schema, contains_null_cols = remove_null_columns_from_schema(schema)

    rename_mapping = get_normalized_arrow_fields_mapping(schema, naming)
    # no clashes in rename ensured above
    rev_mapping = {v: k for k, v in rename_mapping.items()}
    nullable_mapping = {k: is_nullable_column(v) for k, v in columns.items()}
    # All fields from arrow schema that have nullable set to different value than in columns
    # Key is the renamed column name
    nullable_updates: Dict[str, bool] = {}
    for field in schema:
        norm_name = rename_mapping[field.name]
        if norm_name in nullable_mapping and field.nullable != nullable_mapping[norm_name]:
            nullable_updates[norm_name] = nullable_mapping[norm_name]

    dlt_load_id_col = naming.normalize_identifier(C_DLT_LOAD_ID)
    dlt_id_col = naming.normalize_identifier(C_DLT_ID)
    dlt_columns = {dlt_load_id_col, dlt_id_col}

    # remove all columns that are dlt columns but are not present in arrow schema. we do not want to add such columns
    # that should happen in the normalizer
    columns = {
        name: column
        for name, column in columns.items()
        if name not in dlt_columns or name in rev_mapping
    }

    # check if nothing to rename
    skip_normalize = (
        (list(rename_mapping.keys()) == list(rename_mapping.values()) == list(columns.keys()))
        and not nullable_updates
        and not contains_null_cols
    )
    return (
        not skip_normalize,
        rename_mapping,
        rev_mapping,
        nullable_updates,
        columns,
    )


def normalize_py_arrow_item(
    item: TAnyArrowItem,
    columns: TTableSchemaColumns,
    naming: NamingConvention,
    caps: DestinationCapabilitiesContext,
) -> TAnyArrowItem:
    """Normalize arrow `item` schema according to the `columns`. Note that
    columns must be already normalized.

    0. columns with no data type will be dropped
    1. arrow schema field names will be normalized according to `naming`
    2. arrows columns will be reordered according to `columns`
    3. empty columns will be inserted if they are missing, types will be generated using `caps`
    4. arrow columns with different nullability than corresponding schema columns will be updated
    """
    item = remove_null_columns(item)
    schema = item.schema
    should_normalize, rename_mapping, rev_mapping, nullable_updates, columns = (
        should_normalize_arrow_schema(schema, columns, naming)
    )
    if not should_normalize:
        return item

    new_fields = []
    new_columns = []

    for column_name, column in columns.items():
        # get original field name
        field_name = rev_mapping.pop(column_name, column_name)
        if field_name in rename_mapping:
            idx = schema.get_field_index(field_name)
            new_field = schema.field(idx).with_name(column_name)
            if column_name in nullable_updates:
                # Set field nullable to match column
                new_field = new_field.with_nullable(nullable_updates[column_name])
            # use renamed field
            new_fields.append(new_field)
            new_columns.append(item.column(idx))
        else:
            # column does not exist in pyarrow. create empty field and column
            new_field = pyarrow.field(
                column_name,
                get_py_arrow_datatype(column, caps, "UTC"),
                nullable=is_nullable_column(column),
            )
            new_fields.append(new_field)
            new_columns.append(pyarrow.nulls(item.num_rows, type=new_field.type))

    # add the remaining columns
    for column_name, field_name in rev_mapping.items():
        idx = schema.get_field_index(field_name)
        # use renamed field
        new_fields.append(schema.field(idx).with_name(column_name))
        new_columns.append(item.column(idx))

    # create desired type
    return item.__class__.from_arrays(new_columns, schema=pyarrow.schema(new_fields))


def add_dlt_load_id_column(
    item: TAnyArrowItem,
    columns: TTableSchemaColumns,
    caps: DestinationCapabilitiesContext,
    naming: NamingConvention,
    load_id: str,
) -> TAnyArrowItem:
    """
    Adds or replaces the `_dlt_load_id` column.
    """
    dlt_load_id_col_name = naming.normalize_identifier(C_DLT_LOAD_ID)

    idx = item.schema.get_field_index(dlt_load_id_col_name)
    # if the column already exists, get rid of it
    if idx != -1:
        item = remove_columns(item, dlt_load_id_col_name)

    # get pyarrow.string() type
    pyarrow_string = get_py_arrow_datatype(
        # use already existing column definition or use the default
        # NOTE: the existence of the load id column is ensured by this time
        # since it is added in _compute_tables before files are written
        (
            columns[dlt_load_id_col_name]
            if dlt_load_id_col_name in columns
            else dlt_load_id_column()
        ),
        caps,
        "UTC",  # ts is irrelevant to get pyarrow string, but it's required...
    )

    # add the column with the new value at previous index or append
    item = add_constant_column(
        item=item,
        name=dlt_load_id_col_name,
        data_type=pyarrow_string,
        value=load_id,
        nullable=(
            columns[dlt_load_id_col_name]["nullable"]
            if dlt_load_id_col_name in columns
            else dlt_load_id_column()["nullable"]
        ),
        index=idx,
    )

    return item


def get_normalized_arrow_fields_mapping(schema: pyarrow.Schema, naming: NamingConvention) -> StrStr:
    """Normalizes schema field names and returns mapping from original to normalized name. Raises on name collisions"""
    # use normalize_path to be compatible with how regular columns are normalized in dlt.Schema
    norm_f = naming.normalize_path
    name_mapping = {n.name: norm_f(n.name) for n in schema}
    # verify if names uniquely normalize
    normalized_names = set(name_mapping.values())
    if len(name_mapping) != len(normalized_names):
        raise NameNormalizationCollision(
            f"Arrow schema fields normalized from:\n{list(name_mapping.keys())}:\nto:\n"
            f" {list(normalized_names)}"
        )
    return name_mapping


def py_arrow_to_table_schema_columns(
    schema: pyarrow.Schema, caps: DestinationCapabilitiesContext
) -> TTableSchemaColumns:
    """Convert a PyArrow schema to a table schema columns dict.

    Args:
        schema (pyarrow.Schema): pyarrow schema

    Returns:
        TTableSchemaColumns: table schema columns
    """
    result: TTableSchemaColumns = {}
    for field in schema:
        try:
            converted_type = get_column_type_from_py_arrow(field.type, caps)
        except UnsupportedArrowTypeException as e:
            # modify attributes inplace to add context instead of re-raising with `raise e`
            e.field_name = field.name
            raise

        result[field.name] = {
            "name": field.name,
            "nullable": field.nullable,
            **converted_type,
        }
    return result


def columns_to_arrow(
    columns: TTableSchemaColumns,
    caps: DestinationCapabilitiesContext,
    timestamp_timezone: str = "UTC",
) -> pyarrow.Schema:
    """Convert a table schema columns dict to a pyarrow schema.

    Args:
        columns (TTableSchemaColumns): table schema columns

    Returns:
        pyarrow.Schema: pyarrow schema

    """
    caps = caps or DestinationCapabilitiesContext.generic_capabilities()
    return pyarrow.schema(
        [
            pyarrow.field(
                name,
                get_py_arrow_datatype(
                    schema_item,
                    caps,
                    timestamp_timezone,
                ),
                nullable=schema_item.get("nullable", True),
            )
            for name, schema_item in columns.items()
            if schema_item.get("data_type") is not None
        ]
    )


def get_parquet_metadata(parquet_file: TFileOrPath) -> Tuple[int, pyarrow.Schema]:
    """Gets parquet file metadata (including row count and schema)

    Args:
        parquet_file (str): path to parquet file

    Returns:
        FileMetaData: file metadata
    """
    with pyarrow.parquet.ParquetFile(parquet_file) as reader:
        return reader.metadata.num_rows, reader.schema_arrow


def is_arrow_item(item: Any) -> bool:
    return isinstance(item, (pyarrow.Table, pyarrow.RecordBatch))


def to_arrow_scalar(value: Any, arrow_type: pyarrow.DataType) -> Any:
    """Converts python value to an arrow compute friendly version"""
    return pyarrow.scalar(value, type=arrow_type)


def from_arrow_scalar(arrow_value: pyarrow.Scalar) -> Any:
    """Converts arrow scalar into Python type."""
    return arrow_value.as_py()


TNewColumns = Sequence[Tuple[int, pyarrow.Field, Callable[[pyarrow.Table], Iterable[Any]]]]
"""Sequence of tuples: (field index, field, generating function)"""


def add_constant_column(
    item: TAnyArrowItem,
    name: str,
    data_type: pyarrow.DataType,
    value: Any = None,
    nullable: bool = True,
    index: int = -1,
) -> TAnyArrowItem:
    """Add column with a single value to the table.

    Args:
        item: Arrow table or record batch
        name: The new column name
        data_type: The data type of the new column
        nullable: Whether the new column is nullable
        value: The value to fill the new column with
        index: The index at which to insert the new column. Defaults to -1 (append)
    Note:
        This function creates a dictionary field for the new column, which is memory-efficient
        when the column contains a single repeated value.
        The column is created as a DictionaryArray with int8 indices.
    """
    dictionary = pyarrow.array([value], type=data_type)
    zero_buffer = pyarrow.allocate_buffer(item.num_rows, resizable=False)
    ctypes.memset(zero_buffer.address, 0, item.num_rows)

    indices = pyarrow.Array.from_buffers(
        pyarrow.int8(),
        item.num_rows,
        [None, zero_buffer],  # None validity bitmap means arrow assumes all entries are valid
    )
    dict_array = pyarrow.DictionaryArray.from_arrays(indices, dictionary)

    field = pyarrow.field(name, dict_array.type, nullable=nullable)
    if index == -1:
        return item.append_column(field, dict_array)
    return item.add_column(index, field, dict_array)


def pq_stream_with_new_columns(
    parquet_file: TFileOrPath, columns: TNewColumns, row_groups_per_read: int = 1
) -> Iterator[pyarrow.Table]:
    """Add column(s) to the table in batches.

    The table is read from parquet `row_groups_per_read` row groups at a time

    Args:
        parquet_file: path or file object to parquet file
        columns: list of columns to add in the form of (insertion index, `pyarrow.Field`, column_value_callback)
            The callback should accept a `pyarrow.Table` and return an array of values for the column.
        row_groups_per_read: number of row groups to read at a time. Defaults to 1.

    Yields:
        `pyarrow.Table` objects with the new columns added.
    """
    with pyarrow.parquet.ParquetFile(parquet_file) as reader:
        n_groups = reader.num_row_groups
        # Iterate through n row groups at a time
        for i in range(0, n_groups, row_groups_per_read):
            tbl: pyarrow.Table = reader.read_row_groups(
                range(i, min(i + row_groups_per_read, n_groups))
            )
            for idx, field, gen_ in columns:
                if idx == -1:
                    tbl = tbl.append_column(field, gen_(tbl))
                else:
                    tbl = tbl.add_column(idx, field, gen_(tbl))
            yield tbl


def cast_arrow_schema_types(
    schema: pyarrow.Schema,
    type_map: Dict[Callable[[pyarrow.DataType], bool], Callable[..., pyarrow.DataType]],
) -> pyarrow.Schema:
    """Returns type-casted Arrow schema.

    Replaces data types for fields matching a type check in `type_map`.
    Type check functions in `type_map` are assumed to be mutually exclusive, i.e.
    a data type does not match more than one type check function.
    """
    for i, e in enumerate(schema.types):
        for type_check, cast_type in type_map.items():
            if type_check(e):
                if callable(cast_type):
                    cast_type = cast_type(e)
                adjusted_field = schema.field(i).with_type(cast_type)
                schema = schema.set(i, adjusted_field)
                break  # if type matches type check, do not do other type checks
    return schema


def concat_batches_and_tables_in_order(
    tables_or_batches: Iterable[Union[pyarrow.Table, pyarrow.RecordBatch]]
) -> pyarrow.Table:
    """Concatenate iterable of tables and batches into a single table, preserving row order. Zero copy is used during
    concatenation so schemas must be identical.
    """
    batches = []
    tables = []
    for item in tables_or_batches:
        if isinstance(item, pyarrow.RecordBatch):
            batches.append(item)
        elif isinstance(item, pyarrow.Table):
            if batches:
                tables.append(pyarrow.Table.from_batches(batches))
                batches = []
            tables.append(item)
        else:
            raise ValueError(f"Unsupported type: `{type(item)}`")
    if batches:
        tables.append(pyarrow.Table.from_batches(batches))
    # "none" option ensures 0 copy concat
    return pyarrow.concat_tables(tables, promote_options="none")


def transpose_rows_to_columns(
    rows: TDataItems, column_names: Iterable[str]
) -> dict[str, Any]:  # dict[str, np.ndarray]
    """Transpose rows (data items) into columns (numpy arrays). Returns a dictionary of {column_name: column_data}

    Uses pandas if available. Otherwise, use numpy, which is slower
    """
    try:
        from dlt.common.libs.numpy import numpy as np
    except MissingDependencyException:
        raise MissingDependencyException(
            "dlt pyarrow helpers", ["numpy"], "Numpy is required for this pyarrow operation"
        )

    try:
        from pandas._libs import lib

        # NOTE: this is part of public interface now via DataFrame.from_records()
        pivoted_rows = lib.to_object_array_tuples(rows).T
    except ImportError:
        logger.info(
            "Pandas not installed, reverting to numpy.asarray to create a table which is slower"
        )
        pivoted_rows = np.asarray(rows, dtype="object", order="K").T
    return {
        column_name: data.ravel()
        for column_name, data in zip(column_names, np.vsplit(pivoted_rows, len(pivoted_rows)))
    }


def convert_numpy_to_arrow(
    column_data: Any,  # 1-dimensional np.ndarray
    caps: DestinationCapabilitiesContext,
    column_schema: TColumnSchema,
    tz: str,
    safe_arrow_conversion: bool,
) -> Any:  # pyarrow.Array
    """Convert a numpy array to a pyarrow array.

    Args:
        rows: data items
        caps: capabilities of the storage backend
        columns: dlt hints about the table columns (e.g., data type, nullabe)
        tz: time zone identifier
        safe_arrow_conversion: if False, truncation and loss of precision is allowed
            ref: https://arrow.apache.org/docs/python/generated/pyarrow.compute.CastOptions.html#pyarrow.compute.CastOptions

    Returns:
        an arrow Array
    """
    from dlt.common.libs.pyarrow import pyarrow as pa

    dlt_data_type = column_schema.get("data_type")
    inferred_arrow_type = (
        get_py_arrow_datatype(column_schema, caps, tz) if dlt_data_type is not None else None
    )
    arrow_array = None

    # base case (0): allow pyarrow to infer type, or create array of dlt specified type
    try:
        # type=None lets pyarrow infer the type from the data
        arrow_array = pa.array(column_data, type=inferred_arrow_type)
    # detailed error handling should happen in fallback cases
    except (pa.ArrowInvalid, pyarrow.ArrowTypeError):
        logger.warning(
            f"Default conversion to `{inferred_arrow_type}` for `data_type={dlt_data_type}` failed."
            " Using fallback strategies."
        )

    # case 1: pyarrow infers the type (e.g., float, string) THEN cast it to the dlt specified type; less constraints than the base case
    # for example, this handles when backends return decimals as floats or strings
    if arrow_array is None and dlt_data_type is not None:
        try:
            arrow_array = pa.array(column_data).cast(
                inferred_arrow_type, safe=safe_arrow_conversion
            )
        except (pa.ArrowInvalid, pyarrow.ArrowTypeError, pyarrow.ArrowNotImplementedError) as e:
            # TODO add specific error handling as we encounter them
            error_msg = e.args[0]
            if (
                "would cause data loss"
                in error_msg  # specific pyarrow error related to precision loss (i.e., conversion to decimal)
                and dlt_data_type == "decimal"
                and pa.types.is_decimal(inferred_arrow_type)
            ):
                # TODO provide user interface for safe_arrow_conversion=False and include in this error message
                raise PyToArrowConversionException(
                    data_type=dlt_data_type,
                    inferred_arrow_type=inferred_arrow_type,
                    details=(
                        "Insufficient decimal precision. Consider setting `precision` and `scale`"
                        " hints: https://dlthub.com/docs/general-usage/schema/#tables-and-columns"
                    ),
                ) from e

            elif (
                "to utf8 using function cast_string" in error_msg
                and dlt_data_type in ("json", "text")
                and pa.types.is_string(inferred_arrow_type)
            ):
                # this is handled by fallback case 3
                logger.warning(
                    f"Received `data_type='{dlt_data_type}'`, data requires serialization to"
                    " string, slowing extraction. Cast the JSON field to STRING in your database"
                    " system to improve performance. For example, create and extract data from an"
                    " SQL VIEW that SELECT with CAST."
                )

    # case 2: encode Sequence and Mapping types (list, tuples, set, dict, etc.) to JSON strings
    # This logic needs to be before case 3, otherwise pyarrow might infer the deserialized JSON object as a `pyarrow.struct` instead of `pyarrow.string`
    if arrow_array is None and dlt_data_type in (
        "json",
        "text",
    ):  # depending on the backend, JSON columns are inferred as data_type="text"
        json_serialized_values: list[Union[bytes, None]] = []
        for value in column_data:
            if value is None:
                json_serialized_values.append(None)
                continue
            try:
                json_serialized_values.append(json.dumpb(value))
            except TypeError as e:
                raise PyToArrowConversionException(
                    data_type=dlt_data_type,
                    inferred_arrow_type=inferred_arrow_type,
                    details="dlt failed to a JSON-serializable type.",
                ) from e

        arrow_array = pa.array(json_serialized_values).cast(pa.string())

    # case 3: encode Python types unsupported by Arrow. Simple types are converted to strings and complex types to common structures (dict, list)
    # This catches specialized SQL types like `Ranges`
    if arrow_array is None and dlt_data_type is None:
        try:
            arrow_array = pa.array(column_data)
        except (pa.ArrowInvalid, pyarrow.ArrowTypeError):
            logger.warning(
                "Type can't be inferred by `pyarrow`. Values will be encoded as in a loop, slowing"
                " extraction."
            )
            encoded_values: list[Union[None, Mapping[Any, Any], Sequence[Any], str]] = []
            for value in column_data:
                if value is None:
                    encoded_values.append(None)
                    continue
                try:
                    # the 3 types match those supported by `map_nested_in_place()`
                    if isinstance(value, (tuple, dict, list)):
                        encoded_value = map_nested_in_place(custom_encode, value)
                    # convert set to list
                    elif isinstance(value, set):
                        encoded_value = map_nested_in_place(custom_encode, list(value))
                    # no nesting
                    else:
                        encoded_value = custom_encode(value)  # type: ignore[assignment]
                    encoded_values.append(encoded_value)
                except TypeError as e:
                    raise PyToArrowConversionException(
                        data_type=dlt_data_type,
                        inferred_arrow_type=inferred_arrow_type,
                        details="dlt failed to encode values to an Arrow-compatible type.",
                    ) from e

            arrow_array = pa.array(encoded_values)

    if arrow_array is None:
        raise PyToArrowConversionException(
            data_type=dlt_data_type,
            inferred_arrow_type=inferred_arrow_type,
            details="This data type seems currently unsupported by dlt. Please open a GitHub issue",
        )

    return arrow_array


def row_tuples_to_arrow(
    rows: TDataItems,
    caps: DestinationCapabilitiesContext,
    columns: TTableSchemaColumns,
    tz: str,
    safe_arrow_conversion: bool = True,
) -> Any:  # pyarrow.Table
    """Converts the rows to an arrow table using the columns schema.
    1. Pivot rows into columns.
    2. Convert columns to pyarrow arrays; coerce type and nullability; exclude types unsupported by arrow
    3. Create table
    4. Remove columns full of null values

    Args:
        rows: data items
        caps: capabilities of the storage backend
        columns: dlt hints about the table columns (e.g., data type, nullabe)
        tz: time zone identifier
        safe_arrow_conversion: if False, truncation and loss of precision is allowed
            ref: https://arrow.apache.org/docs/python/generated/pyarrow.compute.CastOptions.html#pyarrow.compute.CastOptions

    Returns:
        an arrow Table
    """
    from dlt.common.libs.pyarrow import pyarrow as pa

    columnar = transpose_rows_to_columns(rows, column_names=columns.keys())

    arrow_arrays = []
    arrow_fields = []
    for column_name, column_data in columnar.items():
        column_schema = columns[column_name]

        try:
            arrow_array = convert_numpy_to_arrow(
                column_data, caps, column_schema, tz, safe_arrow_conversion
            )
        # TODO if converting to arrow fail, should we raise or skip column?
        except PyToArrowConversionException as e:
            e.field_name = column_name
            raise e

        field = pa.field(
            name=column_name, type=arrow_array.type, nullable=column_schema.get("nullable", True)
        )
        arrow_arrays.append(arrow_array)
        arrow_fields.append(field)

    # NOTE careful when casting, modifying types, or enforcing schemas in place. Arrow issues are common
    # This can corrupt the data when writing to Parquet
    # ref: https://github.com/apache/arrow/issues/43146
    # ref: https://github.com/apache/arrow/issues/41667
    arrow_table = pa.Table.from_arrays(arrow_arrays, schema=pa.schema(arrow_fields))
    return arrow_table


class NameNormalizationCollision(ValueError):
    def __init__(self, reason: str) -> None:
        msg = f"Arrow column name collision after input data normalization. {reason}"
        super().__init__(msg)


def add_arrow_metadata(
    item: Union[pyarrow.Table, pyarrow.RecordBatch], metadata: dict[str, Any]
) -> pyarrow.Table:
    # Get current metadata or initialize empty
    schema = item.schema
    current = schema.metadata or {}

    # Convert new metadata to bytes and merge
    update = {k.encode("utf-8"): v.encode("utf-8") for k, v in metadata.items()}
    merged = current.copy()
    merged.update(update)

    # Apply updated schema
    new_schema = schema.with_metadata(merged)

    # Rebuild the object with updated schema
    if isinstance(item, pyarrow.Table):
        return pyarrow.Table.from_arrays(item.columns, schema=new_schema)
    else:  # RecordBatch
        return pyarrow.RecordBatch.from_arrays(item.columns, schema=new_schema)


def set_plus0000_timezone_to_utc(tbl: pyarrow.Table) -> pyarrow.Table:
    """
    Convert any +00:00 timestamp columns to UTC.
    Returns the original table object if nothing needed fixing.
    """
    arrays, fields = [], []
    changed = False

    for col, fld in zip(tbl.columns, tbl.schema):
        if pyarrow.types.is_timestamp(fld.type) and fld.type.tz == "+00:00":
            changed = True
            new_type = pyarrow.timestamp(fld.type.unit, "UTC")
            arrays.append(pyarrow.compute.cast(col, new_type))
            fields.append(pyarrow.field(fld.name, new_type, fld.nullable, fld.metadata))
        else:
            arrays.append(col)
            fields.append(fld)

    if not changed:
        return tbl  # perfect no-op

    new_schema = pyarrow.schema(fields, metadata=tbl.schema.metadata)
    return pyarrow.Table.from_arrays(arrays, schema=new_schema)
