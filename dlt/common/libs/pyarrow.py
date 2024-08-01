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
    Tuple,
)

from dlt import version
from dlt.common.pendulum import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import DLT_NAME_PREFIX, TTableSchemaColumns

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnType
from dlt.common.typing import StrStr, TFileOrPath
from dlt.common.normalizers.naming import NamingConvention

try:
    import pyarrow
    import pyarrow.parquet
    import pyarrow.compute
    import pyarrow.dataset
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt pyarrow helpers",
        [f"{version.DLT_PKG_NAME}[parquet]"],
        "Install pyarrow to be allow to load arrow tables, panda frames and to use parquet files.",
    )

TAnyArrowItem = Union[pyarrow.Table, pyarrow.RecordBatch]

ARROW_DECIMAL_MAX_PRECISION = 76


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
        return get_py_arrow_timestamp(column.get("precision") or caps.timestamp_precision, tz)
    elif column_type == "bigint":
        return get_pyarrow_int(column.get("precision"))
    elif column_type == "binary":
        return pyarrow.binary(column.get("precision") or -1)
    elif column_type == "complex":
        # return pyarrow.struct([pyarrow.field('json', pyarrow.string())])
        return pyarrow.string()
    elif column_type == "decimal":
        precision, scale = column.get("precision"), column.get("scale")
        precision_tuple = (
            (precision, scale)
            if precision is not None and scale is not None
            else caps.decimal_precision
        )
        return get_py_arrow_numeric(precision_tuple)
    elif column_type == "wei":
        return get_py_arrow_numeric(caps.wei_precision)
    elif column_type == "date":
        return pyarrow.date32()
    elif column_type == "time":
        return get_py_arrow_time(column.get("precision") or caps.timestamp_precision)
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


def get_column_type_from_py_arrow(dtype: pyarrow.DataType) -> TColumnType:
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
        return dict(data_type="complex")
    else:
        raise ValueError(dtype)


def remove_null_columns(item: TAnyArrowItem) -> TAnyArrowItem:
    """Remove all columns of datatype pyarrow.null() from the table or record batch"""
    return remove_columns(
        item, [field.name for field in item.schema if pyarrow.types.is_null(field.type)]
    )


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
        raise TypeError(f"Unsupported data item type {type(item)}")


def should_normalize_arrow_schema(
    schema: pyarrow.Schema,
    columns: TTableSchemaColumns,
    naming: NamingConvention,
    add_load_id: bool = False,
) -> Tuple[bool, Mapping[str, str], Dict[str, str], Dict[str, bool], bool, TTableSchemaColumns]:
    rename_mapping = get_normalized_arrow_fields_mapping(schema, naming)
    rev_mapping = {v: k for k, v in rename_mapping.items()}
    nullable_mapping = {k: v.get("nullable", True) for k, v in columns.items()}
    # All fields from arrow schema that have nullable set to different value than in columns
    # Key is the renamed column name
    nullable_updates: Dict[str, bool] = {}
    for field in schema:
        norm_name = rename_mapping[field.name]
        if norm_name in nullable_mapping and field.nullable != nullable_mapping[norm_name]:
            nullable_updates[norm_name] = nullable_mapping[norm_name]

    dlt_load_id_col = naming.normalize_table_identifier("_dlt_load_id")
    dlt_id_col = naming.normalize_table_identifier("_dlt_id")
    dlt_columns = {dlt_load_id_col, dlt_id_col}

    # Do we need to add a load id column?
    if add_load_id and dlt_load_id_col in columns:
        try:
            schema.field(dlt_load_id_col)
            needs_load_id = False
        except KeyError:
            needs_load_id = True
    else:
        needs_load_id = False

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
        and not needs_load_id
    )
    return (
        not skip_normalize,
        rename_mapping,
        rev_mapping,
        nullable_updates,
        needs_load_id,
        columns,
    )


def normalize_py_arrow_item(
    item: TAnyArrowItem,
    columns: TTableSchemaColumns,
    naming: NamingConvention,
    caps: DestinationCapabilitiesContext,
    load_id: Optional[str] = None,
) -> TAnyArrowItem:
    """Normalize arrow `item` schema according to the `columns`.

    1. arrow schema field names will be normalized according to `naming`
    2. arrows columns will be reordered according to `columns`
    3. empty columns will be inserted if they are missing, types will be generated using `caps`
    4. arrow columns with different nullability than corresponding schema columns will be updated
    5. Add `_dlt_load_id` column if it is missing and `load_id` is provided
    """
    schema = item.schema
    should_normalize, rename_mapping, rev_mapping, nullable_updates, needs_load_id, columns = (
        should_normalize_arrow_schema(schema, columns, naming, load_id is not None)
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
                nullable=column.get("nullable", True),
            )
            new_fields.append(new_field)
            new_columns.append(pyarrow.nulls(item.num_rows, type=new_field.type))

    # add the remaining columns
    for column_name, field_name in rev_mapping.items():
        idx = schema.get_field_index(field_name)
        # use renamed field
        new_fields.append(schema.field(idx).with_name(column_name))
        new_columns.append(item.column(idx))

    if needs_load_id and load_id:
        # Storage efficient type for a column with constant value
        load_id_type = pyarrow.dictionary(pyarrow.int8(), pyarrow.string())
        new_fields.append(
            pyarrow.field(
                naming.normalize_table_identifier("_dlt_load_id"),
                load_id_type,
                nullable=False,
            )
        )
        new_columns.append(pyarrow.array([load_id] * item.num_rows, type=load_id_type))

    # create desired type
    return item.__class__.from_arrays(new_columns, schema=pyarrow.schema(new_fields))


def get_normalized_arrow_fields_mapping(schema: pyarrow.Schema, naming: NamingConvention) -> StrStr:
    """Normalizes schema field names and returns mapping from original to normalized name. Raises on name collisions"""
    norm_f = naming.normalize_identifier
    name_mapping = {n.name: norm_f(n.name) for n in schema}
    # verify if names uniquely normalize
    normalized_names = set(name_mapping.values())
    if len(name_mapping) != len(normalized_names):
        raise NameNormalizationCollision(
            f"Arrow schema fields normalized from {list(name_mapping.keys())} to"
            f" {list(normalized_names)}"
        )
    return name_mapping


def py_arrow_to_table_schema_columns(schema: pyarrow.Schema) -> TTableSchemaColumns:
    """Convert a PyArrow schema to a table schema columns dict.

    Args:
        schema (pyarrow.Schema): pyarrow schema

    Returns:
        TTableSchemaColumns: table schema columns
    """
    result: TTableSchemaColumns = {}
    for field in schema:
        result[field.name] = {
            "name": field.name,
            "nullable": field.nullable,
            **get_column_type_from_py_arrow(field.type),
        }
    return result


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
    """Converts arrow scalar into Python type. Currently adds "UTC" to naive date times and converts all others to UTC"""
    row_value = arrow_value.as_py()
    # dates are not represented as datetimes but I see connector-x represents
    # datetimes as dates and keeping the exact time inside. probably a bug
    # but can be corrected this way
    if isinstance(row_value, date) and not isinstance(row_value, datetime):
        row_value = pendulum.from_timestamp(arrow_value.cast(pyarrow.int64()).as_py() / 1000)
    elif isinstance(row_value, datetime):
        row_value = pendulum.instance(row_value).in_tz("UTC")
    return row_value


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
    """
    field = pyarrow.field(name, pyarrow.dictionary(pyarrow.int8(), data_type), nullable=nullable)
    if index == -1:
        return item.append_column(field, pyarrow.array([value] * item.num_rows, type=field.type))
    return item.add_column(index, field, pyarrow.array([value] * item.num_rows, type=field.type))


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
                adjusted_field = schema.field(i).with_type(cast_type)
                schema = schema.set(i, adjusted_field)
                break  # if type matches type check, do not do other type checks
    return schema


class NameNormalizationCollision(ValueError):
    def __init__(self, reason: str) -> None:
        msg = f"Arrow column name collision after input data normalization. {reason}"
        super().__init__(msg)
