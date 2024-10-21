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
from dlt.common.schema.typing import C_DLT_ID, C_DLT_LOAD_ID, TTableSchemaColumns
from dlt.common import logger, json
from dlt.common.json import custom_encode, map_nested_in_place

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import is_nullable_column
from dlt.common.typing import StrStr, TFileOrPath
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
        return dict(data_type="json")
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
    """Figure out if any of the normalization steps must be executed. This prevents
    from rewriting arrow tables when no changes are needed. Refer to `normalize_py_arrow_item`
    for a list of normalizations. Note that `column` must be already normalized.
    """
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
    """Normalize arrow `item` schema according to the `columns`. Note that
    columns must be already normalized.

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

    if needs_load_id and load_id:
        # Storage efficient type for a column with constant value
        load_id_type = pyarrow.dictionary(pyarrow.int8(), pyarrow.string())
        new_fields.append(
            pyarrow.field(
                naming.normalize_identifier(C_DLT_LOAD_ID),
                load_id_type,
                nullable=False,
            )
        )
        new_columns.append(pyarrow.array([load_id] * item.num_rows, type=load_id_type))

    # create desired type
    return item.__class__.from_arrays(new_columns, schema=pyarrow.schema(new_fields))


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
    return pyarrow.schema(
        [
            pyarrow.field(
                name,
                get_py_arrow_datatype(
                    schema_item,
                    caps or DestinationCapabilitiesContext.generic_capabilities(),
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
            raise ValueError(f"Unsupported type {type(item)}")
    if batches:
        tables.append(pyarrow.Table.from_batches(batches))
    # "none" option ensures 0 copy concat
    return pyarrow.concat_tables(tables, promote_options="none")


def row_tuples_to_arrow(
    rows: Sequence[Any], caps: DestinationCapabilitiesContext, columns: TTableSchemaColumns, tz: str
) -> Any:
    """Converts the rows to an arrow table using the columns schema.
    Columns missing `data_type` will be inferred from the row data.
    Columns with object types not supported by arrow are excluded from the resulting table.
    """
    from dlt.common.libs.pyarrow import pyarrow as pa
    import numpy as np

    try:
        from pandas._libs import lib

        pivoted_rows = lib.to_object_array_tuples(rows).T
    except ImportError:
        logger.info(
            "Pandas not installed, reverting to numpy.asarray to create a table which is slower"
        )
        pivoted_rows = np.asarray(rows, dtype="object", order="k").T  # type: ignore[call-overload]

    columnar = {
        col: dat.ravel() for col, dat in zip(columns, np.vsplit(pivoted_rows, len(columns)))
    }
    columnar_known_types = {
        col["name"]: columnar[col["name"]]
        for col in columns.values()
        if col.get("data_type") is not None
    }
    columnar_unknown_types = {
        col["name"]: columnar[col["name"]]
        for col in columns.values()
        if col.get("data_type") is None
    }

    arrow_schema = columns_to_arrow(columns, caps, tz)

    for idx in range(0, len(arrow_schema.names)):
        field = arrow_schema.field(idx)
        py_type = type(rows[0][idx])
        # cast double / float ndarrays to decimals if type mismatch, looks like decimals and floats are often mixed up in dialects
        if pa.types.is_decimal(field.type) and issubclass(py_type, (str, float)):
            logger.warning(
                f"Field {field.name} was reflected as decimal type, but rows contains"
                f" {py_type.__name__}. Additional cast is required which may slow down arrow table"
                " generation."
            )
            float_array = pa.array(columnar_known_types[field.name], type=pa.float64())
            columnar_known_types[field.name] = float_array.cast(field.type, safe=False)
        if issubclass(py_type, (dict, list)):
            logger.warning(
                f"Field {field.name} was reflected as JSON type and needs to be serialized back to"
                " string to be placed in arrow table. This will slow data extraction down. You"
                " should cast JSON field to STRING in your database system ie. by creating and"
                " extracting an SQL VIEW that selects with cast."
            )
            json_str_array = pa.array(
                [None if s is None else json.dumps(s) for s in columnar_known_types[field.name]]
            )
            columnar_known_types[field.name] = json_str_array

    # If there are unknown type columns, first create a table to infer their types
    if columnar_unknown_types:
        new_schema_fields = []
        for key in list(columnar_unknown_types):
            arrow_col: Optional[pa.Array] = None
            try:
                arrow_col = pa.array(columnar_unknown_types[key])
                if pa.types.is_null(arrow_col.type):
                    logger.warning(
                        f"Column {key} contains only NULL values and data type could not be"
                        " inferred. This column is removed from a arrow table"
                    )
                    continue

            except pa.ArrowInvalid as e:
                # Try coercing types not supported by arrow to a json friendly format
                # E.g. dataclasses -> dict, UUID -> str
                try:
                    arrow_col = pa.array(
                        map_nested_in_place(custom_encode, list(columnar_unknown_types[key]))
                    )
                    logger.warning(
                        f"Column {key} contains a data type which is not supported by pyarrow and"
                        f" got converted into {arrow_col.type}. This slows down arrow table"
                        " generation."
                    )
                except (pa.ArrowInvalid, TypeError):
                    logger.warning(
                        f"Column {key} contains a data type which is not supported by pyarrow. This"
                        f" column will be ignored. Error: {e}"
                    )
            if arrow_col is not None:
                columnar_known_types[key] = arrow_col
                new_schema_fields.append(
                    pa.field(
                        key,
                        arrow_col.type,
                        nullable=columns[key]["nullable"],
                    )
                )

        # New schema
        column_order = {name: idx for idx, name in enumerate(columns)}
        arrow_schema = pa.schema(
            sorted(
                list(arrow_schema) + new_schema_fields,
                key=lambda x: column_order[x.name],
            )
        )

    return pa.Table.from_pydict(columnar_known_types, schema=arrow_schema)


class NameNormalizationCollision(ValueError):
    def __init__(self, reason: str) -> None:
        msg = f"Arrow column name collision after input data normalization. {reason}"
        super().__init__(msg)
