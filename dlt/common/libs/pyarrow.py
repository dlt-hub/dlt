from typing import Any, Tuple, Optional, Union, Callable, Iterable, Iterator, Sequence, Tuple
from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnType, TColumnSchemaBase
from dlt.common.data_types import TDataType
from dlt.common.typing import TFileOrPath

try:
    import pyarrow
    import pyarrow.parquet
except ModuleNotFoundError:
    raise MissingDependencyException("DLT parquet Helpers", [f"{version.DLT_PKG_NAME}[parquet]"], "DLT Helpers for for parquet.")


TAnyArrowItem = Union[pyarrow.Table, pyarrow.RecordBatch]


def get_py_arrow_datatype(column: TColumnType, caps: DestinationCapabilitiesContext, tz: str) -> Any:
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
        precision_tuple = (precision, scale) if precision is not None and scale is not None else caps.decimal_precision
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


def _get_column_type_from_py_arrow(dtype: pyarrow.DataType) -> TColumnType:
    """Returns (data_type, precision, scale) tuple from pyarrow.DataType
    """
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
        if dtype.bit_width != 64: # 64bit is a default bigint
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
    """Remove all columns of datatype pyarrow.null() from the table or record batch
    """
    if isinstance(item, pyarrow.Table):
        return item.drop([field.name for field in item.schema if pyarrow.types.is_null(field.type)])
    elif isinstance(item, pyarrow.RecordBatch):
        null_idx = [i for i, col in enumerate(item.columns) if pyarrow.types.is_null(col.type)]
        new_schema = item.schema
        for i in reversed(null_idx):
            new_schema = new_schema.remove(i)
        return pyarrow.RecordBatch.from_arrays(
            [col for i, col in enumerate(item.columns) if i not in null_idx],
            schema=new_schema
        )
    else:
        raise ValueError(item)



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
            **_get_column_type_from_py_arrow(field.type),
        }
    return result


def get_row_count(parquet_file: TFileOrPath) -> int:
    """Get the number of rows in a parquet file.

    Args:
        parquet_file (str): path to parquet file

    Returns:
        int: number of rows
    """
    with pyarrow.parquet.ParquetFile(parquet_file) as reader:
        return reader.metadata.num_rows  # type: ignore[no-any-return]


def is_arrow_item(item: Any) -> bool:
    return isinstance(item, (pyarrow.Table, pyarrow.RecordBatch))


TNewColumns = Sequence[Tuple[pyarrow.Field, Callable[[pyarrow.Table], Iterable[Any]]]]


def pq_stream_with_new_columns(
    parquet_file: TFileOrPath, columns: TNewColumns, row_groups_per_read: int = 1
) -> Iterator[pyarrow.Table]:
    """Add column(s) to the table in batches.

    The table is read from parquet `row_groups_per_read` row groups at a time

    Args:
        parquet_file: path or file object to parquet file
        columns: list of columns to add in the form of (`pyarrow.Field`, column_value_callback)
            The callback should accept a `pyarrow.Table` and return an array of values for the column.
        row_groups_per_read: number of row groups to read at a time. Defaults to 1.

    Yields:
        `pyarrow.Table` objects with the new columns added.
    """
    with pyarrow.parquet.ParquetFile(parquet_file) as reader:
        n_groups = reader.num_row_groups
        # Iterate through n row groups at a time
        for i in range(0, n_groups, row_groups_per_read):
            tbl: pyarrow.Table = reader.read_row_groups(range(i, min(i + row_groups_per_read, n_groups)))
            for col in columns:
                tbl = tbl.append_column(col[0], col[1](tbl))
            yield tbl
