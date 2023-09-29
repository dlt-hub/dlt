from typing import Any, Tuple, Optional
from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnType
from dlt.common.data_types import TDataType

try:
    import pyarrow
    import pyarrow.parquet
except ModuleNotFoundError:
    raise MissingDependencyException("DLT parquet Helpers", [f"{version.DLT_PKG_NAME}[parquet]"], "DLT Helpers for for parquet.")


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

# TODO precision and scale
def _get_column_type_from_py_arrow(dtype: pyarrow.DataType) -> TDataType:
    if pyarrow.types.is_string(dtype) or pyarrow.types.is_large_string(dtype):
        return "text"
    if pyarrow.types.is_floating(dtype):
        return "double"
    if pyarrow.types.is_boolean(dtype):
        return "bool"
    if pyarrow.types.is_timestamp(dtype):
        return "timestamp"
    if pyarrow.types.is_date(dtype):
        return "date"
    if pyarrow.types.is_time(dtype):
        return "time"
    if pyarrow.types.is_integer(dtype):
        return "bigint"
    if pyarrow.types.is_binary(dtype) or pyarrow.types.is_large_binary(dtype) or pyarrow.types.is_fixed_size_binary(dtype):
        return "binary"
    if pyarrow.types.is_decimal(dtype):
        return "decimal"
    if pyarrow.types.is_nested(dtype):
        return "complex"
    else:
        raise ValueError(dtype)

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
            "data_type": _get_column_type_from_py_arrow(field.type),
            "nullable": field.nullable,
        }
    return result
