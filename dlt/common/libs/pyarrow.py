from dlt.common.exceptions import MissingDependencyException
from typing import Any

try:
    import pyarrow
    import pyarrow.parquet
except ImportError:
    raise MissingDependencyException("DLT parquet Helpers", ["parquet"], "DLT Helpers for for parquet.")


def get_py_arrow_datatype(column_type: str) -> Any:
    if column_type == "text":
        return pyarrow.string()
    elif column_type == "double":
        return pyarrow.float64()
    elif column_type == "bool":
        return pyarrow.bool_()
    elif column_type == "timestamp":
        return pyarrow.timestamp('ms')
    elif column_type == "bigint":
        return pyarrow.int64()
    elif column_type == "binary":
        return pyarrow.binary()
    elif column_type == "complex":
        return pyarrow.string()
    elif column_type == "decimal":
        return pyarrow.decimal128(38, 18)
    elif column_type == "wei":
        return pyarrow.decimal128(38, 0)
    elif column_type == "date":
        return pyarrow.date32()
    else:
        raise ValueError(column_type)