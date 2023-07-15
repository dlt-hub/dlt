from dlt.common.exceptions import MissingDependencyException
from typing import Any, Tuple

from dlt.common.destination.capabilities import DestinationCapabilitiesContext

try:
    import pyarrow
    import pyarrow.parquet
except ImportError:
    raise MissingDependencyException("DLT parquet Helpers", ["parquet"], "DLT Helpers for for parquet.")


def get_py_arrow_datatype(column_type: str, caps: DestinationCapabilitiesContext) -> Any:
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
        # return pyarrow.struct([pyarrow.field('json', pyarrow.string())])
        return pyarrow.string()
    elif column_type == "decimal":
        return get_py_arrow_numeric(caps.decimal_precision)
    elif column_type == "wei":
        return get_py_arrow_numeric(caps.wei_precision)
    elif column_type == "date":
        return pyarrow.date32()
    else:
        raise ValueError(column_type)


def get_py_arrow_numeric(precision: Tuple[int, int]) -> Any:
    if precision[0] <= 38:
        return pyarrow.decimal128(*precision)
    if precision[0] <= 76:
        return pyarrow.decimal256(*precision)
    # for higher precision use max precision and trim scale to leave the most significant part
    return pyarrow.decimal256(76, max(0, 76 - (precision[0] - precision[1])))
