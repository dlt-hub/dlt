from typing import Any
from dlt.common.exceptions import MissingDependencyException

try:
    import pandas
except ModuleNotFoundError:
    raise MissingDependencyException("dlt Pandas Helpers", ["pandas"])


def pandas_to_arrow(df: pandas.DataFrame) -> Any:
    """Converts pandas to arrow or raises an exception if pyarrow is not installed"""
    from dlt.common.libs.pyarrow import pyarrow as pa

    return pa.Table.from_pandas(df)
