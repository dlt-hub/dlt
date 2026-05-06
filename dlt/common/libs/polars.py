from typing import Any, Union

from dlt.common.exceptions import MissingDependencyException

try:
    import polars
    from polars import DataFrame, LazyFrame
except ModuleNotFoundError:
    raise MissingDependencyException("dlt Polars Helpers", ["polars"])


def polars_to_arrow(df: Union[polars.DataFrame, polars.LazyFrame]) -> Any:
    """Converts a Polars DataFrame or LazyFrame to a PyArrow Table.

    LazyFrames are auto-collected before conversion.
    """
    if isinstance(df, polars.LazyFrame):
        df = df.collect()
    return df.to_arrow()
