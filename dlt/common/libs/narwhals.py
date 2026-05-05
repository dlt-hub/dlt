from __future__ import annotations

from typing import TYPE_CHECKING, Any

import narwhals
from narwhals.typing import IntoDataFrame

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow


def is_dataframe(obj: Any) -> bool:
    maybe_converted = narwhals.from_native(obj, allow_series=False, pass_through=True)
    if isinstance(maybe_converted, (narwhals.DataFrame, narwhals.LazyFrame)):
        return True
    return False


def df_to_arrow(df: IntoDataFrame) -> pyarrow.Table:
    """Converts any narwhals-compatible eager or lazy frame to a pyarrow table.
    lazy frames are eagerly collected.
    """
    nw_df = narwhals.from_native(df, allow_series=False, pass_through=True)
    if isinstance(nw_df, narwhals.LazyFrame):
        nw_df = nw_df.collect()

    return nw_df.to_arrow()
