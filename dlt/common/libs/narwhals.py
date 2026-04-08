from __future__ import annotations

from typing import TYPE_CHECKING

import polars
import narwhals
from narwhals.typing import IntoDataFrame

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow


def df_to_arrow(df: IntoDataFrame) -> pyarrow.Table:
    """Converts any narwhals-compatible eager or lazy frame to a pyarrow table.

    lazy frames are eagerly collected.
    """
    nw_df = narwhals.from_native(df)
    if isinstance(nw_df, narwhals.LazyFrame):
        nw_df = nw_df.collect()

    return nw_df.to_arrow()
