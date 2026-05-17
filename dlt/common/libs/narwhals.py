from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals


if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from dlt.common.libs.pyarrow import pyarrow


def df_to_arrow(df: IntoFrame) -> pyarrow.Table:
    """Converts any narwhals-compatible eager or lazy frame to a pyarrow table.
    lazy frames are eagerly collected.
    """
    nw_df = narwhals.from_native(df, allow_series=False)
    if isinstance(nw_df, narwhals.LazyFrame):
        nw_df = nw_df.collect()

    return nw_df.to_arrow()
