from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pyarrow


def write_table_parquet(table: "pyarrow.Table", path: Union[str, Path]) -> None:
    from dlt.common.libs.pyarrow import pyarrow as _pa

    _pa.parquet.write_table(table, Path(path))
