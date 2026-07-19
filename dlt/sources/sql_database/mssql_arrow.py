"""Native Arrow extraction backend for mssql-python via cursor.arrow_reader."""

from typing import Any, Dict, Iterator

from dlt.common import logger
from dlt.common.typing import TDataItem

from .helpers import TableLoader, SelectClause, register_table_loader_backend


class MssqlArrowTableLoader(TableLoader):
    """Table loader using ``cursor.arrow_reader`` from mssql-python for zero-copy Arrow batches.

    Falls back to the standard ``pyarrow`` path when ``arrow_reader`` is unavailable.
    """

    def _load_rows(
        self, query: SelectClause, backend_kwargs: Dict[str, Any]
    ) -> Iterator[TDataItem]:
        from dlt.common.libs.pyarrow import pyarrow as pa

        with self.engine.connect() as conn:
            # yield_per interferes with arrow_reader streaming
            result = conn.execute(query)
            try:
                cursor = getattr(result, "cursor", None)
                if cursor is not None and hasattr(cursor, "arrow_reader"):
                    logger.info("Using mssql-python arrow_reader for native Arrow batches")
                    reader = cursor.arrow_reader(batch_size=self.chunk_size)
                    for batch in reader:
                        yield pa.Table.from_batches([batch])
                else:
                    logger.warning(
                        "cursor.arrow_reader not available; falling back to pyarrow backend"
                    )
                    yield from self._convert_result(result, backend_kwargs)
            finally:
                result.close()


register_table_loader_backend("mssql_arrow", MssqlArrowTableLoader)
