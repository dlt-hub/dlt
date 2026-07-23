"""Native Arrow extraction backend for mssql-python via cursor.arrow_reader."""

from typing import Any, Dict, Iterator

from dlt.common import logger
from dlt.common.typing import TDataItem

from .helpers import TableLoader, SelectClause, register_table_loader_backend


class MssqlArrowTableLoader(TableLoader):
    """Table loader using ``cursor.arrow_reader`` from mssql-python for zero-copy Arrow batches."""

    def _load_rows(
        self, query: SelectClause, backend_kwargs: Dict[str, Any]
    ) -> Iterator[TDataItem]:
        from dlt.common.libs.pyarrow import pyarrow as pa

        with self.engine.connect() as conn:
            # yield_per interferes with arrow_reader streaming
            result = conn.execute(query)
            try:
                logger.info("Using mssql-python arrow_reader for native Arrow batches")
                reader = result.cursor.arrow_reader(batch_size=self.chunk_size)
                for batch in reader:
                    yield pa.Table.from_batches([batch])
            finally:
                result.close()


register_table_loader_backend("mssql_arrow", MssqlArrowTableLoader)
