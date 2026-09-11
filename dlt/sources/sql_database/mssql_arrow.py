"""Native Arrow extraction backend for mssql-python via cursor.arrow_reader."""

from typing import Any, Dict, Iterator

from dlt.common import logger
from dlt.common.exceptions import TerminalValueError
from dlt.common.typing import TDataItem

from .helpers import TableLoader, SelectClause, register_table_loader_backend


class MssqlArrowTableLoader(TableLoader):
    """Table loader using `cursor.arrow_reader` from mssql-python for zero-copy Arrow batches."""

    def _open_arrow_reader(self, result: Any) -> Any:
        """Opens the driver's Arrow reader, or explains why this engine cannot hand one out."""
        cursor = result.cursor
        if not hasattr(cursor, "arrow_reader"):
            raise TerminalValueError(
                'backend="mssql_arrow" reads Arrow batches straight off the mssql-python cursor,'
                f' but the engine is connected with "{self.engine.url.drivername}" and hands out a'
                f" {type(cursor).__name__} that has no `arrow_reader`. Connect with"
                ' "mssql+mssqlpython://", the dialect SQLAlchemy ships from 2.1.0b2 onwards, or'
                ' use backend="pyarrow" on this connection.'
            )
        return cursor.arrow_reader(batch_size=self.chunk_size)

    def _load_rows(
        self, query: SelectClause, backend_kwargs: Dict[str, Any]
    ) -> Iterator[TDataItem]:
        from dlt.common.libs.pyarrow import pyarrow as pa, cast_connectorx_temporal_columns

        with self.engine.connect() as conn:
            result = conn.execute(query)
            try:
                logger.info("Using mssql-python arrow_reader for native Arrow batches")
                reader = self._open_arrow_reader(result)
                try:
                    for batch in reader:
                        tbl = pa.Table.from_batches([batch])
                        yield cast_connectorx_temporal_columns(tbl)
                except BaseException:
                    # close before result.close(); a close failure must not mask a prior error
                    try:
                        reader.close()
                    except Exception:
                        logger.warning("failed to close mssql-python arrow reader", exc_info=True)
                    raise
                else:
                    reader.close()
            finally:
                result.close()


register_table_loader_backend("mssql_arrow", MssqlArrowTableLoader)
