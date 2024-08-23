from dlt.common.schema import Schema
from dlt.common.schema.utils import find_incomplete_columns
from dlt.common.schema.exceptions import UnboundColumnException
from dlt.common import logger


def verify_normalized_schema(schema: Schema) -> None:
    """Verify the schema is valid for next stage after normalization.

    1. Log warning if any incomplete nullable columns are in any data tables
    2. Raise `UnboundColumnException` on incomplete non-nullable columns (e.g. missing merge/primary key)
    """
    for table_name, column, nullable in find_incomplete_columns(
        schema.data_tables(seen_data_only=True)
    ):
        exc = UnboundColumnException(schema.name, table_name, column)
        if nullable:
            logger.warning(str(exc))
        else:
            raise exc
