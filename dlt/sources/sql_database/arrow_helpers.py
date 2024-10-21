from typing import Any, Sequence

from dlt.common.schema.typing import TTableSchemaColumns

from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import (
    row_tuples_to_arrow as _row_tuples_to_arrow,
)


@with_config
def row_tuples_to_arrow(
    rows: Sequence[Any],
    caps: DestinationCapabilitiesContext = None,
    columns: TTableSchemaColumns = None,
    tz: str = None,
) -> Any:
    """Converts `column_schema` to arrow schema using `caps` and `tz`. `caps` are injected from the container - which
    is always the case if run within the pipeline. This will generate arrow schema compatible with the destination.
    Otherwise generic capabilities are used
    """
    return _row_tuples_to_arrow(
        rows, caps or DestinationCapabilitiesContext.generic_capabilities(), columns, tz
    )
