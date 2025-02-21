from typing import Any, Sequence

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration import with_config
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext


def row_tuples_to_arrow(
    rows: Sequence[Any],
    columns: TTableSchemaColumns = None,
    tz: str = None,
) -> Any:
    """Converts `column_schema` to arrow schema using `caps` and `tz`. `caps` are injected from the container - which
    is always the case if run within the pipeline. This will generate arrow schema compatible with the destination.
    Otherwise generic capabilities are used
    """
    from dlt.common.libs.pyarrow import row_tuples_to_arrow as _row_tuples_to_arrow

    return _row_tuples_to_arrow(
        rows=rows,
        caps=Container().get(DestinationCapabilitiesContext)
        or DestinationCapabilitiesContext.generic_capabilities(),
        columns=columns,
        tz=tz,
        safe_arrow_conversion=True,
    )
