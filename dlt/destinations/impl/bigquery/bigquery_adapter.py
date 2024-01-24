from typing import Any

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract import DltResource, resource as make_resource


def bigquery_adapter(
    data: Any,
) -> DltResource:
    """Prepares data for the Bigquery destination.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.

    Returns:
        DltResource: A resource with applied Bigquery-specific hints.

    Raises:
        ValueError:

    Examples:
        >>> data = [{"name": "Alice", "description": "Software developer"}]
        >>> bigquery_adapter(data)
        [DltResource with hints applied]
    """
    # Wrap `data` in a resource if not an instance already.
    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name = None if hasattr(data, "__name__") else "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data

    column_hints: TTableSchemaColumns = {}
    raise NotImplementedError
