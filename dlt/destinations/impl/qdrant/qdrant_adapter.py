from typing import Any

from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.extract import DltResource, resource as make_resource
from dlt.destinations.utils import ensure_resource

VECTORIZE_HINT = "x-qdrant-embed"


def qdrant_adapter(
    data: Any,
    embed: TColumnNames = None,
) -> DltResource:
    """Prepares data for the Qdrant destination by specifying which columns
    should be embedded.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        embed (TColumnNames, optional): Specifies columns to generate embeddings for.
            Can be a single column name as a string or a list of column names.

    Returns:
        DltResource: A resource with applied qdrant-specific hints.

    Raises:
        ValueError: If input for `embed` invalid or empty.

    Examples:
        >>> data = [{"name": "Anush", "description": "Integrations Hacker"}]
        >>> qdrant_adapter(data, embed="description")
        [DltResource with hints applied]
    """
    resource = ensure_resource(data)

    column_hints: TTableSchemaColumns = {}

    if embed:
        if isinstance(embed, str):
            embed = [embed]
        if not isinstance(embed, list):
            raise ValueError(
                "embed must be a list of column names or a single column name as a string"
            )

        for column_name in embed:
            column_hints[column_name] = {
                "name": column_name,
                VECTORIZE_HINT: True,  # type: ignore
            }

    if not column_hints:
        raise ValueError("A value for 'embed' must be specified.")
    else:
        resource.apply_hints(columns=column_hints)

    return resource
