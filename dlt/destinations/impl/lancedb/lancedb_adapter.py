from typing import Any

from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.destinations.utils import ensure_resource
from dlt.extract import DltResource


VECTORIZE_HINT = "x-lancedb-embed"
DOCUMENT_ID_HINT = "x-lancedb-doc-id"


def lancedb_adapter(
    data: Any,
    embed: TColumnNames = None,
    document_id: TColumnNames = None,
) -> DltResource:
    """Prepares data for the LanceDB destination by specifying which columns should be embedded.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        embed (TColumnNames, optional): Specify columns to generate embeddings for.
            It can be a single column name as a string, or a list of column names.
        document_id (TColumnNames, optional): Specify columns which represenet the document
            and which will be appended to primary/merge keys.

    Returns:
        DltResource: A resource with applied LanceDB-specific hints.

    Raises:
        ValueError: If input for `embed` invalid or empty.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Moonbase Engineer"}]
        >>> lancedb_adapter(data, embed="description")
        [DltResource with hints applied]
    """
    resource = ensure_resource(data)

    column_hints: TTableSchemaColumns = {}

    if embed:
        if isinstance(embed, str):
            embed = [embed]
        if not isinstance(embed, list):
            raise ValueError(
                "'embed' must be a list of column names or a single column name as a string."
            )

        for column_name in embed:
            column_hints[column_name] = {
                "name": column_name,
                VECTORIZE_HINT: True,  # type: ignore[misc]
            }

    if document_id:
        if isinstance(document_id, str):
            embed = [document_id]
        if not isinstance(document_id, list):
            raise ValueError(
                "'document_id' must be a list of column names or a single column name as a string."
            )

        for column_name in document_id:
            column_hints[column_name] = {
                "name": column_name,
                DOCUMENT_ID_HINT: True,  # type: ignore[misc]
            }

    if not column_hints:
        raise ValueError("At least one of 'embed' or 'document_id' must be specified.")
    else:
        resource.apply_hints(columns=column_hints)

    return resource
