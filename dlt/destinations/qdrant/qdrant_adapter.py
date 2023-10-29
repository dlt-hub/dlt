from typing import Any

from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.extract.decorators import resource as make_resource
from dlt.extract.source import DltResource

VECTORIZE_HINT = "x-qdrant-vectorize"


def qdrant_adapter(
    data: Any,
    vectorize: TColumnNames = None,
) -> DltResource:
    """Prepares data for the Qdrant destination by specifying which columns
    should be embedded.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        embedding_field (TColumnNames, optional): Specifies columns that should be
            vectorized. Can be a single column name as a string or a list of
            column names.

    Returns:
        DltResource: A resource with applied qdrant-specific hints.

    Raises:
        ValueError: If input for `vectorize` or `tokenization` is invalid
            or neither is specified.

    Examples:
        >>> data = [{"name": "Alice", "description": "Software developer"}]
        >>> qdrant_adapter(data, embedding_field="description")
        [DltResource with hints applied]
    """
    # wrap `data` in a resource if not an instance already
    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name: str = None
        if not hasattr(data, "__name__"):
            resource_name = "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data

    column_hints: TTableSchemaColumns = {}

    if vectorize:
        if isinstance(vectorize, str):
            vectorize = [vectorize]
        if not isinstance(vectorize, list):
            raise ValueError(
                "vectorize must be a list of column names or a single "
                "column name as a string"
            )

        for column_name in vectorize:
            column_hints[column_name] = {
                "name": column_name,
                VECTORIZE_HINT: True,  # type: ignore
            }

    if not column_hints:
        raise ValueError(
            "'embedding_field' must be specified.")
    else:
        resource.apply_hints(columns=column_hints)

    return resource
