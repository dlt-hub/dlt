from typing import List, Dict, Union, Any, Literal, Set, get_args

from dlt.common.schema.typing import TColumnNames
from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.extract.decorators import resource as make_resource
from dlt.extract.source import DltResource

TTokenizationTMethod = Literal["word", "lowercase", "whitespace", "field"]
TOKENIZATION_METHODS: Set[TTokenizationTMethod] = set(get_args(TTokenizationTMethod))
TTokenizationSetting = Dict[str, TTokenizationTMethod]
"""Maps column names to tokenization types supported by Weaviate"""

VECTORIZE_HINT = "x-weaviate-vectorize"
TOKENIZATION_HINT = "x-weaviate-tokenization"


def weaviate_adapter(
    data: Any,
    vectorize: TColumnNames = None,
    tokenization: TTokenizationSetting = None,
) -> DltResource:

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
        # create weaviate-specific vectorize hints
        for column_name in vectorize:
            column_hints[column_name] = {
                "name": column_name,
                VECTORIZE_HINT: True,  # type: ignore
            }

    if tokenization:
        for column_name, method in tokenization.items():
            if method not in TOKENIZATION_METHODS:
                raise ValueError(f"Tokenization type {method} for column {column_name} is invalid. Allowed ")
            if column_name in column_hints:
                column_hints[column_name][TOKENIZATION_HINT] = method  # type: ignore
            else:
                column_hints[column_name] = {
                    "name": column_name,
                    TOKENIZATION_HINT: method,  # type: ignore
                }

    if not column_hints:
        raise ValueError("Either 'vectorize' or 'tokenization' must be specified.")
    else:
        resource.apply_hints(columns=column_hints)

    return resource
