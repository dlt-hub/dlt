from typing import Dict, Any, Literal, Set

from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames, get_args
from dlt.extract import DltResource, resource as make_resource
from dlt.destinations.utils import get_resource_for_adapter

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
    """Prepares data for the Weaviate destination by specifying which columns
    should be vectorized and which tokenization method to use.

    Vectorization is done by Weaviate's vectorizer modules. The vectorizer module
    can be configured in dlt configuration file under
    `[destination.weaviate.vectorizer]` and `[destination.weaviate.module_config]`.
    The default vectorizer module is `text2vec-openai`. See also:
    https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        vectorize (TColumnNames, optional): Specifies columns that should be
            vectorized. Can be a single column name as a string or a list of
            column names.
        tokenization (TTokenizationSetting, optional): A dictionary mapping column
            names to tokenization methods supported by Weaviate. The tokenization
            methods are one of the values in `TOKENIZATION_METHODS`:
            - 'word',
            - 'lowercase',
            - 'whitespace',
            - 'field'.

    Returns:
        DltResource: A resource with applied Weaviate-specific hints.

    Raises:
        ValueError: If input for `vectorize` or `tokenization` is invalid
            or neither is specified.

    Examples:
        >>> data = [{"name": "Alice", "description": "Software developer"}]
        >>> weaviate_adapter(data, vectorize="description", tokenization={"description": "word"})
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)

    column_hints: TTableSchemaColumns = {}
    if vectorize:
        if isinstance(vectorize, str):
            vectorize = [vectorize]
        if not isinstance(vectorize, list):
            raise ValueError(
                "`vectorize` must be a list of column names or a single column name as a string"
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
                raise ValueErrorWithKnownValues("method", method, TOKENIZATION_METHODS)

            if column_name in column_hints:
                column_hints[column_name][TOKENIZATION_HINT] = method  # type: ignore
            else:
                column_hints[column_name] = {
                    "name": column_name,
                    TOKENIZATION_HINT: method,  # type: ignore
                }

    # this makes sure that {} as column_hints never gets into apply_hints (that would reset existing columns)
    if not column_hints:
        raise ValueError("Either 'vectorize' or 'tokenization' must be specified.")
    else:
        resource.apply_hints(columns=column_hints)

    return resource
