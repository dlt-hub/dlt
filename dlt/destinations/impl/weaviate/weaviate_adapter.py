from typing import Dict, Any, List, Literal, Optional, Set
from typing_extensions import TypedDict

from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames, get_args
from dlt.extract import DltResource, resource as make_resource
from dlt.extract.items import TTableHintTemplate
from dlt.destinations.utils import get_resource_for_adapter

TTokenizationTMethod = Literal["word", "lowercase", "whitespace", "field"]
TOKENIZATION_METHODS: Set[TTokenizationTMethod] = set(get_args(TTokenizationTMethod))
TTokenizationSetting = Dict[str, TTokenizationTMethod]
"""Maps column names to tokenization types supported by Weaviate"""

VECTORIZE_HINT = "x-weaviate-vectorize"
TOKENIZATION_HINT = "x-weaviate-tokenization"
VECTOR_HINT = "x-weaviate-vector"
NAMED_VECTORS_HINT = "x-weaviate-named-vectors"


class TWeaviateNamedVector(TypedDict, total=False):
    """One named vector of a Weaviate collection."""

    vectorize: List[str]
    """Columns the vector is built from."""
    vectorizer: Optional[str]
    """Vectorizer module. Defaults to the destination `vectorizer`."""


TWeaviateNamedVectors = Dict[str, TWeaviateNamedVector]


def weaviate_adapter(
    data: Any,
    vectorize: TColumnNames = None,
    tokenization: TTokenizationSetting = None,
    vector: str = None,
    named_vectors: TWeaviateNamedVectors = None,
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
        vector (str, optional): Name of a column holding a precomputed embedding. The column
            is stored as the object vector instead of as a property, and the collection is
            created without a vectorizer.
        named_vectors (TWeaviateNamedVectors, optional): Maps a vector name to the columns it
            is built from and, optionally, its own vectorizer module. Lets one collection carry
            several independently configured vectors.

    Returns:
        DltResource: A resource with applied Weaviate-specific hints.

    Raises:
        ValueError: If input for `vectorize`, `tokenization` or `vector` is invalid
            or none is specified.

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

    if vector:
        if not isinstance(vector, str):
            raise ValueError("`vector` must be a single column name as a string")
        if vectorize and vector in vectorize:
            raise ValueError(
                f"Column `{vector}` cannot be both vectorized by Weaviate and supplied as a"
                " precomputed vector"
            )
        # typed as json so the normalizer keeps the embedding as one value
        # instead of unnesting the list into a child table
        column_hints[vector] = {
            "name": vector,
            "data_type": "json",
            VECTOR_HINT: True,  # type: ignore
        }

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    if named_vectors:
        for vector_name, spec in named_vectors.items():
            sources = spec.get("vectorize")
            if not sources:
                raise ValueError(f"Named vector `{vector_name}` must list the columns to vectorize")
            if isinstance(sources, str):
                named_vectors[vector_name]["vectorize"] = [sources]
        additional_table_hints[NAMED_VECTORS_HINT] = named_vectors

    # this makes sure that {} as column_hints never gets into apply_hints (that would reset existing columns)
    if not column_hints and not additional_table_hints:
        raise ValueError(
            "One of 'vectorize', 'tokenization', 'vector' or 'named_vectors' must be specified."
        )
    resource.apply_hints(
        columns=column_hints or None, additional_table_hints=additional_table_hints or None
    )

    return resource
