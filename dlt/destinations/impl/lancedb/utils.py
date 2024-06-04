from typing import List, Type, Optional

from lancedb.embeddings import TextEmbeddingFunction
from lancedb.pydantic import LanceModel, Vector
from pydantic import create_model

from dlt.common.typing import DictStrAny


def infer_lancedb_model_from_data(
    data: List[DictStrAny],
    id_field_name: str,
    vector_field_name: str,
    embedding_fields: List[str],
    embedding_model_func: TextEmbeddingFunction,
    embedding_model_dimensions: Optional[int] = None,
) -> Type[LanceModel]:
    """Infers a LanceModel from passed data records.

    Args:
        data (List[DictStrAny]): Data records to infer schema from.
        id_field_name (str): The name of the ID field.
        vector_field_name (str): The name of the vector field.
        embedding_fields (List[str]): The names of the embedding fields.
        embedding_model_func (TextEmbeddingFunction): The function used to create the embedding model.
        embedding_model_dimensions (int): The dimensions of the embedding model.

    Returns:
        Type[LanceModel]: The inferred LanceModel.
    """

    template_schema: Type[LanceModel] = create_model(
        "TemplateSchema",
        __base__=LanceModel,
        __module__=__name__,
        __validators__={},
        **{
            id_field_name: (str, ...),
            vector_field_name: (
                Vector(
                    embedding_model_dimensions
                    if embedding_model_dimensions
                    else embedding_model_func.ndims()
                ),
                ...,
            ),
        },
    )

    field_types = {}
    for field_name in data[0].keys():
        if field_name != id_field_name and field_name != vector_field_name:
            field_types[field_name] = (
                str,  # Infer all fields as str
                embedding_model_func.SourceField()
                if field_name in embedding_fields
                else None,  # Set default to None to make fields optional
            )

    inferred_schema: Type[LanceModel] = create_model(
        "InferredSchema",
        __base__=template_schema,
        __module__=__name__,
        **field_types,
    )

    # pprint(f"template_schema fields:\n{template_schema.model_fields}")
    # pprint(f"inferred_schema fields:\n{inferred_schema.model_fields}")

    return inferred_schema
