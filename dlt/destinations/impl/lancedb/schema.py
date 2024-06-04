"""Utilities for creating Pydantic model schemas from table schemas."""

from typing import (
    List,
    Any,
    cast,
    Type,
    Optional,
    Dict,
)

from lancedb.embeddings import TextEmbeddingFunction  # type: ignore
from lancedb.pydantic import LanceModel, Vector  # type: ignore
from pydantic import create_model

from dlt.common.schema import Schema, TColumnSchema
from dlt.common.typing import DictStrAny
from dlt.destinations.type_mapping import TypeMapper


TLanceModel = Type[LanceModel]


def make_field_schema(
    column_name: str,
    column: TColumnSchema,
    type_mapper: TypeMapper,
    embedding_model_func: TextEmbeddingFunction,
    embedding_fields: List[str],
) -> DictStrAny:
    return {
        column_name: (
            type_mapper.to_db_type(column),
            (embedding_model_func.SourceField() if column_name in embedding_fields else ...),
        )
    }


def make_fields(
    table_name: str,
    schema: Schema,
    type_mapper: TypeMapper,
    embedding_model_func: TextEmbeddingFunction,
    embedding_fields: List[str],
) -> List[Dict[str, Any]]:
    """Creates a Pydantic properties schema from a table schema.

    Args:
        embedding_fields (List[str]):
        embedding_model_func (TextEmbeddingFunction):
        type_mapper (TypeMapper):
        schema (Schema): Schema to use.
        table_name: The table name for which columns should be converted to a pydantic model.
    """

    return [
        make_field_schema(
            column_name,
            column,
            type_mapper=type_mapper,
            embedding_model_func=embedding_model_func,
            embedding_fields=embedding_fields,
        )
        for column_name, column in schema.get_table_columns(table_name).items()
    ]


def create_template_schema(
    id_field_name: str,
    vector_field_name: str,
    embedding_fields: List[str],
    embedding_model_func: TextEmbeddingFunction,
    embedding_model_dimensions: Optional[int],
) -> Type[LanceModel]:
    # Only create vector Field if there is one or more embedding fields defined.
    special_fields = {
        id_field_name: (str, ...),
    }
    if embedding_fields:
        special_fields[vector_field_name] = (
            Vector(embedding_model_dimensions or embedding_model_func.ndims()),
            ...,
        )
    return cast(
        TLanceModel,
        create_model(  # type: ignore[call-overload]
            "TemplateSchema",
            __base__=LanceModel,
            __module__=__name__,
            __validators__={},
            **special_fields,
        ),
    )
