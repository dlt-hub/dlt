"""Utilities for creating Pydantic model schemas from table schemas."""

from typing import (
    List,
    cast,
    Optional,
)

import pyarrow as pa
from lancedb.embeddings import TextEmbeddingFunction  # type: ignore
from typing_extensions import TypeAlias

from dlt.common.schema import Schema, TColumnSchema
from dlt.common.typing import DictStrAny
from dlt.destinations.type_mapping import TypeMapper


TArrowSchema: TypeAlias = pa.Schema
TArrowDataType: TypeAlias = pa.DataType
TArrowField: TypeAlias = pa.Field
NULL_SCHEMA: TArrowSchema = pa.schema([])
"""Empty pyarrow Schema with no fields."""


def arrow_schema_to_dict(schema: TArrowSchema) -> DictStrAny:
    return {field.name: field.type for field in schema}


def make_arrow_field_schema(
    column_name: str,
    column: TColumnSchema,
    type_mapper: TypeMapper,
    embedding_model_func: Optional[TextEmbeddingFunction] = None,
    embedding_fields: Optional[List[str]] = None,
) -> TArrowDataType:
    raise NotImplementedError


def make_arrow_table_schema(
    table_name: str,
    schema: Schema,
    type_mapper: TypeMapper,
    id_field_name: Optional[str] = None,
    vector_field_name: Optional[str] = None,
    embedding_fields: Optional[List[str]] = None,
    embedding_model_func: Optional[TextEmbeddingFunction] = None,
    embedding_model_dimensions: Optional[int] = None,
) -> TArrowSchema:
    """Creates a PyArrow schema from a dlt schema."""
    arrow_schema: List[TArrowField] = []

    if id_field_name:
        arrow_schema.append(pa.field(id_field_name, pa.string()))

    if embedding_fields:
        vec_size = embedding_model_dimensions or embedding_model_func.ndims()
        arrow_schema.append(
            pa.field(vector_field_name, pa.list_(pa.float32(), vec_size))
        )

    for column_name, column in schema.get_table_columns(table_name).items():
        dtype = cast(TArrowDataType, type_mapper.to_db_type(column))

        if embedding_fields and column_name in embedding_fields:
            metadata = {"embedding_source": "true"}
        else:
            metadata = None

        field = pa.field(column_name, dtype, metadata=metadata)
        arrow_schema.append(field)

    return pa.schema(arrow_schema)
