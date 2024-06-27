"""Utilities for creating arrow schemas from table schemas."""

from dlt.common.json import json
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
) -> TArrowField:
    """Creates a PyArrow field from a dlt column schema."""
    dtype = cast(TArrowDataType, type_mapper.to_db_type(column))
    return pa.field(column_name, dtype)


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
        # User's provided dimension config, if provided, takes precedence.
        vec_size = embedding_model_dimensions or embedding_model_func.ndims()
        arrow_schema.append(pa.field(vector_field_name, pa.list_(pa.float32(), vec_size)))

    for column_name, column in schema.get_table_columns(table_name).items():
        field = make_arrow_field_schema(column_name, column, type_mapper)
        arrow_schema.append(field)

    metadata = {}
    if embedding_model_func:
        # Get the registered alias if it exists, otherwise use the class name.
        name = getattr(
            embedding_model_func,
            "__embedding_function_registry_alias__",
            embedding_model_func.__class__.__name__,
        )
        embedding_functions = [
            {
                "source_column": source_column,
                "vector_column": vector_field_name,
                "name": name,
                "model": embedding_model_func.safe_model_dump(),
            }
            for source_column in embedding_fields
        ]
        metadata["embedding_functions"] = json.dumps(embedding_functions).encode("utf-8")

    return pa.schema(arrow_schema, metadata=metadata)
