"""Utilities for creating arrow schemas from table schemas."""
from typing import (
    List,
    cast,
    Optional,
)
import pyarrow as pa
from lancedb.embeddings import TextEmbeddingFunction
from typing_extensions import TypeAlias

from dlt.common import logger
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.json import json
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.typing import DictStrAny


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
    type_mapper: DataTypeMapper,
) -> TArrowField:
    """Creates a PyArrow field from a dlt column schema."""
    dtype = cast(TArrowDataType, type_mapper.to_destination_type(column, None))
    # preserve nullability
    return pa.field(column_name, dtype, nullable=column.get("nullable", True))


def make_arrow_table_schema(
    table_name: str,
    schema: Schema,
    type_mapper: DataTypeMapper,
    vector_column: Optional[str] = None,
    embedding_fields: Optional[List[str]] = None,
    embedding_model_func: Optional[TextEmbeddingFunction] = None,
) -> TArrowSchema:
    """Creates a PyArrow schema from a dlt schema."""
    arrow_schema: List[TArrowField] = []
    columns = schema.get_table_columns(table_name)

    for column_name, column in columns.items():
        field = make_arrow_field_schema(column_name, column, type_mapper)
        arrow_schema.append(field)

    if embedding_fields:
        if vector_column not in columns:
            vec_size = embedding_model_func.ndims()
            arrow_schema.append(pa.field(vector_column, pa.list_(pa.float32(), vec_size)))
        else:
            # bring your own vector
            logger.info(
                f"LanceDb table `{table_name}` in schema `{schema.name}` contains user supplied"
                f" vector column `{vector_column}`. Arrow column type must fit the vector"
                " dimensions."
            )

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
                "vector_column": vector_column,
                "name": name,
                "model": embedding_model_func.safe_model_dump(),
            }
            for source_column in embedding_fields
        ]
        metadata["embedding_functions"] = json.dumps(embedding_functions).encode("utf-8")

    return pa.schema(arrow_schema, metadata=metadata)
