"""Utilities for creating arrow schemas from table schemas."""
from typing import (
    Iterator,
    List,
    Union,
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
TArrowData: TypeAlias = Union[pa.Table, pa.RecordBatchReader]


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
    vector_field_name: Optional[str] = None,
    embedding_fields: Optional[List[str]] = None,
    embedding_model_func: Optional[TextEmbeddingFunction] = None,
    embedding_model_dimensions: Optional[int] = None,
) -> TArrowSchema:
    """Creates a PyArrow schema from a dlt schema."""
    arrow_schema: List[TArrowField] = []
    columns = schema.get_table_columns(table_name)

    for column_name, column in columns.items():
        field = make_arrow_field_schema(column_name, column, type_mapper)
        arrow_schema.append(field)

    if embedding_fields:
        if vector_field_name not in columns:
            # User's provided dimension config, if provided, takes precedence.
            vec_size = embedding_model_dimensions or embedding_model_func.ndims()
            arrow_schema.append(pa.field(vector_field_name, pa.list_(pa.float32(), vec_size)))
        else:
            # bring your own vector
            logger.info(
                f"LanceDb table `{table_name}` in schema `{schema.name}` contains user supplied"
                f" vector column `{vector_field_name}`. Arrow column type must fit the vector"
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
                "vector_column": vector_field_name,
                "name": name,
                "model": embedding_model_func.safe_model_dump(),
            }
            for source_column in embedding_fields
        ]
        metadata["embedding_functions"] = json.dumps(embedding_functions).encode("utf-8")

    return pa.schema(arrow_schema, metadata=metadata)


def add_vector_column(
    records: TArrowData, table_schema: TArrowSchema, vector_column: str
) -> TArrowData:
    """Inserts a null `vector_column` at the index the table holds it at, when records omit it.

    A merge matches the payload schema against the table by name, order and type.
    """
    # vector column already there
    if vector_column in records.schema.names or vector_column not in table_schema.names:
        return records

    col = table_schema.field(vector_column)
    idx = table_schema.get_field_index(vector_column)

    if isinstance(records, pa.RecordBatchReader):
        fields = list(records.schema)
        out_schema = pa.schema(
            fields[:idx] + [col] + fields[idx:], metadata=records.schema.metadata
        )

        def batches() -> Iterator[pa.RecordBatch]:
            for batch in records:
                yield batch.add_column(idx, col, pa.nulls(batch.num_rows, type=col.type))

        return pa.RecordBatchReader.from_batches(out_schema, batches())

    return records.add_column(idx, col, pa.nulls(len(records), type=col.type))
