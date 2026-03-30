"""Utilities for creating arrow schemas from table schemas."""
from typing import cast
import pyarrow as pa
from typing_extensions import TypeAlias

from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.schema import TColumnSchema


TArrowSchema: TypeAlias = pa.Schema
TArrowDataType: TypeAlias = pa.DataType
TArrowField: TypeAlias = pa.Field


def make_arrow_field_schema(
    column_name: str,
    column: TColumnSchema,
    type_mapper: DataTypeMapper,
) -> TArrowField:
    """Creates a PyArrow field from a dlt column schema."""
    dtype = cast(TArrowDataType, type_mapper.to_destination_type(column, None))
    # preserve nullability
    return pa.field(column_name, dtype, nullable=column.get("nullable", True))
