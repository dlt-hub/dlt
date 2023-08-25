from typing import Type, Union, get_type_hints, get_args

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns, TColumnSchema
from dlt.common.data_types import py_type_to_sc_type, TDataType
from dlt.common.typing import is_optional_type, extract_inner_type, is_list_generic_type, is_dict_generic_type, is_union

try:
    from pydantic import BaseModel, Field
except ImportError:
    raise MissingDependencyException("DLT pydantic Helpers", ["pydantic"], "DLT Helpers for for pydantic.")


def pydantic_to_table_schema_columns(model: Union[BaseModel, Type[BaseModel]]) -> TTableSchemaColumns:
    """Convert a pydantic model to a table schema columns dict

    Args:
        model: The pydantic model to convert. Can be a class or an instance.

    Returns:
        TTableSchemaColumns: table schema columns dict
    """
    result: TTableSchemaColumns = {}

    fields = model.__fields__
    for field_name, field in fields.items():  # type: ignore[union-attr]
        annotation = field.annotation
        nullable = is_optional_type(annotation)

        if is_union(annotation):
            inner_type = get_args(annotation)[0]
        else:
            inner_type = extract_inner_type(annotation)

        if is_list_generic_type(inner_type):
            inner_type = list
        elif is_dict_generic_type(inner_type) or issubclass(inner_type, BaseModel):
            inner_type = dict

        name = field.alias or field_name

        result[name] = {
            "name": name,
            "data_type": py_type_to_sc_type(inner_type),
            "nullable": nullable,
        }

    return result
