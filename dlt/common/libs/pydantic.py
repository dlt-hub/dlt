from typing import Type, Union, get_type_hints, get_args, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.data_types import py_type_to_sc_type, TDataType
from dlt.common.typing import is_optional_type, extract_inner_type, is_list_generic_type, is_dict_generic_type, is_union

try:
    from pydantic import BaseModel, Field, Json
except ImportError:
    raise MissingDependencyException("DLT pydantic Helpers", ["pydantic"], "DLT Helpers for for pydantic.")


def pydantic_to_table_schema_columns(model: Union[BaseModel, Type[BaseModel]], skip_complex_types: bool = False) -> TTableSchemaColumns:
    """Convert a pydantic model to a table schema columns dict

    Args:
        model: The pydantic model to convert. Can be a class or an instance.
        skip_complex_types: If True, columns of complex types (`dict`, `list`, `BaseModel`) will be excluded from the result.

    Returns:
        TTableSchemaColumns: table schema columns dict
    """
    result: TTableSchemaColumns = {}

    fields = model.__fields__
    for field_name, field in fields.items():
        annotation = field.annotation
        if inner_annotation := getattr(annotation, 'inner_type', None):
            # This applies to pydantic.Json fields, the inner type is the type after json parsing
            # (In pydantic 2 the outer annotation is the final type)
            annotation = inner_annotation
        nullable = is_optional_type(annotation)

        if is_union(annotation):
            inner_type = get_args(annotation)[0]
        else:
            inner_type = extract_inner_type(annotation)

        if inner_type is Json:  # Same as `field: Json[Any]`
            inner_type = Any

        if inner_type is Any:  # Any fields will be inferred from data
            continue

        if is_list_generic_type(inner_type):
            inner_type = list
        elif is_dict_generic_type(inner_type) or issubclass(inner_type, BaseModel):
            inner_type = dict

        name = field.alias or field_name
        data_type = py_type_to_sc_type(inner_type)
        if data_type == 'complex' and skip_complex_types:
            continue

        result[name] = {
            "name": name,
            "data_type": data_type,
            "nullable": nullable,
        }

    return result
