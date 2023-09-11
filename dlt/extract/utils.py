from typing import Union, List, Any, Sequence, cast
from collections.abc import Mapping as C_Mapping

from dlt.common.exceptions import MissingDependencyException
from dlt.extract.typing import TTableHintTemplate, TDataItem, TFunHintTemplate
from dlt.common.schema.typing import TColumnNames, TAnySchemaColumns, TTableSchemaColumns
from dlt.common.typing import TDataItem

try:
    from dlt.common.libs import pydantic
except MissingDependencyException:
    pydantic = None


def resolve_column_value(column_hint: TTableHintTemplate[TColumnNames], item: TDataItem) -> Union[Any, List[Any]]:
    """Extract values from the data item given a column hint.
    Returns either a single value or list of values when hint is a composite.
    """
    columns = column_hint(item) if callable(column_hint) else column_hint
    if isinstance(columns, str):
        return item[columns]
    return [item[k] for k in columns]


def ensure_table_schema_columns(columns: TAnySchemaColumns) -> TTableSchemaColumns:
    """Convert supported column schema types to a column dict which
    can be used in resource schema.

    Args:
        columns: A dict of column schemas, a list of column schemas, or a pydantic model
    """
    if isinstance(columns, C_Mapping):
        # fill missing names in short form was used
        for col_name in columns:
            columns[col_name]["name"] = col_name
        return columns
    elif isinstance(columns, Sequence):
        # Assume list of columns
        return {col['name']: col for col in columns}
    elif pydantic is not None and (
        isinstance(columns, pydantic.BaseModel) or issubclass(columns, pydantic.BaseModel)
    ):
        return pydantic.pydantic_to_table_schema_columns(columns)

    raise ValueError(f"Unsupported columns type: {type(columns)}")


def ensure_table_schema_columns_hint(columns: TTableHintTemplate[TAnySchemaColumns]) -> TTableHintTemplate[TTableSchemaColumns]:
    """Convert column schema hint to a hint returning `TTableSchemaColumns`.
    A callable hint is wrapped in another function which converts the original result.
    """
    if callable(columns) and not isinstance(columns, type):
        def wrapper(item: TDataItem) -> TTableSchemaColumns:
            return ensure_table_schema_columns(cast(TFunHintTemplate[TAnySchemaColumns], columns)(item))
        return wrapper

    return ensure_table_schema_columns(columns)
