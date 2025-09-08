import dataclasses
import datetime
from typing import TypeVar

from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import TTableSchema


T = TypeVar("T")

DLT_TO_PY_TYPE_MAPPING: dict[TDataType, type] = {
    "text": str,
    "double": float,
    "bool": bool,
    "timestamp": datetime.datetime,
    "bigint": int,
    "binary": bytes,
    "json": dict,
    "decimal": float,  # TODO add Decimal support
    "wei": float,  # TODO add Decimal support
    "date": datetime.date,
    "time": datetime.time,
}

# we need to avoid name collisions; Python doesn't allow `-` in statically defined names
_DATACLASS_NAME_TEMPLATE = "DltModel-{table_name}"

# TODO make this generic to allow to swap TypeMapper
# TODO this could generate typeddict instead; depends on the use case
# TODO allow user to manually specify fields and factory
def table_schema_to_dataclass(table_schema: TTableSchema) -> type:
    """
    
    Example:
        ```python
        pipeline: dlt.Pipeline = dlt.pipeline(...)
        schema: dlt.Schema = pipe.default_schema
        model: _DltModel-foo = table_schema_to_dataclass(schema.tables["foo"])
        ```
    
    """
    cls_name = _DATACLASS_NAME_TEMPLATE.format(table_name=table_schema["name"])
    fields: list[tuple[str, type]] = []
    for column_name, column in table_schema["columns"].items():
        py_type = DLT_TO_PY_TYPE_MAPPING[column["data_type"]]
        # can handle `nullable` via type annotations
        # if column.get("nullable") is True:
        #    py_type = Optional[py_type]

        attribute = (column_name, py_type)#, field)
        fields.append(attribute)
    
    return dataclasses.make_dataclass(cls_name=cls_name, fields=fields, kw_only=True, slots=True)
