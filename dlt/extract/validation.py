from typing import Optional, Protocol, TypeVar, Generic, Type, Union

try:
    from pydantic import BaseModel as PydanticBaseModel
except ModuleNotFoundError:
    PydanticBaseModel = None  # type: ignore[misc]

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TAnySchemaColumns, ColumnValidator
from dlt.extract.typing import TTableHintTemplate


_TPydanticModel = TypeVar("_TPydanticModel", bound=PydanticBaseModel)


class PydanticValidator(ColumnValidator, Generic[_TPydanticModel]):
    model: Type[_TPydanticModel]
    def __init__(self, model: Type[_TPydanticModel]) -> None:
        self.model = model

    def __call__(self, item: TDataItem) -> _TPydanticModel:
        """Validate a data item agains the pydantic model"""
        if item is None:
            return None
        return self.model.parse_obj(item)


def get_column_validator(columns: TTableHintTemplate[TAnySchemaColumns]) -> Optional[ColumnValidator]:
    if PydanticBaseModel is not None and isinstance(columns, type) and issubclass(columns, PydanticBaseModel):
        return PydanticValidator(columns)
    return None
