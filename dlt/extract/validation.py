from typing import Optional, Protocol, TypeVar, Generic, Type, Union, Any

try:
    from pydantic import BaseModel as PydanticBaseModel, ValidationError as PydanticValidationError
except ModuleNotFoundError:
    PydanticBaseModel = None  # type: ignore[misc]

from dlt.extract.exceptions import ValidationError
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TAnySchemaColumns, ColumnValidator
from dlt.extract.typing import TTableHintTemplate


_TPydanticModel = TypeVar("_TPydanticModel", bound=PydanticBaseModel)


class PydanticValidator(ColumnValidator, Generic[_TPydanticModel]):
    model: Type[_TPydanticModel]
    def __init__(self, model: Type[_TPydanticModel]) -> None:
        self.model = model

    def __call__(self, item: TDataItem, meta: Any = None) -> _TPydanticModel:
        """Validate a data item agains the pydantic model"""
        if item is None:
            return None
        try:
            return self.model.parse_obj(item)
        except PydanticValidationError as e:
            raise ValidationError(e) from e


def get_column_validator(columns: TTableHintTemplate[TAnySchemaColumns]) -> Optional[ColumnValidator]:
    if PydanticBaseModel is not None and isinstance(columns, type) and issubclass(columns, PydanticBaseModel):
        return PydanticValidator(columns)
    return None
