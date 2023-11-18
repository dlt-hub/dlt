from typing import Optional, Protocol, TypeVar, Generic, Type, Union, Any, List

try:
    from pydantic import BaseModel as PydanticBaseModel, ValidationError as PydanticValidationError, create_model
except ModuleNotFoundError:
    PydanticBaseModel = None  # type: ignore[misc]

from dlt.extract.exceptions import ValidationError
from dlt.common.typing import TDataItems
from dlt.common.schema.typing import TAnySchemaColumns
from dlt.extract.typing import TTableHintTemplate, ValidateItem


_TPydanticModel = TypeVar("_TPydanticModel", bound=PydanticBaseModel)


class PydanticValidator(ValidateItem, Generic[_TPydanticModel]):
    model: Type[_TPydanticModel]
    def __init__(self, model: Type[_TPydanticModel]) -> None:
        self.model = model

        # Create a model for validating list of items in batch
        self.list_model = create_model(
            "List" + model.__name__,
            items=(List[model], ...)  # type: ignore[valid-type]
        )

    def __call__(self, item: TDataItems, meta: Any = None) -> Union[_TPydanticModel, List[_TPydanticModel]]:
        """Validate a data item against the pydantic model"""
        if item is None:
            return None
        try:
            if isinstance(item, list):
                return self.list_model(items=item).items  # type: ignore[attr-defined, no-any-return]
            return self.model.parse_obj(item)
        except PydanticValidationError as e:
            raise ValidationError(self, item, e) from e

    def __str__(self, *args: Any, **kwargs: Any) -> str:
        return f"PydanticValidator(model={self.model.__qualname__})"


def get_column_validator(columns: TTableHintTemplate[TAnySchemaColumns]) -> Optional[ValidateItem]:
    if PydanticBaseModel is not None and isinstance(columns, type) and issubclass(columns, PydanticBaseModel):
        return PydanticValidator(columns)
    return None
