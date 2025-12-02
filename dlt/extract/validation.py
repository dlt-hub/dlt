from typing import Optional, Tuple, TypeVar, Generic, Type, Union, Any, List
from dlt.common.schema.schema import Schema

try:
    from pydantic import BaseModel as PydanticBaseModel
except ModuleNotFoundError:
    PydanticBaseModel = Any  # type: ignore[misc, assignment]

from dlt.common.typing import TDataItems
from dlt.common.schema.typing import TAnySchemaColumns, TSchemaContract, TSchemaEvolutionMode
from dlt.extract.items import TTableHintTemplate
from dlt.extract.items_transform import BaseItemTransform, ValidateItem


_TPydanticModel = TypeVar("_TPydanticModel", bound=PydanticBaseModel)


class PydanticValidator(ValidateItem, Generic[_TPydanticModel]):
    model: Type[_TPydanticModel]

    def __init__(
        self,
        model: Type[_TPydanticModel],
        column_mode: TSchemaEvolutionMode,
        data_mode: TSchemaEvolutionMode,
    ) -> None:
        from dlt.common.libs.pydantic import apply_schema_contract_to_model, create_list_model

        BaseItemTransform.__init__(self)
        self.column_mode: TSchemaEvolutionMode = column_mode
        self.data_mode: TSchemaEvolutionMode = data_mode
        self.model = apply_schema_contract_to_model(model, column_mode, data_mode)
        self.list_model = create_list_model(self.model, data_mode)

    def __call__(self, item: TDataItems, meta: Any = None) -> TDataItems:
        if item is None:
            return None

        from dlt.common.libs.pydantic import validate_and_filter_item, validate_and_filter_items

        if isinstance(item, list):
            input_is_model = bool(item) and isinstance(item[0], PydanticBaseModel)
            validated_list = validate_and_filter_items(
                self.table_name, self.list_model, item, self.column_mode, self.data_mode
            )
            if input_is_model:
                input_fields = set(item[0].__class__.model_fields.keys())
                validated_fields = set(validated_list[0].__class__.model_fields.keys())
                if input_fields.issubset(validated_fields):
                    return validated_list
            return [m.dict(by_alias=True) for m in validated_list]

        input_is_model = isinstance(item, PydanticBaseModel)
        validated = validate_and_filter_item(
            self.table_name, self.model, item, self.column_mode, self.data_mode
        )
        if validated is None:
            return None
        if input_is_model:
            input_fields = set(item.__class__.model_fields.keys())
            validated_fields = set(validated.__class__.model_fields.keys())
            if input_fields.issubset(validated_fields):
                return validated
        return validated.dict(by_alias=True)


def create_item_validator(
    columns: TTableHintTemplate[TAnySchemaColumns],
    schema_contract: TTableHintTemplate[TSchemaContract] = None,
) -> Tuple[Optional[ValidateItem], TTableHintTemplate[TSchemaContract]]:
    """Creates item validator for a `columns` definition and a `schema_contract`

    Returns a tuple (validator, schema contract). If validator could not be created, returns None at first position.
    If schema_contract was not specified a default schema contract for given validator will be returned
    """
    if (
        PydanticBaseModel is not None
        and isinstance(columns, type)
        and issubclass(columns, PydanticBaseModel)
    ):
        assert not callable(
            schema_contract
        ), "schema_contract cannot be dynamic for Pydantic item validator"

        from dlt.common.libs.pydantic import extra_to_column_mode, get_extra_from_model

        # freeze the columns if we have a fully defined table and no other explicit contract
        expanded_schema_contract = Schema.expand_schema_contract_settings(
            schema_contract,
            # corresponds to default Pydantic behavior
            default={
                "tables": "evolve",
                "columns": extra_to_column_mode(get_extra_from_model(columns)),
                "data_type": "freeze",
            },
        )
        return (
            PydanticValidator(
                columns, expanded_schema_contract["columns"], expanded_schema_contract["data_type"]
            ),
            schema_contract or expanded_schema_contract,
        )
    return None, schema_contract
