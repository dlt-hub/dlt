from copy import copy
import pytest
from typing import ClassVar, Union, Optional, List, Dict, Any
from enum import Enum

from datetime import datetime, date, time  # noqa: I251
from dlt.common import Decimal
from dlt.common import json

from dlt.common.libs.pydantic import DltConfig, pydantic_to_table_schema_columns, apply_schema_contract_to_model, validate_item, validate_items, create_list_model
from pydantic import BaseModel, Json, AnyHttpUrl, ConfigDict, ValidationError


class StrEnum(str, Enum):
    a = "a_value"
    b = "b_value"
    c = "c_value"


class IntEnum(int, Enum):
    a = 0
    b = 1
    c = 2


class MixedEnum(Enum):
    a_int = 0
    b_str = "b_value"
    c_int = 2


class NestedModel(BaseModel):
    nested_field: str


class Model(BaseModel):
    bigint_field: int
    text_field: str
    timestamp_field: datetime
    date_field: date
    decimal_field: Decimal
    double_field: float
    time_field: time

    nested_field: NestedModel
    list_field: List[str]

    union_field: Union[int, str]

    optional_field: Optional[float]

    blank_dict_field: dict  # type: ignore[type-arg]
    parametrized_dict_field: Dict[str, int]

    str_enum_field: StrEnum
    int_enum_field: IntEnum
    # Both of these shouold coerce to str
    mixed_enum_int_field: MixedEnum
    mixed_enum_str_field: MixedEnum

    json_field: Json[List[str]]

    url_field: AnyHttpUrl

    any_field: Any
    json_any_field: Json[Any]


class ModelWithConfig(Model):
    model_config = ConfigDict(frozen=True, extra="allow")


TEST_MODEL_INSTANCE = Model(
    bigint_field=1, text_field="text", timestamp_field=datetime.now(),
    date_field=date.today(), decimal_field=Decimal(1.1), double_field=1.1,
    time_field=time(1, 2, 3, 12345),
    nested_field=NestedModel(nested_field="nested"),
    list_field=["a", "b", "c"],
    union_field=1,
    optional_field=None,
    blank_dict_field={},
    parametrized_dict_field={"a": 1, "b": 2, "c": 3},
    str_enum_field=StrEnum.a,
    int_enum_field=IntEnum.a,
    mixed_enum_int_field=MixedEnum.a_int,
    mixed_enum_str_field=MixedEnum.b_str,
    json_field=json.dumps(["a", "b", "c"]),  # type: ignore[arg-type]
    url_field="https://example.com",  # type: ignore[arg-type]
    any_field="any_string",
    json_any_field=json.dumps("any_string"),
)


@pytest.mark.parametrize('instance', [True, False])
def test_pydantic_model_to_columns(instance: bool) -> None:
    if instance:
        model = TEST_MODEL_INSTANCE
    else:
        model = Model  # type: ignore[assignment]

    result = pydantic_to_table_schema_columns(model)

    assert result["bigint_field"]["data_type"] == "bigint"
    assert result["text_field"]["data_type"] == "text"
    assert result["timestamp_field"]["data_type"] == "timestamp"
    assert result["date_field"]["data_type"] == "date"
    assert result["decimal_field"]["data_type"] == "decimal"
    assert result["double_field"]["data_type"] == "double"
    assert result["time_field"]["data_type"] == "time"
    assert result["nested_field"]["data_type"] == "complex"
    assert result['list_field']['data_type'] == 'complex'
    assert result['union_field']['data_type'] == 'bigint'
    assert result['optional_field']['data_type'] == 'double'
    assert result['optional_field']['nullable'] is True
    assert result['blank_dict_field']['data_type'] == 'complex'
    assert result['parametrized_dict_field']['data_type'] == 'complex'
    assert result['str_enum_field']['data_type'] == 'text'
    assert result['int_enum_field']['data_type'] == 'bigint'
    assert result['mixed_enum_int_field']['data_type'] == 'text'
    assert result['mixed_enum_str_field']['data_type'] == 'text'
    assert result['json_field']['data_type'] == 'complex'
    assert result['url_field']['data_type'] == 'text'

    # Any type fields are excluded from schema
    assert 'any_field' not in result
    assert 'json_any_field' not in result


def test_pydantic_model_skip_complex_types() -> None:
    class SkipNestedModel(Model):
        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": True}

    result = pydantic_to_table_schema_columns(SkipNestedModel)

    assert result["bigint_field"]["data_type"] == "bigint"
    assert "nested_field" not in result
    assert "list_field" not in result
    assert "blank_dict_field" not in result
    assert "parametrized_dict_field" not in result
    assert "json_field" not in result
    assert result["bigint_field"]["data_type"] == "bigint"
    assert result["text_field"]["data_type"] == "text"
    assert result["timestamp_field"]["data_type"] == "timestamp"


def test_model_for_column_mode() -> None:
    # extra prop
    instance_extra = TEST_MODEL_INSTANCE.dict()
    instance_extra["extra_prop"] = "EXTRA"
    # back to string
    instance_extra["json_field"] = json.dumps(["a", "b", "c"])
    instance_extra["json_any_field"] = json.dumps("any_string")

    # evolve - allow extra fields
    model_evolve = apply_schema_contract_to_model(ModelWithConfig, "evolve")
    # assert "frozen" in model_evolve.model_config
    extra_instance = model_evolve.parse_obj(instance_extra)
    assert hasattr(extra_instance, "extra_prop")
    assert extra_instance.extra_prop == "EXTRA"
    model_evolve = apply_schema_contract_to_model(Model, "evolve")  # type: ignore[arg-type]
    extra_instance = model_evolve.parse_obj(instance_extra)
    assert extra_instance.extra_prop == "EXTRA"  # type: ignore[attr-defined]

    # freeze - validation error on extra fields
    model_freeze = apply_schema_contract_to_model(ModelWithConfig, "freeze")
    # assert "frozen" in model_freeze.model_config
    with pytest.raises(ValidationError) as py_ex:
        model_freeze.parse_obj(instance_extra)
    assert py_ex.value.errors()[0]["loc"] == ('extra_prop',)
    model_freeze = apply_schema_contract_to_model(Model, "freeze")  # type: ignore[arg-type]
    with pytest.raises(ValidationError) as py_ex:
        model_freeze.parse_obj(instance_extra)
    assert py_ex.value.errors()[0]["loc"] == ('extra_prop',)

    # discard row - same as freeze
    model_freeze = apply_schema_contract_to_model(ModelWithConfig, "discard_row")
    with pytest.raises(ValidationError) as py_ex:
        model_freeze.parse_obj(instance_extra)
    assert py_ex.value.errors()[0]["loc"] == ('extra_prop',)

    # discard value - ignore extra fields
    model_discard = apply_schema_contract_to_model(ModelWithConfig, "discard_value")
    extra_instance = model_discard.parse_obj(instance_extra)
    assert not hasattr(extra_instance, "extra_prop")
    model_evolve = apply_schema_contract_to_model(Model, "evolve")  # type: ignore[arg-type]
    extra_instance = model_discard.parse_obj(instance_extra)
    assert not hasattr(extra_instance, "extra_prop")

    # evolve data but freeze new columns
    model_freeze = apply_schema_contract_to_model(ModelWithConfig, "evolve", "freeze")
    instance_extra_2 = copy(instance_extra)
    # should parse ok
    model_discard.parse_obj(instance_extra_2)
    # this must fail validation
    instance_extra_2["bigint_field"] = "NOT INT"
    with pytest.raises(ValidationError):
        model_discard.parse_obj(instance_extra_2)
    # let the datatypes evolve
    model_freeze = apply_schema_contract_to_model(ModelWithConfig, "evolve", "evolve")
    print(model_freeze.parse_obj(instance_extra_2).dict())

    with pytest.raises(ValueError):
        apply_schema_contract_to_model(ModelWithConfig, "evolve", "discard_value")


def test_items_validation() -> None:

    class ItemModel(BaseModel):
        b: bool
        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": True}


    item = ItemModel(b=True)
    print(ItemModel.dlt_config)
    print(item.dlt_config)

    #ItemRootModel = RootModel(bool)

    list_model = create_list_model(ItemModel)
    list_model = apply_schema_contract_to_model(list_model, "freeze", "discard_row")

    items = validate_items(list_model, [{"b": True}, {"b": 2}, {"b": 3}, {"b": False}], "freeze", "discard_row")
    assert len(items) == 2
    assert items[0].b is True
    assert items[1].b is False