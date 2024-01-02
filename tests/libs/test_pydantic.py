from copy import copy
import pytest
from typing import (
    ClassVar,
    Sequence,
    Mapping,
    Dict,
    MutableMapping,
    MutableSequence,
    Union,
    Optional,
    List,
    Dict,
    Any,
)
from typing_extensions import Annotated, get_args, get_origin
from enum import Enum

from datetime import datetime, date, time  # noqa: I251
from dlt.common import Decimal
from dlt.common import json

from dlt.common.libs.pydantic import (
    DltConfig,
    pydantic_to_table_schema_columns,
    apply_schema_contract_to_model,
    validate_item,
    validate_items,
    create_list_model,
)
from pydantic import BaseModel, Json, AnyHttpUrl, ConfigDict, ValidationError

from dlt.common.schema.exceptions import DataValidationError


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
    bigint_field=1,
    text_field="text",
    timestamp_field=datetime.now(),
    date_field=date.today(),
    decimal_field=Decimal(1.1),
    double_field=1.1,
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


@pytest.mark.parametrize("instance", [True, False])
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
    assert result["list_field"]["data_type"] == "complex"
    assert result["union_field"]["data_type"] == "bigint"
    assert result["optional_field"]["data_type"] == "double"
    assert result["optional_field"]["nullable"] is True
    assert result["blank_dict_field"]["data_type"] == "complex"
    assert result["parametrized_dict_field"]["data_type"] == "complex"
    assert result["str_enum_field"]["data_type"] == "text"
    assert result["int_enum_field"]["data_type"] == "bigint"
    assert result["mixed_enum_int_field"]["data_type"] == "text"
    assert result["mixed_enum_str_field"]["data_type"] == "text"
    assert result["json_field"]["data_type"] == "complex"
    assert result["url_field"]["data_type"] == "text"

    # Any type fields are excluded from schema
    assert "any_field" not in result
    assert "json_any_field" not in result


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
    assert py_ex.value.errors()[0]["loc"] == ("extra_prop",)
    model_freeze = apply_schema_contract_to_model(Model, "freeze")  # type: ignore[arg-type]
    with pytest.raises(ValidationError) as py_ex:
        model_freeze.parse_obj(instance_extra)
    assert py_ex.value.errors()[0]["loc"] == ("extra_prop",)

    # discard row - same as freeze
    model_freeze = apply_schema_contract_to_model(ModelWithConfig, "discard_row")
    with pytest.raises(ValidationError) as py_ex:
        model_freeze.parse_obj(instance_extra)
    assert py_ex.value.errors()[0]["loc"] == ("extra_prop",)

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

    with pytest.raises(NotImplementedError):
        apply_schema_contract_to_model(ModelWithConfig, "evolve", "discard_value")


def test_nested_model_config_propagation() -> None:
    class UserLabel(BaseModel):
        label: str

    class UserAddress(BaseModel):
        street: str
        zip_code: Sequence[int]
        label: Optional[UserLabel]
        ro_labels: Mapping[str, UserLabel]
        wr_labels: MutableMapping[str, List[UserLabel]]
        ro_list: Sequence[UserLabel]
        wr_list: MutableSequence[Dict[str, UserLabel]]

    class User(BaseModel):
        user_id: int
        name: Annotated[str, "PII", "name"]
        created_at: Optional[datetime]
        labels: List[str]
        user_label: UserLabel
        user_labels: List[UserLabel]
        address: Annotated[UserAddress, "PII", "address"]
        unity: Union[UserAddress, UserLabel, Dict[str, UserAddress]]

        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": True}

    model_freeze = apply_schema_contract_to_model(User, "evolve", "freeze")
    from typing import get_type_hints

    # print(model_freeze.__fields__)
    # extra is modified
    assert model_freeze.__fields__["address"].annotation.__name__ == "UserAddressExtraAllow"  # type: ignore[index]
    # annotated is preserved
    assert issubclass(get_origin(model_freeze.__fields__["address"].rebuild_annotation()), Annotated)  # type: ignore[arg-type, index]
    # UserAddress is converted to UserAddressAllow only once
    assert model_freeze.__fields__["address"].annotation is get_args(model_freeze.__fields__["unity"].annotation)[0]  # type: ignore[index]

    # print(User.__fields__)
    # print(User.__fields__["name"].annotation)
    # print(model_freeze.model_config)
    # print(model_freeze.__fields__)
    # print(model_freeze.__fields__["name"].annotation)
    # print(model_freeze.__fields__["address"].annotation)


def test_item_list_validation() -> None:
    class ItemModel(BaseModel):
        b: bool
        opt: Optional[int] = None
        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": False}

    # non validating items removed from the list (both extra and declared)
    discard_model = apply_schema_contract_to_model(ItemModel, "discard_row", "discard_row")
    discard_list_model = create_list_model(discard_model)
    # violate data type
    items = validate_items(
        "items",
        discard_list_model,
        [{"b": True}, {"b": 2, "opt": "not int", "extra": 1.2}, {"b": 3}, {"b": False}],
        "discard_row",
        "discard_row",
    )
    # {"b": 2, "opt": "not int", "extra": 1.2} - note that this will generate 3 errors for the same item
    # and is crucial in our tests when discarding rows
    assert len(items) == 2
    assert items[0].b is True
    assert items[1].b is False
    # violate extra field
    items = validate_items(
        "items",
        discard_list_model,
        [{"b": True}, {"b": 2}, {"b": 3}, {"b": False, "a": False}],
        "discard_row",
        "discard_row",
    )
    assert len(items) == 1
    assert items[0].b is True

    # freeze on non validating items (both extra and declared)
    freeze_model = apply_schema_contract_to_model(ItemModel, "freeze", "freeze")
    freeze_list_model = create_list_model(freeze_model)
    # violate data type
    with pytest.raises(DataValidationError) as val_ex:
        validate_items(
            "items",
            freeze_list_model,
            [{"b": True}, {"b": 2}, {"b": 3}, {"b": False}],
            "freeze",
            "freeze",
        )
    assert val_ex.value.schema_name is None
    assert val_ex.value.table_name == "items"
    assert val_ex.value.column_name == str(("items", 1, "b"))  # pydantic location
    assert val_ex.value.schema_entity == "data_type"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is freeze_list_model
    assert val_ex.value.data_item == {"b": 2}
    # extra type
    with pytest.raises(DataValidationError) as val_ex:
        validate_items(
            "items",
            freeze_list_model,
            [{"b": True}, {"a": 2, "b": False}, {"b": 3}, {"b": False}],
            "freeze",
            "freeze",
        )
    assert val_ex.value.schema_name is None
    assert val_ex.value.table_name == "items"
    assert val_ex.value.column_name == str(("items", 1, "a"))  # pydantic location
    assert val_ex.value.schema_entity == "columns"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is freeze_list_model
    assert val_ex.value.data_item == {"a": 2, "b": False}

    # discard values
    discard_value_model = apply_schema_contract_to_model(ItemModel, "discard_value", "freeze")
    discard_list_model = create_list_model(discard_value_model)
    # violate extra field
    items = validate_items(
        "items",
        discard_list_model,
        [{"b": True}, {"b": False, "a": False}],
        "discard_value",
        "freeze",
    )
    assert len(items) == 2
    # "a" extra got remove
    assert items[1].dict() == {"b": False, "opt": None}
    # violate data type
    with pytest.raises(NotImplementedError):
        apply_schema_contract_to_model(ItemModel, "discard_value", "discard_value")

    # evolve data types and extras
    evolve_model = apply_schema_contract_to_model(ItemModel, "evolve", "evolve")
    evolve_list_model = create_list_model(evolve_model)
    # for data types a lenient model will be created that accepts any type
    items = validate_items(
        "items",
        evolve_list_model,
        [{"b": True}, {"b": 2}, {"b": 3}, {"b": False}],
        "evolve",
        "evolve",
    )
    assert len(items) == 4
    assert items[0].b is True
    assert items[1].b == 2
    # extra fields allowed
    items = validate_items(
        "items",
        evolve_list_model,
        [{"b": True}, {"b": 2}, {"b": 3}, {"b": False, "a": False}],
        "evolve",
        "evolve",
    )
    assert len(items) == 4
    assert items[3].b is False
    assert items[3].a is False  # type: ignore[attr-defined]

    # accept new types but discard new columns
    mixed_model = apply_schema_contract_to_model(ItemModel, "discard_row", "evolve")
    mixed_list_model = create_list_model(mixed_model)
    # for data types a lenient model will be created that accepts any type
    items = validate_items(
        "items",
        mixed_list_model,
        [{"b": True}, {"b": 2}, {"b": 3}, {"b": False}],
        "discard_row",
        "evolve",
    )
    assert len(items) == 4
    assert items[0].b is True
    assert items[1].b == 2
    # extra fields forbidden - full rows discarded
    items = validate_items(
        "items",
        mixed_list_model,
        [{"b": True}, {"b": 2}, {"b": 3}, {"b": False, "a": False}],
        "discard_row",
        "evolve",
    )
    assert len(items) == 3


def test_item_validation() -> None:
    class ItemModel(BaseModel):
        b: bool
        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": False}

    # non validating items removed from the list (both extra and declared)
    discard_model = apply_schema_contract_to_model(ItemModel, "discard_row", "discard_row")
    # violate data type
    assert validate_item("items", discard_model, {"b": 2}, "discard_row", "discard_row") is None
    # violate extra field
    assert (
        validate_item(
            "items", discard_model, {"b": False, "a": False}, "discard_row", "discard_row"
        )
        is None
    )

    # freeze on non validating items (both extra and declared)
    freeze_model = apply_schema_contract_to_model(ItemModel, "freeze", "freeze")
    # violate data type
    with pytest.raises(DataValidationError) as val_ex:
        validate_item("items", freeze_model, {"b": 2}, "freeze", "freeze")
    assert val_ex.value.schema_name is None
    assert val_ex.value.table_name == "items"
    assert val_ex.value.column_name == str(("b",))  # pydantic location
    assert val_ex.value.schema_entity == "data_type"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is freeze_model
    assert val_ex.value.data_item == {"b": 2}
    # extra type
    with pytest.raises(DataValidationError) as val_ex:
        validate_item("items", freeze_model, {"a": 2, "b": False}, "freeze", "freeze")
    assert val_ex.value.schema_name is None
    assert val_ex.value.table_name == "items"
    assert val_ex.value.column_name == str(("a",))  # pydantic location
    assert val_ex.value.schema_entity == "columns"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is freeze_model
    assert val_ex.value.data_item == {"a": 2, "b": False}

    # discard values
    discard_value_model = apply_schema_contract_to_model(ItemModel, "discard_value", "freeze")
    # violate extra field
    item = validate_item(
        "items", discard_value_model, {"b": False, "a": False}, "discard_value", "freeze"
    )
    # "a" extra got removed
    assert item.dict() == {"b": False}

    # evolve data types and extras
    evolve_model = apply_schema_contract_to_model(ItemModel, "evolve", "evolve")
    # for data types a lenient model will be created that accepts any type
    item = validate_item("items", evolve_model, {"b": 2}, "evolve", "evolve")
    assert item.b == 2
    # extra fields allowed
    item = validate_item("items", evolve_model, {"b": False, "a": False}, "evolve", "evolve")
    assert item.b is False
    assert item.a is False  # type: ignore[attr-defined]

    # accept new types but discard new columns
    mixed_model = apply_schema_contract_to_model(ItemModel, "discard_row", "evolve")
    # for data types a lenient model will be created that accepts any type
    item = validate_item("items", mixed_model, {"b": 3}, "discard_row", "evolve")
    assert item.b == 3
    # extra fields forbidden - full rows discarded
    assert (
        validate_item("items", mixed_model, {"b": False, "a": False}, "discard_row", "evolve")
        is None
    )
