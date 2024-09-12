import sys
from copy import copy
from dataclasses import dataclass, field
import uuid
import pytest
from typing import (
    ClassVar,
    Final,
    Generic,
    Sequence,
    Mapping,
    Dict,
    MutableMapping,
    MutableSequence,
    TypeVar,
    Union,
    Optional,
    List,
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
    validate_and_filter_item,
    validate_and_filter_items,
    create_list_model,
)
from pydantic import UUID4, BaseModel, Json, AnyHttpUrl, ConfigDict, ValidationError

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


class BookGenre(str, Enum):
    scifi = "scifi"
    action = "action"
    thriller = "thriller"


@dataclass
class BookInfo:
    isbn: Optional[str] = field(default="ISBN")
    author: Optional[str] = field(default="Charles Bukowski")


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
    account_id: UUID4
    optional_uuid: Optional[UUID4]
    name: Annotated[str, "PII", "name"]
    favorite_book: Annotated[Union[Annotated[BookInfo, "meta"], BookGenre, None], "union metadata"]
    created_at: Optional[datetime]
    labels: List[str]
    user_label: UserLabel
    user_labels: List[UserLabel]
    address: Annotated[UserAddress, "PII", "address"]
    uuid_or_str: Union[str, UUID4, None]
    unity: Union[UserAddress, UserLabel, Dict[str, UserAddress]]
    # NOTE: added "int" because this type was clashing with a type
    # in a delta-rs library that got cached and that re-orders the union
    location: Annotated[Optional[Union[str, List[str], int]], None]
    something_required: Annotated[Union[str, int], type(None)]
    final_location: Final[Annotated[Union[str, int], None]]  # type: ignore[misc]
    final_optional: Final[Annotated[Optional[str], None]]  # type: ignore[misc]

    dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}


USER_INSTANCE_DATA = dict(
    user_id=1,
    account_id=uuid.uuid4(),
    optional_uuid=None,
    favorite_book=BookInfo(isbn="isbn-xyz", author="author"),
    name="random name",
    created_at=datetime.now(),
    labels=["str"],
    user_label=dict(label="123"),
    user_labels=[
        dict(label="123"),
    ],
    address=dict(
        street="random street",
        zip_code=[1234566, 4567789],
        label=dict(label="123"),
        ro_labels={
            "x": dict(label="123"),
        },
        wr_labels={
            "y": [
                dict(label="123"),
            ]
        },
        ro_list=[
            dict(label="123"),
        ],
        wr_list=[
            {
                "x": dict(label="123"),
            }
        ],
    ),
    unity=dict(label="123"),
    uuid_or_str=uuid.uuid4(),
    location="Florida keys",
    final_location="Ginnie Springs",
    something_required=123,
    final_optional=None,
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
    assert result["nested_field"]["data_type"] == "json"
    assert result["list_field"]["data_type"] == "json"
    assert result["union_field"]["data_type"] == "bigint"
    assert result["optional_field"]["data_type"] == "double"
    assert result["optional_field"]["nullable"] is True
    assert result["blank_dict_field"]["data_type"] == "json"
    assert result["parametrized_dict_field"]["data_type"] == "json"
    assert result["str_enum_field"]["data_type"] == "text"
    assert result["int_enum_field"]["data_type"] == "bigint"
    assert result["mixed_enum_int_field"]["data_type"] == "text"
    assert result["mixed_enum_str_field"]["data_type"] == "text"
    assert result["json_field"]["data_type"] == "json"
    assert result["url_field"]["data_type"] == "text"

    # Any type fields are excluded from schema
    assert "any_field" not in result
    assert "json_any_field" not in result


def test_pydantic_model_to_columns_annotated() -> None:
    # We need to check if pydantic_to_table_schema_columns is idempotent
    # and can generate the same schema from the class and from the class instance.
    schema_from_user_class = pydantic_to_table_schema_columns(User)
    schema_from_user_instance = pydantic_to_table_schema_columns(User(**USER_INSTANCE_DATA))  # type: ignore
    assert schema_from_user_class == schema_from_user_instance
    assert schema_from_user_class["location"]["nullable"] is True
    assert schema_from_user_class["final_location"]["nullable"] is False
    assert schema_from_user_class["something_required"]["nullable"] is False
    assert schema_from_user_class["final_optional"]["nullable"] is True


def test_pydantic_model_skip_nested_types() -> None:
    class SkipNestedModel(Model):
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

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
    # TODO: finish writing this test
    model_freeze = apply_schema_contract_to_model(User, "evolve", "freeze")
    from typing import get_type_hints

    # print(model_freeze.__fields__)
    # extra is modified
    assert model_freeze.__fields__["address"].annotation.__name__ == "UserAddressExtraAllow"  # type: ignore[index]
    # annotated is preserved
    type_origin = get_origin(model_freeze.__fields__["address"].rebuild_annotation())  # type: ignore[index]
    assert issubclass(type_origin, Annotated)  # type: ignore[arg-type]
    # UserAddress is converted to UserAddressAllow only once
    type_annotation = model_freeze.__fields__["address"].annotation  # type: ignore[index]
    assert type_annotation is get_args(model_freeze.__fields__["unity"].annotation)[0]  # type: ignore[index]

    # print(User.__fields__)
    # print(User.__fields__["name"].annotation)
    # print(model_freeze.model_config)
    # print(model_freeze.__fields__)
    # print(model_freeze.__fields__["name"].annotation)
    # print(model_freeze.__fields__["address"].annotation)


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Runs only on Python 3.10 and later")
def test_nested_model_config_propagation_optional_with_pipe():
    """We would like to test that using Optional and new | syntax works as expected
    when generating a schema thus two versions of user model are defined and both instantiated
    then we generate schema for both and check if results are the same.
    """

    class UserLabelPipe(BaseModel):
        label: str

    class UserAddressPipe(BaseModel):
        street: str
        zip_code: Sequence[int]
        label: UserLabelPipe | None  # type: ignore[misc, syntax, unused-ignore]
        ro_labels: Mapping[str, UserLabelPipe]
        wr_labels: MutableMapping[str, List[UserLabelPipe]]
        ro_list: Sequence[UserLabelPipe]
        wr_list: MutableSequence[Dict[str, UserLabelPipe]]

    class UserPipe(BaseModel):
        user_id: int
        name: Annotated[str, "PII", "name"]
        created_at: datetime | None  # type: ignore[misc, syntax, unused-ignore]
        labels: List[str]
        user_label: UserLabelPipe
        user_labels: List[UserLabelPipe]
        address: Annotated[UserAddressPipe, "PII", "address"]
        unity: Union[UserAddressPipe, UserLabelPipe, Dict[str, UserAddressPipe]]
        location: Annotated[Union[str, List[str]] | None, None]  # type: ignore[misc, syntax, unused-ignore]
        something_required: Annotated[Union[str, int], type(None)]
        final_location: Final[Annotated[Union[str, int], None]]  # type: ignore[misc, syntax, unused-ignore]
        final_optional: Final[Annotated[str | None, None]]  # type: ignore[misc, syntax, unused-ignore]

        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

    # TODO: move to separate test
    model_freeze = apply_schema_contract_to_model(UserPipe, "evolve", "freeze")
    from typing import get_type_hints

    # print(model_freeze.__fields__)
    # extra is modified
    assert model_freeze.__fields__["address"].annotation.__name__ == "UserAddressPipeExtraAllow"  # type: ignore[index]
    # annotated is preserved
    type_origin = get_origin(model_freeze.__fields__["address"].rebuild_annotation())  # type: ignore[index]
    assert issubclass(type_origin, Annotated)  # type: ignore[arg-type]
    # UserAddress is converted to UserAddressAllow only once
    type_annotation = model_freeze.__fields__["address"].annotation  # type: ignore[index]
    assert type_annotation is get_args(model_freeze.__fields__["unity"].annotation)[0]  # type: ignore[index]

    # We need to check if pydantic_to_table_schema_columns is idempotent
    # and can generate the same schema from the class and from the class instance.

    user = UserPipe(**USER_INSTANCE_DATA)  # type: ignore
    schema_from_user_class = pydantic_to_table_schema_columns(UserPipe)
    schema_from_user_instance = pydantic_to_table_schema_columns(user)
    assert schema_from_user_class == schema_from_user_instance
    assert schema_from_user_class["location"]["nullable"] is True
    assert schema_from_user_class["final_location"]["nullable"] is False
    assert schema_from_user_class["something_required"]["nullable"] is False
    assert schema_from_user_class["final_optional"]["nullable"] is True


def test_item_list_validation() -> None:
    class ItemModel(BaseModel):
        b: bool
        opt: Optional[int] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": False}

    # non validating items removed from the list (both extra and declared)
    discard_model = apply_schema_contract_to_model(ItemModel, "discard_row", "discard_row")
    discard_list_model = create_list_model(discard_model)
    # violate data type
    items = validate_and_filter_items(
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
    items = validate_and_filter_items(
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
        validate_and_filter_items(
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
        validate_and_filter_items(
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
    items = validate_and_filter_items(
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
    items = validate_and_filter_items(
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
    items = validate_and_filter_items(
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
    items = validate_and_filter_items(
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
    items = validate_and_filter_items(
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
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": False}

    # non validating items removed from the list (both extra and declared)
    discard_model = apply_schema_contract_to_model(ItemModel, "discard_row", "discard_row")
    # violate data type
    assert (
        validate_and_filter_item("items", discard_model, {"b": 2}, "discard_row", "discard_row")
        is None
    )
    # violate extra field
    assert (
        validate_and_filter_item(
            "items", discard_model, {"b": False, "a": False}, "discard_row", "discard_row"
        )
        is None
    )

    # freeze on non validating items (both extra and declared)
    freeze_model = apply_schema_contract_to_model(ItemModel, "freeze", "freeze")
    # violate data type
    with pytest.raises(DataValidationError) as val_ex:
        validate_and_filter_item("items", freeze_model, {"b": 2}, "freeze", "freeze")
    assert val_ex.value.schema_name is None
    assert val_ex.value.table_name == "items"
    assert val_ex.value.column_name == str(("b",))  # pydantic location
    assert val_ex.value.schema_entity == "data_type"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is freeze_model
    assert val_ex.value.data_item == {"b": 2}
    # extra type
    with pytest.raises(DataValidationError) as val_ex:
        validate_and_filter_item("items", freeze_model, {"a": 2, "b": False}, "freeze", "freeze")
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
    item = validate_and_filter_item(
        "items", discard_value_model, {"b": False, "a": False}, "discard_value", "freeze"
    )
    # "a" extra got removed
    assert item.dict() == {"b": False}

    # evolve data types and extras
    evolve_model = apply_schema_contract_to_model(ItemModel, "evolve", "evolve")
    # for data types a lenient model will be created that accepts any type
    item = validate_and_filter_item("items", evolve_model, {"b": 2}, "evolve", "evolve")
    assert item.b == 2
    # extra fields allowed
    item = validate_and_filter_item(
        "items", evolve_model, {"b": False, "a": False}, "evolve", "evolve"
    )
    assert item.b is False
    assert item.a is False  # type: ignore[attr-defined]

    # accept new types but discard new columns
    mixed_model = apply_schema_contract_to_model(ItemModel, "discard_row", "evolve")
    # for data types a lenient model will be created that accepts any type
    item = validate_and_filter_item("items", mixed_model, {"b": 3}, "discard_row", "evolve")
    assert item.b == 3
    # extra fields forbidden - full rows discarded
    assert (
        validate_and_filter_item(
            "items", mixed_model, {"b": False, "a": False}, "discard_row", "evolve"
        )
        is None
    )


class ChildModel(BaseModel):
    child_attribute: str
    optional_child_attribute: Optional[str] = None


class Parent(BaseModel):
    child: ChildModel
    optional_parent_attribute: Optional[str] = None


@pytest.mark.parametrize("config_attr", ("skip_nested_types", "skip_complex_types"))
def test_pydantic_model_flattened_when_skip_nested_types_is_true(config_attr: str):
    class MyParent(Parent):
        dlt_config: ClassVar[DltConfig] = {config_attr: True}  # type: ignore

    schema = pydantic_to_table_schema_columns(MyParent)

    assert schema == {
        "child__child_attribute": {
            "data_type": "text",
            "name": "child__child_attribute",
            "nullable": False,
        },
        "child__optional_child_attribute": {
            "data_type": "text",
            "name": "child__optional_child_attribute",
            "nullable": True,
        },
        "optional_parent_attribute": {
            "data_type": "text",
            "name": "optional_parent_attribute",
            "nullable": True,
        },
    }


@pytest.mark.parametrize("config_attr", ("skip_nested_types", "skip_complex_types"))
def test_considers_model_as_complex_when_skip_nested_types_is_false(config_attr: str):
    class MyParent(Parent):
        data_dictionary: Dict[str, Any] = None
        dlt_config: ClassVar[DltConfig] = {config_attr: False}  # type: ignore

    schema = pydantic_to_table_schema_columns(MyParent)

    assert schema == {
        "child": {"data_type": "json", "name": "child", "nullable": False},
        "data_dictionary": {"data_type": "json", "name": "data_dictionary", "nullable": False},
        "optional_parent_attribute": {
            "data_type": "text",
            "name": "optional_parent_attribute",
            "nullable": True,
        },
    }


def test_considers_dictionary_as_complex_when_skip_nested_types_is_false():
    class MyParent(Parent):
        data_list: List[str] = []
        data_dictionary: Dict[str, Any] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": False}

    schema = pydantic_to_table_schema_columns(MyParent)

    assert schema["data_dictionary"] == {
        "data_type": "json",
        "name": "data_dictionary",
        "nullable": False,
    }

    assert schema["data_list"] == {
        "data_type": "json",
        "name": "data_list",
        "nullable": False,
    }


def test_skip_json_types_when_skip_nested_types_is_true_and_field_is_not_pydantic_model():
    class MyParent(Parent):
        data_list: List[str] = []
        data_dictionary: Dict[str, Any] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

    schema = pydantic_to_table_schema_columns(MyParent)

    assert "data_dictionary" not in schema
    assert "data_list" not in schema
