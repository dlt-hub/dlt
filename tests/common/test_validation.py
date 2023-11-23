from copy import deepcopy
import pytest
import yaml
from typing import Dict, List, Literal, Mapping, Sequence, TypedDict, Optional, Union

from dlt.common import json
from dlt.common.exceptions import DictValidationException
from dlt.common.schema.typing import TStoredSchema, TColumnSchema
from dlt.common.schema.utils import simple_regex_validator
from dlt.common.typing import DictStrStr, StrStr
from dlt.common.validation import validate_dict, validate_dict_ignoring_xkeys


TLiteral = Literal["uno", "dos", "tres"]


class TDict(TypedDict):
    field: TLiteral


class TTestRecord(TypedDict):
    f_bool: bool
    f_str: str
    f_int: int
    f_float: float
    f_optional_int: Optional[int]
    f_list_simple: List[str]
    f_seq_simple: Sequence[str]
    f_seq_optional_str: Optional[Sequence[str]]
    f_seq_of_optional_int: Sequence[Optional[int]]
    f_list_of_dict: Optional[Sequence[TColumnSchema]]
    f_dict_simple: DictStrStr
    f_map_simple: StrStr
    f_map_of_dict: Mapping[str, TColumnSchema]
    f_column: TColumnSchema
    f_literal: TLiteral
    f_literal_optional: Optional[TLiteral]
    f_seq_literal: Sequence[Optional[TLiteral]]
    f_optional_union: Optional[Union[TLiteral, TDict]]


TEST_COL: TColumnSchema = {"name": "col1", "data_type": "bigint", "nullable": False}

TEST_COL_LIST: List[TColumnSchema] = [
    {"name": "col1", "data_type": "bigint", "nullable": False},
    {"name": "col2", "data_type": "double", "nullable": False},
    {"name": "col3", "data_type": "bool", "nullable": False},
]

TEST_DOC: TTestRecord = {
    "f_bool": True,
    "f_str": "test",
    "f_int": 121,
    "f_float": 121.1,
    "f_optional_int": -1291,
    "f_list_simple": ["a"],
    "f_seq_simple": ["x", "y"],
    "f_seq_optional_str": ["opt1", "opt2"],
    "f_seq_of_optional_int": [1, 2, 3],
    "f_list_of_dict": TEST_COL_LIST,
    "f_dict_simple": {"col1": "map_me"},
    "f_map_simple": {"col1": "map_me"},
    "f_map_of_dict": {"col1": deepcopy(TEST_COL)},
    "f_column": deepcopy(TEST_COL),
    "f_literal": "uno",
    "f_literal_optional": "dos",
    "f_seq_literal": ["uno", "dos", "tres"],
    "f_optional_union": {"field": "uno"},
}


@pytest.fixture
def test_doc() -> TTestRecord:
    return deepcopy(TEST_DOC)


def test_validate_schema_cases() -> None:
    with open(
        "tests/common/cases/schemas/eth/ethereum_schema_v8.yml", mode="r", encoding="utf-8"
    ) as f:
        schema_dict: TStoredSchema = yaml.safe_load(f)

    validate_dict_ignoring_xkeys(
        spec=TStoredSchema,
        doc=schema_dict,
        path=".",
        validator_f=simple_regex_validator,
    )

    # with open("tests/common/cases/schemas/rasa/event.schema.json") as f:
    #     schema_dict: TStoredSchema = json.load(f)

    # validate_dict(TStoredSchema, schema_dict, ".", lambda k: not k.startswith("x-"))


def test_validate_doc() -> None:
    validate_dict(TTestRecord, TEST_DOC, ".")


def test_missing_values(test_doc: TTestRecord) -> None:
    del test_doc["f_bool"]  # type: ignore[misc]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert "f_bool" in str(e.value)

    # data type is optional now
    # remove prop at child document
    test_doc = deepcopy(TEST_DOC)
    del test_doc["f_list_of_dict"][0]["data_type"]
    validate_dict(TTestRecord, test_doc, ".")

    test_doc = deepcopy(TEST_DOC)
    # data type is optional now
    del test_doc["f_map_of_dict"]["col1"]["data_type"]
    validate_dict(TTestRecord, test_doc, ".")


def test_extra_values(test_doc: TTestRecord) -> None:
    # extra element at the top
    test_doc["f_extra"] = 1  # type: ignore[typeddict-unknown-key]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert "f_extra" in str(e.value)

    # add prop at child document
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_list_of_dict"][0]["f_extra"] = 1  # type: ignore[typeddict-unknown-key]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert "f_extra" in str(e.value)
    assert e.value.path == "./f_list_of_dict[0]"

    test_doc = deepcopy(TEST_DOC)
    test_doc["f_map_of_dict"]["col1"]["f_extra"] = "2"  # type: ignore[typeddict-unknown-key]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert "f_extra" in str(e.value)
    assert e.value.path == "./f_map_of_dict[col1]"


def test_invalid_types(test_doc: TTestRecord) -> None:
    test_doc["f_bool"] = "a"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_bool"
    assert e.value.value == "a"

    # break list type
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_list_simple"] = test_doc["f_map_of_dict"]  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_list_simple"
    assert e.value.value == test_doc["f_map_of_dict"]

    # break typed dict
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_column"] = "break"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_column"
    assert e.value.value == "break"

    # break dict type
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_map_simple"] = "break"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_map_simple"
    assert e.value.value == "break"

    # break child type
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_column"]["cluster"] = 1  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "cluster"
    assert e.value.value == 1
    assert e.value.path == "./f_column"

    test_doc = deepcopy(TEST_DOC)
    test_doc["f_seq_optional_str"][0] = 1  # type: ignore[index]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_seq_optional_str[0]"
    assert e.value.value == 1
    assert e.value.path == "."

    # break literal
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_literal"] = "cinco"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_literal"
    assert e.value.value == "cinco"

    # break optional literal
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_literal_optional"] = "cinco"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_literal_optional"
    assert e.value.value == "cinco"

    # break literal in a list
    test_doc = deepcopy(TEST_DOC)
    test_doc["f_seq_literal"][2] = "cinco"  # type: ignore[index]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_seq_literal[2]"
    assert e.value.value == "cinco"


def test_optional(test_doc: TTestRecord) -> None:
    del test_doc["f_seq_optional_str"]  # type: ignore[misc]
    # still validates
    validate_dict(TTestRecord, test_doc, ".")


def test_filter(test_doc: TTestRecord) -> None:
    test_doc["x-extra"] = "x-annotation"  # type: ignore[typeddict-unknown-key]
    # remove x-extra with a filter
    validate_dict(TTestRecord, test_doc, ".", filter_f=lambda k: k != "x-extra")


def test_nested_union(test_doc: TTestRecord) -> None:
    test_doc["f_optional_union"] = {"field": "uno"}
    validate_dict(TTestRecord, TEST_DOC, ".")

    test_doc["f_optional_union"] = {"field": "not valid"}  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_optional_union"
    assert e.value.value == {"field": "not valid"}

    test_doc["f_optional_union"] = "dos"
    validate_dict(TTestRecord, test_doc, ".")

    test_doc["f_optional_union"] = "blah"  # type: ignore[typeddict-item]
    with pytest.raises(DictValidationException) as e:
        validate_dict(TTestRecord, test_doc, ".")
    assert e.value.field == "f_optional_union"
    assert e.value.value == "blah"
