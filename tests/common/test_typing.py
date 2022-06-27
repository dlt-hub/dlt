
from typing import List, Literal, Mapping, MutableMapping, MutableSequence, Sequence, TypedDict, Optional

from dlt.common.typing import extract_optional_type, is_dict_generic_type, is_list_generic_type, is_literal_type, is_optional_type, is_typeddict



class TTestTyDi(TypedDict):
    field: str


TTestLi = Literal["a", "b", "c"]
TOptionalLi = Optional[TTestLi]
TOptionalTyDi = Optional[TTestTyDi]


def test_is_typeddict() -> None:
    assert is_typeddict(TTestTyDi) is True
    assert is_typeddict(is_typeddict) is False
    assert is_typeddict(Sequence[str]) is False


def test_is_list_type() -> None:
    # yes - we need a generic type
    assert is_list_generic_type(list) is False
    assert is_list_generic_type(List[str]) is True
    assert is_list_generic_type(Sequence[str]) is True
    assert is_list_generic_type(MutableSequence[str]) is True


def test_is_dict_type() -> None:
    assert is_dict_generic_type(dict) is False
    assert is_dict_generic_type(Mapping[str, str]) is True
    assert is_dict_generic_type(MutableMapping[str, str]) is True


def test_literal() -> None:
    assert is_literal_type(TTestLi) is True
    assert is_literal_type("a") is False
    assert is_literal_type(List[str]) is False


def test_optional() -> None:
    assert is_optional_type(TOptionalLi) is True
    assert is_optional_type(TOptionalTyDi) is True
    assert is_optional_type(TTestTyDi) is False
    assert extract_optional_type(TOptionalLi) is TTestLi
    assert extract_optional_type(TOptionalTyDi) is TTestTyDi
