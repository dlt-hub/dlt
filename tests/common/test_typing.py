
from typing import List, Literal, Mapping, MutableMapping, MutableSequence, NewType, Sequence, TypeVar, TypedDict, Optional, Union
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, get_config_if_union_hint
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults

from dlt.common.typing import StrAny, extract_inner_type, extract_optional_type, is_dict_generic_type, is_list_generic_type, is_literal_type, is_newtype_type, is_optional_type, is_typeddict



class TTestTyDi(TypedDict):
    field: str


TTestLi = Literal["a", "b", "c"]
TOptionalLi = Optional[TTestLi]
TOptionalTyDi = Optional[TTestTyDi]


def test_is_typeddict() -> None:
    assert is_typeddict(TTestTyDi) is True
    assert is_typeddict(is_typeddict) is False
    assert is_typeddict(Sequence[str]) is False


def test_is_list_generic_type() -> None:
    # yes - we need a generic type
    assert is_list_generic_type(list) is False
    assert is_list_generic_type(List[str]) is True
    assert is_list_generic_type(Sequence[str]) is True
    assert is_list_generic_type(MutableSequence[str]) is True


def test_is_dict_generic_type() -> None:
    assert is_dict_generic_type(dict) is False
    assert is_dict_generic_type(Mapping[str, str]) is True
    assert is_dict_generic_type(MutableMapping[str, str]) is True


def test_is_literal() -> None:
    assert is_literal_type(TTestLi) is True
    assert is_literal_type("a") is False
    assert is_literal_type(List[str]) is False


def test_optional() -> None:
    assert is_optional_type(TOptionalLi) is True
    assert is_optional_type(TOptionalTyDi) is True
    assert is_optional_type(TTestTyDi) is False
    assert extract_optional_type(TOptionalLi) is TTestLi
    assert extract_optional_type(TOptionalTyDi) is TTestTyDi


def test_is_newtype() -> None:
    assert is_newtype_type(NewType("NT1", str)) is True
    assert is_newtype_type(TypeVar("TV1", bound=str)) is False
    assert is_newtype_type(1) is False


def test_extract_inner_type() -> None:
    assert extract_inner_type(1) == 1
    assert extract_inner_type(str) is str
    assert extract_inner_type(NewType("NT1", str)) is str
    assert extract_inner_type(NewType("NT2", NewType("NT3", int))) is int
    assert extract_inner_type(Optional[NewType("NT3", bool)]) is bool  # noqa
    l_1 = Literal[1, 2, 3]
    assert extract_inner_type(l_1) is int
    nt_l_2 = NewType("NTL2", float)
    assert extract_inner_type(nt_l_2, preserve_new_types=True) is nt_l_2
    l_2 = Literal[nt_l_2(1.238), nt_l_2(2.343)]
    assert extract_inner_type(l_2) is float


def test_get_config_if_union() -> None:
    assert get_config_if_union_hint(str) is None
    assert get_config_if_union_hint(Optional[str]) is None
    assert get_config_if_union_hint(Union[BaseException, str, StrAny]) is None
    assert get_config_if_union_hint(Union[BaseConfiguration, str, StrAny]) is BaseConfiguration
    assert get_config_if_union_hint(Union[str, BaseConfiguration, StrAny]) is BaseConfiguration
    assert get_config_if_union_hint(Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]) is GcpServiceAccountCredentialsWithoutDefaults
