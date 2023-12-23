from typing import (
    ClassVar,
    Final,
    List,
    Literal,
    Mapping,
    MutableMapping,
    MutableSequence,
    NewType,
    Sequence,
    TypeVar,
    TypedDict,
    Optional,
    Union,
)
from typing_extensions import Annotated, get_args

from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    get_config_if_union_hint,
)
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.typing import (
    StrAny,
    extract_inner_type,
    extract_union_types,
    is_dict_generic_type,
    is_list_generic_type,
    is_literal_type,
    is_newtype_type,
    is_optional_type,
    is_typeddict,
    is_union_type,
    is_annotated,
)


class TTestTyDi(TypedDict):
    field: str


TTestLi = Literal["a", "b", "c"]
TOptionalLi = Optional[TTestLi]
TOptionalTyDi = Optional[TTestTyDi]

TOptionalUnionLiTyDi = Optional[Union[TTestTyDi, TTestLi]]


def test_is_typeddict() -> None:
    assert is_typeddict(TTestTyDi) is True
    assert is_typeddict(is_typeddict) is False  # type: ignore[arg-type]
    assert is_typeddict(Sequence[str]) is False


def test_is_list_generic_type() -> None:
    # yes - we need a generic type
    assert is_list_generic_type(list) is False
    assert is_list_generic_type(List[str]) is True
    assert is_list_generic_type(Sequence[str]) is True
    assert is_list_generic_type(MutableSequence[str]) is True
    assert is_list_generic_type(TOptionalUnionLiTyDi) is False  # type: ignore[arg-type]


def test_is_dict_generic_type() -> None:
    assert is_dict_generic_type(dict) is False
    assert is_dict_generic_type(Mapping[str, str]) is True
    assert is_dict_generic_type(MutableMapping[str, str]) is True


def test_is_literal() -> None:
    assert is_literal_type(TTestLi) is True  # type: ignore[arg-type]
    assert is_literal_type(Final[TTestLi]) is True  # type: ignore[arg-type]
    assert is_literal_type("a") is False  # type: ignore[arg-type]
    assert is_literal_type(List[str]) is False


def test_optional() -> None:
    assert is_optional_type(TOptionalLi) is True  # type: ignore[arg-type]
    assert is_optional_type(ClassVar[TOptionalLi]) is True  # type: ignore[arg-type]
    assert is_optional_type(TOptionalTyDi) is True  # type: ignore[arg-type]
    assert is_optional_type(TTestTyDi) is False
    assert extract_union_types(TOptionalLi) == [TTestLi, type(None)]  # type: ignore[arg-type]
    assert extract_union_types(TOptionalTyDi) == [TTestTyDi, type(None)]  # type: ignore[arg-type]


def test_union_types() -> None:
    assert is_optional_type(TOptionalLi) is True  # type: ignore[arg-type]
    assert is_optional_type(TOptionalTyDi) is True  # type: ignore[arg-type]
    assert is_optional_type(TTestTyDi) is False
    assert extract_union_types(TOptionalLi) == [TTestLi, type(None)]  # type: ignore[arg-type]
    assert extract_union_types(TOptionalTyDi) == [TTestTyDi, type(None)]  # type: ignore[arg-type]
    assert is_optional_type(TOptionalUnionLiTyDi) is True  # type: ignore[arg-type]
    assert extract_union_types(TOptionalUnionLiTyDi) == [TTestTyDi, TTestLi, type(None)]  # type: ignore[arg-type]
    assert is_union_type(MutableSequence[str]) is False


def test_is_newtype() -> None:
    NT1 = NewType("NT1", str)
    assert is_newtype_type(NT1) is True
    assert is_newtype_type(ClassVar[NT1]) is True  # type: ignore[arg-type]
    assert is_newtype_type(TypeVar("TV1", bound=str)) is False  # type: ignore[arg-type]
    assert is_newtype_type(1) is False  # type: ignore[arg-type]


def test_is_annotated() -> None:
    TA = Annotated[str, "PII", "name"]
    assert is_annotated(TA) is True
    a_t, *a_m = get_args(TA)
    assert a_t is str
    assert a_m == ["PII", "name"]


def test_extract_inner_type() -> None:
    assert extract_inner_type(1) == 1  # type: ignore[arg-type]
    assert extract_inner_type(str) is str
    assert extract_inner_type(NewType("NT1", str)) is str
    assert extract_inner_type(NewType("NT2", NewType("NT3", int))) is int
    assert extract_inner_type(Optional[NewType("NT3", bool)]) is bool  # type: ignore[arg-type]  # noqa: F821
    l_1 = Literal[1, 2, 3]
    assert extract_inner_type(l_1) is int  # type: ignore[arg-type]
    NTL2 = NewType("NTL2", float)
    assert extract_inner_type(NTL2, preserve_new_types=True) is NTL2
    l_2 = Literal[NTL2(1.238), NTL2(2.343)]  # type: ignore[valid-type]
    assert extract_inner_type(l_2) is float  # type: ignore[arg-type]


def test_get_config_if_union() -> None:
    assert get_config_if_union_hint(str) is None
    assert get_config_if_union_hint(Optional[str]) is None  # type: ignore[arg-type]
    assert get_config_if_union_hint(Union[BaseException, str, StrAny]) is None  # type: ignore[arg-type]
    assert get_config_if_union_hint(Union[BaseConfiguration, str, StrAny]) is BaseConfiguration  # type: ignore[arg-type]
    assert get_config_if_union_hint(Union[str, BaseConfiguration, StrAny]) is BaseConfiguration  # type: ignore[arg-type]
    assert (
        get_config_if_union_hint(
            Union[GcpServiceAccountCredentialsWithoutDefaults, StrAny, str]  # type: ignore[arg-type]
        )
        is GcpServiceAccountCredentialsWithoutDefaults
    )
