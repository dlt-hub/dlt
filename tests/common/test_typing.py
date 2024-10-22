import pytest
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
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
from uuid import UUID


from dlt import TSecretValue
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    get_config_if_union_hint,
)
from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.typing import (
    StrAny,
    TSecretStrValue,
    extract_inner_type,
    extract_union_types,
    get_all_types_of_class_in_union,
    is_dict_generic_type,
    is_list_generic_type,
    is_literal_type,
    is_newtype_type,
    is_optional_type,
    is_subclass,
    is_typeddict,
    is_union_type,
    is_annotated,
    is_callable_type,
)


class TTestTyDi(TypedDict):
    field: str


@dataclass
class UuidVersion:
    uuid_version: Literal[1, 3, 4, 5]

    def __hash__(self) -> int:
        return self.uuid_version


UUID4 = Annotated[UUID, UuidVersion(4)]


@dataclass
class MyDataclass:
    booba_tooba: str


TTestLi = Literal["a", "b", "c"]
TOptionalLi = Optional[TTestLi]
TOptionalTyDi = Optional[TTestTyDi]

TOptionalUnionLiTyDi = Optional[Union[TTestTyDi, TTestLi]]


def test_is_callable_type() -> None:
    class AsyncClass:
        async def __call__(self):
            pass

    class NotAsyncClass:
        def __call__(self):
            pass

    class ClassNoCall:
        pass

    class ClassBaseCall(NotAsyncClass):
        pass

    async def async_func():
        pass

    def non_async_func():
        pass

    CallableType = Callable[[Any], Any]

    # new type is a callable and will be ignored
    ANewType = NewType("ANewType", str)

    for callable_, t_ in zip(
        [True, True, True, False, True, True, False, False, True, False],
        [
            AsyncClass,
            NotAsyncClass,
            ClassBaseCall,
            ClassNoCall,
            async_func,
            non_async_func,
            Literal[1, 2, 3],
            "A",
            CallableType,
            ANewType,
        ],
    ):
        assert is_callable_type(t_) is callable_  # type: ignore[arg-type]


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
    NT1 = NewType("NT1", Optional[TTestLi])  # type: ignore[valid-newtype]
    assert is_literal_type(NT1) is True
    assert is_literal_type(NewType("NT2", NT1)) is True


def test_optional() -> None:
    assert is_optional_type(TOptionalLi) is True  # type: ignore[arg-type]
    assert is_optional_type(ClassVar[TOptionalLi]) is True  # type: ignore[arg-type]
    assert is_optional_type(Annotated[TOptionalLi, Optional]) is True  # type: ignore[arg-type]
    assert is_optional_type(Annotated[TOptionalLi, "random metadata string"]) is True  # type: ignore[arg-type]
    assert is_optional_type(Optional[Annotated[str, "random metadata string"]]) is True  # type: ignore[arg-type]
    assert is_optional_type(Final[Annotated[Optional[str], "annotated metadata"]]) is True  # type: ignore[arg-type]
    assert is_optional_type(Final[Annotated[Optional[str], None]]) is True  # type: ignore[arg-type]
    assert is_optional_type(Final[Annotated[Union[str, int], None]]) is False  # type: ignore[arg-type]
    assert is_optional_type(Annotated[Union[str, int], type(None)]) is False  # type: ignore[arg-type]
    assert is_optional_type(TOptionalTyDi) is True  # type: ignore[arg-type]
    NT1 = NewType("NT1", Optional[str])  # type: ignore[valid-newtype]
    assert is_optional_type(NT1) is True
    assert is_optional_type(ClassVar[NT1]) is True  # type: ignore[arg-type]
    assert is_optional_type(NewType("NT2", NT1)) is True
    assert is_optional_type(NewType("NT2", Annotated[NT1, 1])) is True
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
    assert is_newtype_type(Optional[NT1]) is True  # type: ignore[arg-type]


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
    assert extract_inner_type(NewType("NT1", Optional[str])) is str


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


def test_extract_annotated_inner_type() -> None:
    assert extract_inner_type(Annotated[TOptionalLi, Optional]) is str  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[TOptionalLi, "random metadata string"]) is str  # type: ignore[arg-type]
    assert extract_inner_type(Optional[Annotated[str, "random metadata string"]]) is str  # type: ignore[arg-type]
    assert extract_inner_type(Final[Annotated[Optional[str], "annotated metadata"]]) is str  # type: ignore[arg-type]
    assert extract_inner_type(Final[Annotated[Optional[str], None]]) is str  # type: ignore[arg-type]
    assert extract_inner_type(Final[Annotated[Union[str, int], None]]) is Union[str, int]  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[Union[str, int], type(None)]) is Union[str, int]  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[Optional[UUID4], "meta"]) is UUID  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[Optional[MyDataclass], "meta"]) is MyDataclass  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[MyDataclass, Optional]) is MyDataclass  # type: ignore[arg-type]
    assert extract_inner_type(Annotated[MyDataclass, "random metadata string"]) is MyDataclass  # type: ignore[arg-type]


def test_is_subclass() -> None:
    from dlt.extract import Incremental

    assert is_subclass(Incremental, BaseConfiguration) is True
    assert is_subclass(Incremental[float], Incremental[int]) is True
    assert is_subclass(BaseConfiguration, Incremental[int]) is False
    assert is_subclass(list, Sequence) is True
    assert is_subclass(list, Sequence[str]) is True
    # unions, new types, literals etc. will always produce False
    assert is_subclass(list, Optional[list]) is False
    assert is_subclass(Optional[list], list) is False
    assert is_subclass(list, TTestLi) is False
    assert is_subclass(TTestLi, TTestLi) is False
    assert is_subclass(list, NewType("LT", list)) is False


def test_get_all_types_of_class_in_union() -> None:
    from dlt.extract import Incremental

    # optional is an union
    assert get_all_types_of_class_in_union(Optional[str], str) == [str]
    # both classes and type aliases are recognized
    assert get_all_types_of_class_in_union(Optional[Incremental], BaseConfiguration) == [
        Incremental
    ]
    assert get_all_types_of_class_in_union(Optional[Incremental[float]], BaseConfiguration) == [
        Incremental[float]
    ]
    # by default superclasses are not recognized
    assert get_all_types_of_class_in_union(Union[BaseConfiguration, str], Incremental[float]) == []
    assert get_all_types_of_class_in_union(
        Union[BaseConfiguration, str], Incremental[float], with_superclass=True
    ) == [BaseConfiguration]


def test_secret_type() -> None:
    # typing must be ok
    val: TSecretValue = 1  # noqa
    val_2: TSecretValue = b"ABC"  # noqa

    # must evaluate to self at runtime
    assert TSecretValue("a") == "a"
    assert TSecretValue(b"a") == b"a"
    assert TSecretValue(7) == 7
    assert isinstance(TSecretValue(7), int)

    # secret str evaluates to str
    val_str: TSecretStrValue = "x"  # noqa
    # here we expect ignore!
    val_str_err: TSecretStrValue = 1  # type: ignore[assignment] # noqa

    assert TSecretStrValue("x_str") == "x_str"
    assert TSecretStrValue({}) == "{}"
