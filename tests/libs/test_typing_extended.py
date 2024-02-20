from dataclasses import dataclass
from typing import (
    Final,
    Literal,
    TypedDict,
    Optional,
    Union,
)
from typing_extensions import assert_type, Annotated
from uuid import UUID

from pydantic import UUID4

from dlt.common.typing import (
    extract_inner_type,
)


class TTestTyDi(TypedDict):
    field: str


@dataclass
class MyDataclass:
    booba_tooba: str


TTestLi = Literal["a", "b", "c"]
TOptionalLi = Optional[TTestLi]
TOptionalTyDi = Optional[TTestTyDi]

TOptionalUnionLiTyDi = Optional[Union[TTestTyDi, TTestLi]]


def test_extract_annotated_inner_type() -> None:
    assert_type(extract_inner_type(Annotated[TOptionalLi, Optional]), str)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[TOptionalLi, "random metadata string"]), str)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Optional[Annotated[str, "random metadata string"]]), str)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Final[Annotated[Optional[str], "annotated metadata"]]), str)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Final[Annotated[Optional[str], None]]), str)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Final[Annotated[Union[str, int], None]]), Union[str, int])  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[Union[str, int], type(None)]), Union[str, int])  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[Optional[UUID4], "meta"]), UUID)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[Optional[MyDataclass], "meta"]), MyDataclass)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[MyDataclass, Optional]), MyDataclass)  # type: ignore[arg-type]
    assert_type(extract_inner_type(Annotated[MyDataclass, "random metadata string"]), MyDataclass)  # type: ignore[arg-type]
