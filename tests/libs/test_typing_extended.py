from dataclasses import dataclass
from typing import (
    Final,
    Literal,
    TypedDict,
    Optional,
    Union,
)
from typing_extensions import Annotated
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
