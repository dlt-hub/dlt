from typing_extensions import Generic, TypedDict

from typing import Any, Callable, List, Literal, Optional, Sequence, TypeVar

from dlt.common.schema.typing import TColumnNames
from dlt.common.typing import TSortOrder
from dlt.extract.items import TTableHintTemplate

TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]
OnCursorValueMissing = Literal["raise", "include", "exclude"]


class IncrementalColumnState(TypedDict):
    initial_value: Optional[Any]
    last_value: Optional[Any]
    unique_hashes: List[str]


class IncrementalArgs(TypedDict, Generic[TCursorValue], total=False):
    cursor_path: str
    initial_value: Optional[TCursorValue]
    last_value_func: Optional[LastValueFunc[TCursorValue]]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    end_value: Optional[TCursorValue]
    row_order: Optional[TSortOrder]
    allow_external_schedulers: Optional[bool]
