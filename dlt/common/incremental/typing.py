from typing import Any, Callable, List, Literal, Optional, Sequence, TypeVar, Union

from dlt.common.typing import TSortOrder, TTableHintTemplate, TColumnNames, TypedDict

TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]
OnCursorValueMissing = Literal["raise", "include", "exclude"]

TIncrementalRange = Literal["open", "closed"]


class IncrementalColumnState(TypedDict):
    initial_value: Optional[Any]
    last_value: Optional[Any]
    unique_hashes: List[str]


class IncrementalArgs(TypedDict, total=False):
    cursor_path: str
    initial_value: Optional[Any]
    last_value_func: Optional[Union[LastValueFunc[str], Literal["min", "max"]]]
    """Last value callable or name of built in function"""
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    end_value: Optional[Any]
    row_order: Optional[TSortOrder]
    allow_external_schedulers: Optional[bool]
    lag: Optional[Union[float, int]]
    on_cursor_value_missing: Optional[OnCursorValueMissing]
    range_start: Optional[TIncrementalRange]
    range_end: Optional[TIncrementalRange]
