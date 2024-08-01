from typing import TypedDict, Optional, Any, List, Literal, TypeVar, Callable, Sequence


TCursorValue = TypeVar("TCursorValue", bound=Any)
LastValueFunc = Callable[[Sequence[TCursorValue]], Any]
OnCursorValueNone = Literal["raise", "include", "exclude"]


class IncrementalColumnState(TypedDict):
    initial_value: Optional[Any]
    last_value: Optional[Any]
    unique_hashes: List[str]
