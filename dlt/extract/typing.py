from typing import Any, Callable, TypedDict, TypeVar, Union, Awaitable

from dlt.common.typing import TDataItem, TDataItems
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDisposition


TDeferredDataItems = Callable[[], TDataItems]
TAwaitableDataItems = Awaitable[TDataItems]
TPipedDataItems = Union[TDataItems, TDeferredDataItems, TAwaitableDataItems]

TDynHintType = TypeVar("TDynHintType")
TFunHintTemplate = Callable[[TDataItem], TDynHintType]
TTableHintTemplate = Union[TDynHintType, TFunHintTemplate[TDynHintType]]


class TTableSchemaTemplate(TypedDict, total=False):
    name: TTableHintTemplate[str]
    # description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDisposition]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]


class DataItemWithMeta:
    __slots__ = "meta", "data"

    meta: Any
    data: TDataItems

    def __init__(self, meta: Any, data: TDataItems) -> None:
        self.meta = meta
        self.data = data


class TableNameMeta:
    __slots__ = "table_name"

    table_name: str

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name


# define basic transformation functions
# class FilterItemFunctionWithMeta(Protocol):
#     def __call__(self, item: TDataItem, meta: Any = ...) -> bool:
#         ...

FilterItemFunctionWithMeta = Callable[[TDataItem, str], bool]
FilterItemFunction = Callable[[TDataItem], bool]
