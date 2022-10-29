from typing import Callable, TypedDict, TypeVar, Union, List, Awaitable

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
    description: TTableHintTemplate[str]
    write_disposition: TTableHintTemplate[TWriteDisposition]
    # table_sealed: Optional[bool]
    parent: TTableHintTemplate[str]
    columns: TTableHintTemplate[TTableSchemaColumns]
