import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Iterator, Literal, Optional, Protocol, TypeVar, Union, Awaitable

from dlt.common.typing import TAny, TDataItem, TDataItems


TDecompositionStrategy = Literal["none", "scc"]
TDeferredDataItems = Callable[[], TDataItems]
TAwaitableDataItems = Awaitable[TDataItems]
TPipedDataItems = Union[TDataItems, TDeferredDataItems, TAwaitableDataItems]

TDynHintType = TypeVar("TDynHintType")
TFunHintTemplate = Callable[[TDataItem], TDynHintType]
TTableHintTemplate = Union[TDynHintType, TFunHintTemplate[TDynHintType]]


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


class SupportsPipe(Protocol):
    """A protocol with the core Pipe properties and operations"""
    name: str
    """Pipe name which is inherited by a resource"""
    parent: "SupportsPipe"
    """A parent of the current pipe"""
    @property
    def has_parent(self) -> bool:
        """Checks if pipe is connected to parent pipe from which it takes data items. Connected pipes are created from transformer resources"""
        ...


ItemTransformFunctionWithMeta = Callable[[TDataItem, str], TAny]
ItemTransformFunctionNoMeta = Callable[[TDataItem], TAny]
ItemTransformFunc = Union[ItemTransformFunctionWithMeta[TAny], ItemTransformFunctionNoMeta[TAny]]

class ItemTransform(ABC, Generic[TAny]):
    _f_meta: ItemTransformFunctionWithMeta[TAny] = None
    _f: ItemTransformFunctionNoMeta[TAny] = None

    def __init__(self, transform_f: ItemTransformFunc[TAny]) -> None:
        # inspect the signature
        sig = inspect.signature(transform_f)
        # TODO: use TypeGuard here to get rid of type ignore
        if len(sig.parameters) == 1:
            self._f = transform_f  # type: ignore
        else:  # TODO: do better check
            self._f_meta = transform_f  # type: ignore

    def bind(self: "ItemTransform[TAny]", pipe: SupportsPipe) -> "ItemTransform[TAny]":
        return self

    @abstractmethod
    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        """Transforms `item` (a list of TDataItem or a single TDataItem) and returns or yields TDataItems. Returns None to consume item (filter out)"""
        pass


class FilterItem(ItemTransform[bool]):
    # mypy needs those to type correctly
    _f_meta: ItemTransformFunctionWithMeta[bool]
    _f: ItemTransformFunctionNoMeta[bool]

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if isinstance(item, list):
            if self._f_meta:
                item = [i for i in item if self._f_meta(i, meta)]
            else:
                item = [i for i in item if self._f(i)]
            if not item:
                # item was fully consumed by the filter
                return None
            return item
        else:
            if self._f_meta:
                return item if self._f_meta(item, meta) else None
            else:
                return item if self._f(item) else None


class MapItem(ItemTransform[TDataItem]):
    # mypy needs those to type correctly
    _f_meta: ItemTransformFunctionWithMeta[TDataItem]
    _f: ItemTransformFunctionNoMeta[TDataItem]

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if isinstance(item, list):
            if self._f_meta:
                return [self._f_meta(i, meta) for i in item]
            else:
                return [self._f(i) for i in item]
        else:
            if self._f_meta:
                return self._f_meta(item, meta)
            else:
                return self._f(item)


class YieldMapItem(ItemTransform[Iterator[TDataItem]]):
     # mypy needs those to type correctly
    _f_meta: ItemTransformFunctionWithMeta[TDataItem]
    _f: ItemTransformFunctionNoMeta[TDataItem]

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        if isinstance(item, list):
            for i in item:
                if self._f_meta:
                    yield from self._f_meta(i, meta)
                else:
                    yield from self._f(i)
        else:
            if self._f_meta:
                yield from self._f_meta(item, meta)
            else:
                yield from self._f(item)


class ValidateItem(ItemTransform[TDataItem]):
    """Base class for validators of data items.

    Subclass should implement the `__call__` method to either return the data item(s) or raise `extract.exceptions.ValidationError`.
    See `PydanticValidator` for possible implementation.
    """
