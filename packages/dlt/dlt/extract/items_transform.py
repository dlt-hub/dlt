import inspect
import time

from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterator,
    Optional,
    Union,
)
from concurrent.futures import Future

from dlt.common.typing import (
    TAny,
    TDataItem,
    TDataItems,
)

from dlt.extract.utils import (
    wrap_iterator,
)

from dlt.extract.items import SupportsPipe


ItemTransformFunctionWithMeta = Callable[[TDataItem, str], TAny]
ItemTransformFunctionNoMeta = Callable[[TDataItem], TAny]
ItemTransformFunc = Union[ItemTransformFunctionWithMeta[TAny], ItemTransformFunctionNoMeta[TAny]]


class ItemTransform(ABC, Generic[TAny]):
    _f_meta: ItemTransformFunctionWithMeta[TAny] = None
    _f: ItemTransformFunctionNoMeta[TAny] = None

    placement_affinity: ClassVar[float] = 0
    """Tell how strongly an item sticks to start (-1) or end (+1) of pipe."""

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
            # preserve type of empty lists
            if len(item) == 0:
                return item

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
            # preserve type of empty lists
            if len(item) == 0:
                return item

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
            # preserve type of empty lists
            if len(item) == 0:
                yield item

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

    placement_affinity: ClassVar[float] = 0.9  # stick to end but less than incremental

    table_name: str

    def bind(self, pipe: SupportsPipe) -> ItemTransform[TDataItem]:
        self.table_name = pipe.name
        return self


class LimitItem(ItemTransform[TDataItem]):
    placement_affinity: ClassVar[float] = 1.1  # stick to end right behind incremental

    def __init__(self, max_items: Optional[int], max_time: Optional[float]) -> None:
        self.max_items = max_items if max_items is not None else -1
        self.max_time = max_time

    def bind(self, pipe: SupportsPipe) -> "LimitItem":
        # we also wrap iterators to make them stoppable
        if isinstance(pipe.gen, Iterator):
            pipe.replace_gen(wrap_iterator(pipe.gen))

        self.gen = pipe.gen
        self.count = 0
        self.exhausted = False
        self.start_time = time.time()

        return self

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        self.count += 1

        # detect when the limit is reached, max time or yield count
        if (
            (self.count == self.max_items)
            or (self.max_time and time.time() - self.start_time > self.max_time)
            or self.max_items == 0
        ):
            self.exhausted = True
            if inspect.isgenerator(self.gen):
                self.gen.close()

            # if max items is not 0, we return the last item
            # otherwise never return anything
            if self.max_items != 0:
                return item

        # do not return any late arriving items
        if self.exhausted:
            return None

        return item
