import inspect
import time

from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterator,
    Mapping,
    Optional,
    Union,
    Dict,
    cast,
)

from dlt.common.data_writers.writers import count_rows_in_items
from dlt.common.typing import TAny, TDataItem, TDataItems, TypeVar

from dlt.extract.utils import (
    wrap_iterator,
)

from dlt.extract.items import SupportsPipe


ItemTransformFunctionWithMeta = Callable[[TDataItem, str], TAny]
ItemTransformFunctionNoMeta = Callable[[TDataItem], TAny]
ItemTransformFunc = Union[ItemTransformFunctionWithMeta[TAny], ItemTransformFunctionNoMeta[TAny]]

MetricsFunctionWithMeta = Callable[[TDataItems, Any, Dict[str, Any]], None]

TCustomMetrics = TypeVar(
    "TCustomMetrics", covariant=True, bound=Mapping[str, Any], default=Dict[str, Any]
)


class BaseItemTransform(Generic[TCustomMetrics]):
    def __init__(self) -> None:
        self._custom_metrics: TCustomMetrics = cast(TCustomMetrics, {})

    @property
    def custom_metrics(self) -> TCustomMetrics:
        return self._custom_metrics


class ItemTransform(BaseItemTransform[TCustomMetrics], ABC, Generic[TAny, TCustomMetrics]):
    _f_meta: ItemTransformFunctionWithMeta[TAny] = None
    _f: ItemTransformFunctionNoMeta[TAny] = None

    placement_affinity: ClassVar[float] = 0
    """Tell how strongly an item sticks to start (-1) or end (+1) of pipe."""

    def __init__(self, transform_f: ItemTransformFunc[TAny]) -> None:
        super().__init__()
        # inspect the signature
        sig = inspect.signature(transform_f)
        # TODO: use TypeGuard here to get rid of type ignore
        if len(sig.parameters) == 1:
            self._f = transform_f  # type: ignore
        else:  # TODO: do better check
            self._f_meta = transform_f  # type: ignore

    def bind(
        self: "ItemTransform[TAny, TCustomMetrics]", pipe: SupportsPipe
    ) -> "ItemTransform[TAny, TCustomMetrics]":
        return self

    @abstractmethod
    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        """Transforms `item` (a list of TDataItem or a single TDataItem) and returns or yields TDataItems. Returns None to consume item (filter out)"""
        pass


class FilterItem(ItemTransform[bool, Dict[str, Any]]):
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


class MapItem(ItemTransform[TDataItem, Dict[str, Any]]):
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


class YieldMapItem(ItemTransform[Iterator[TDataItem], Dict[str, Any]]):
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


class ValidateItem(ItemTransform[TDataItem, Dict[str, Any]]):
    """Base class for validators of data items.

    Subclass should implement the `__call__` method to either return the data item(s) or raise `extract.exceptions.ValidationError`.
    See `PydanticValidator` for possible implementation.
    """

    placement_affinity: ClassVar[float] = 0.9  # stick to end but less than incremental

    table_name: str

    def bind(self, pipe: SupportsPipe) -> ItemTransform[TDataItem, Dict[str, Any]]:
        self.table_name = pipe.name
        return self


class LimitItem(ItemTransform[TDataItem, Dict[str, Any]]):
    placement_affinity: ClassVar[float] = 1.1  # stick to end right behind incremental

    def __init__(
        self, max_items: Optional[int], max_time: Optional[float], count_rows: bool
    ) -> None:
        BaseItemTransform.__init__(self)
        self.max_items = max_items if max_items is not None else -1
        self.max_time = max_time
        self.count_rows = count_rows

    def bind(self, pipe: SupportsPipe) -> "LimitItem":
        # we also wrap iterators to make them stoppable
        if isinstance(pipe.gen, Iterator):
            pipe.replace_gen(wrap_iterator(pipe.gen))

        self.gen = pipe.gen
        self.count = 0
        self.exhausted = False
        self.start_time = time.time()

        return self

    def limit(self, chunk_size: int) -> Optional[int]:
        """Calculate the maximum number of rows to which result is limited. Limit works in chunks
        that controlled by the data source and this must be provided in `chunk_size`.
        `chunk_size` will be ignore if counting rows (`count_rows` is `True`). Mind that
        this count method will not split batches so you may get more items (up to the full last batch)
        than `limit` method indicates.
        """
        if self.max_items in (None, -1):
            return None
        return self.max_items * (1 if self.count_rows else chunk_size)

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        # do not count None
        if item is None:
            return None

        # do not return any late arriving items
        if self.exhausted:
            return None

        if self.count_rows:
            self.count += count_rows_in_items(item)
        else:
            # NOTE: we count empty batches/pages in this mode
            self.count += 1

        # detect when the limit is reached, max time or yield count
        if (
            (self.count >= self.max_items and self.max_items >= 0)
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
            return None

        return item


class MetricsItem(ItemTransform[None, Dict[str, Any]]):
    """Collects custom metrics from data flowing through the pipe without modifying items.

    The metrics function receives items, optionally meta, and a metrics dictionary that it can
    update in-place. The items are passed through unchanged.
    """

    _metrics_f: MetricsFunctionWithMeta = None

    def __init__(self, metrics_f: MetricsFunctionWithMeta) -> None:
        BaseItemTransform.__init__(self)
        self._metrics_f = metrics_f

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        self._metrics_f(item, meta, self._custom_metrics)
        return item
