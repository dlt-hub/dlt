from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Iterator,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Union,
    Awaitable,
    TYPE_CHECKING,
    NamedTuple,
)
from concurrent.futures import Future

from dlt.common.typing import (
    TAny,
    TDataItem,
    TDataItems,
    TTableHintTemplate,
    TFunHintTemplate,
    TDynHintType,
)

TDecompositionStrategy = Literal["none", "scc"]
TDeferredDataItems = Callable[[], TDataItems]
TAwaitableDataItems = Awaitable[TDataItems]
TPipedDataItems = Union[TDataItems, TDeferredDataItems, TAwaitableDataItems]


if TYPE_CHECKING:
    TItemFuture = Future[TPipedDataItems]
else:
    TItemFuture = Future


class PipeItem(NamedTuple):
    item: TDataItems
    step: int
    pipe: "SupportsPipe"
    meta: Any


class ResolvablePipeItem(NamedTuple):
    # mypy unable to handle recursive types, ResolvablePipeItem should take itself in "item"
    item: Union[TPipedDataItems, Iterator[TPipedDataItems]]
    step: int
    pipe: "SupportsPipe"
    meta: Any


class FuturePipeItem(NamedTuple):
    item: TItemFuture
    step: int
    pipe: "SupportsPipe"
    meta: Any


class SourcePipeItem(NamedTuple):
    item: Union[Iterator[TPipedDataItems], Iterator[ResolvablePipeItem]]
    step: int
    pipe: "SupportsPipe"
    meta: Any


# pipeline step may be iterator of data items or mapping function that returns data item or another iterator
TPipeStep = Union[
    Iterable[TPipedDataItems],
    Iterator[TPipedDataItems],
    # Callable with meta
    Callable[[TDataItems, Optional[Any]], TPipedDataItems],
    Callable[[TDataItems, Optional[Any]], Iterator[TPipedDataItems]],
    Callable[[TDataItems, Optional[Any]], Iterator[ResolvablePipeItem]],
    # Callable without meta
    Callable[[TDataItems], TPipedDataItems],
    Callable[[TDataItems], Iterator[TPipedDataItems]],
    Callable[[TDataItems], Iterator[ResolvablePipeItem]],
]


class DataItemWithMeta:
    __slots__ = ("meta", "data")

    def __init__(self, meta: Any, data: TDataItems) -> None:
        self.meta = meta
        self.data = data


class TableNameMeta:
    __slots__ = ("table_name",)

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name


class SupportsPipe(Protocol):
    """A protocol with the core Pipe properties and operations"""

    name: str
    """Pipe name which is inherited by a resource"""
    parent: "SupportsPipe"
    """A parent of the current pipe"""

    @property
    def gen(self) -> TPipeStep:
        """A data generating step"""
        ...

    def replace_gen(self, gen: TPipeStep) -> None:
        """Replaces data generating step. Assumes that you know what are you doing"""
        ...

    def __getitem__(self, i: int) -> TPipeStep:
        """Get pipe step at index"""
        ...

    def __len__(self) -> int:
        """Length of a pipe"""
        ...

    @property
    def has_parent(self) -> bool:
        """Checks if pipe is connected to parent pipe from which it takes data items. Connected pipes are created from transformer resources"""
        ...

    def close(self) -> None:
        """Closes pipe generator"""
        ...
