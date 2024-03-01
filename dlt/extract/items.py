from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator, NamedTuple, Union, Optional
from concurrent.futures import Future

from dlt.common.typing import TDataItems
from dlt.extract.typing import TPipedDataItems, DataItemWithMeta, TItemFuture

if TYPE_CHECKING:
    from dlt.extract.pipe import Pipe


class PipeItem(NamedTuple):
    item: TDataItems
    step: int
    pipe: "Pipe"
    meta: Any


class ResolvablePipeItem(NamedTuple):
    # mypy unable to handle recursive types, ResolvablePipeItem should take itself in "item"
    item: Union[TPipedDataItems, Iterator[TPipedDataItems]]
    step: int
    pipe: "Pipe"
    meta: Any


class FuturePipeItem(NamedTuple):
    item: TItemFuture
    step: int
    pipe: "Pipe"
    meta: Any


class SourcePipeItem(NamedTuple):
    item: Union[Iterator[TPipedDataItems], Iterator[ResolvablePipeItem]]
    step: int
    pipe: "Pipe"
    meta: Any
