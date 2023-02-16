import os
import asyncio
import inspect
from typing import List, Sequence

import pytest

import dlt
from dlt.common import sleep
from dlt.common.typing import TDataItems
from dlt.extract.typing import DataItemWithMeta
from dlt.extract.pipe import FilterItem, Pipe, PipeItem, PipeIterator

# from tests.utils import preserve_environ


def test_add_step() -> None:
    data = [1, 2, 3]
    data_iter = iter(data)
    p = Pipe.from_data("data", data_iter)

    def item_step(item):
        assert item in data
        return item

    def item_meta_step(item, meta):
        assert item in data
        assert meta is None
        return item

    p.add_step(item_step)
    p.add_step(item_meta_step)
    assert p.head is data_iter
    assert p.tail is item_meta_step
    assert p.tail(3, None) == 3
    # the middle step should be wrapped
    mid = p.steps[1]
    assert mid is not item_step
    sig = inspect.signature(mid)

    # includes meta
    assert len(sig.parameters) == 2
    # meta is ignored
    assert mid(2) == 2
    assert mid(2, meta="META>") == 2

    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == data


def test_pipe_propagate_meta() -> None:
    data = [1, 2, 3]
    _meta = ["M1", {"A": 1}, [1, 2, 3]]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(_meta, data)]
    p = Pipe.from_data("data", iter(meta_data))
    _l = list(PipeIterator.from_pipe(p))
    # check items
    assert [pi.item for pi in _l] == data
    # assert meta
    assert [pi.meta for pi in _l] == _meta

    # pass meta through mapping functions
    p = Pipe.from_data("data", iter(meta_data))

    def item_meta_step(item: int, meta):
        assert _meta[item-1] == meta
        return item*2

    p.add_step(item_meta_step)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # pass meta through transformer
    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step)

    # does not take meta
    def transformer(item):
        yield item*item

    def item_meta_step_trans(item: int, meta):
        # reverse all transformations on item
        meta_idx = int(item**0.5//2)
        assert _meta[meta_idx-1] == meta
        return item*2

    t = Pipe("tran", [transformer], parent=p)
    t.add_step(item_meta_step_trans)
    _l = list(PipeIterator.from_pipe(t))
    # item got propagated through transformation -> transformer -> transformation
    assert [int((pi.item//2)**0.5//2) for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # same but with the fork step
    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step)
    t = Pipe("tran", [transformer], parent=p)
    t.add_step(item_meta_step_trans)
    # do not yield parents
    _l = list(PipeIterator.from_pipes([p, t], yield_parents=False))
    # same result
    assert [int((pi.item//2)**0.5//2) for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # same but yield parents
    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step)
    t = Pipe("tran", [transformer], parent=p)
    t.add_step(item_meta_step_trans)
    _l = list(PipeIterator.from_pipes([p, t], yield_parents=True))
    # same result for transformer
    tran_l = [pi for pi in _l if pi.pipe._pipe_id == t._pipe_id]
    assert [int((pi.item//2)**0.5//2) for pi in tran_l] == data
    assert [pi.meta for pi in tran_l] == _meta
    data_l = [pi for pi in _l if pi.pipe._pipe_id == p._pipe_id]
    # data pipe went only through one transformation
    assert [int(pi.item//2) for pi in data_l] == data
    assert [pi.meta for pi in data_l] == _meta


def test_pipe_transformation_changes_meta() -> None:
    data = [1, 2, 3]
    _meta = ["M1", {"A": 1}, [1, 2, 3]]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(_meta, data)]
    p = Pipe.from_data("data", iter(meta_data))

    def item_meta_step(item: int, meta):
        assert _meta[item-1] == meta
        # return meta, it should overwrite existing one
        return DataItemWithMeta("X" + str(item), item*2)

    p.add_step(item_meta_step)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also works for deferred transformations
    @dlt.defer
    def item_meta_step_defer(item: int, meta):
        assert _meta[item-1] == meta
        sleep(item * 0.2)
        # return meta, it should overwrite existing one
        return DataItemWithMeta("X" + str(item), item*2)

    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step_defer)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also works for yielding transformations
    def item_meta_step_flat(item: int, meta):
        assert _meta[item-1] == meta
        # return meta, it should overwrite existing one
        yield DataItemWithMeta("X" + str(item), item*2)

    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step_flat)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also works for async
    async def item_meta_step_async(item: int, meta):
        assert _meta[item-1] == meta
        await asyncio.sleep(item * 0.2)
        # this returns awaitable
        return DataItemWithMeta("X" + str(item), item*2)

    p = Pipe.from_data("data", iter(meta_data))
    p.add_step(item_meta_step_async)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also lets the transformer return meta

    def transformer(item: int):
        yield DataItemWithMeta("X" + str(item), item*2)

    p = Pipe.from_data("data", iter(meta_data))
    t = Pipe("tran", [transformer], parent=p)
    _l = list(PipeIterator.from_pipe(t))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also with fork
    p = Pipe.from_data("data", iter(meta_data))
    t = Pipe("tran", [transformer], parent=p)
    _l = list(PipeIterator.from_pipes([p, t], yield_parents=False))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]


def test_pipe_multiple_iterations() -> None:
    # list based pipe should iterate many times
    p = Pipe.from_data("data", [1, 2, 3])
    l1 = list(PipeIterator.from_pipe(p))
    l2 = list(PipeIterator.from_pipe(p))
    l3 = list(PipeIterator.from_pipes([p]))
    l4 = list(PipeIterator.from_pipes([p]))
    assert _f_items(l1) == [1, 2, 3]
    assert _f_items(l1) == _f_items(l2) == _f_items(l3) == _f_items(l4)

    # function based pipes should evaluate many times
    def _gen():
        for i in [1, 2, 3]:
            yield i

    p = Pipe.from_data("data", _gen)
    l1 = list(PipeIterator.from_pipe(p))
    l2 = list(PipeIterator.from_pipe(p))
    l3 = list(PipeIterator.from_pipes([p]))
    l4 = list(PipeIterator.from_pipes([p]))
    assert _f_items(l1) == [1, 2, 3]
    assert _f_items(l1) == _f_items(l2) == _f_items(l3) == _f_items(l4)

    # this pipe will evaluate only once
    p = Pipe.from_data("data", _gen())
    l1 = list(PipeIterator.from_pipe(p))
    l3 = list(PipeIterator.from_pipes([p]))
    assert _f_items(l1) == [1, 2, 3]
    assert l3 == []


def test_filter_step() -> None:
    p = Pipe.from_data("data", [1, 2, 3, 4])
    p.add_step(FilterItem(lambda item, _: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, 4]
    # also should work on the list which if fully filtered must become None
    p = Pipe.from_data("data", [[1, 3], 2, [3, 4]])
    p.add_step(FilterItem(lambda item, _: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, [4]]
    # also should filter based on meta
    data = [1, 2, 3]
    meta = [True, True, False]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(meta, data)]
    p = Pipe.from_data("data", meta_data)
    p.add_step(FilterItem(lambda _, meta: bool(meta)))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [1, 2]

    # try the lambda that takes only item (no meta)
    p = Pipe.from_data("data", [1, 2, 3, 4])
    p.add_step(FilterItem(lambda item: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, 4]
    # also should work on the list which if fully filtered must become None
    p = Pipe.from_data("data", [[1, 3], 2, [3, 4]])
    p.add_step(FilterItem(lambda item: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, [4]]


def test_pipe_copy_on_fork() -> None:
    doc = {"e": 1, "l": 2}
    parent = Pipe.from_data("data", [doc])
    child1 = Pipe("tr1", [lambda x: x], parent=parent)
    child2 = Pipe("tr2", [lambda x: x], parent=parent)

    # no copy, construct iterator
    elems = list(PipeIterator.from_pipes([child1, child2], yield_parents=False, copy_on_fork=False))
    # those are the same instances
    assert doc is elems[0].item is elems[1].item

    # copy item on fork
    elems = list(PipeIterator.from_pipes([child1, child2], yield_parents=False, copy_on_fork=True))
    # first fork does not copy
    assert doc is elems[0].item
    # second fork copies
    assert elems[0].item is not elems[1].item


@pytest.mark.skip("Not implemented")
def test_async_pipe_exception() -> None:
    pass


@pytest.mark.skip("Not implemented")
def test_thread_pipe_exception() -> None:
    pass


@pytest.mark.skip("Not implemented")
def test_sync_pipe_exception() -> None:
    pass


def _f_items(pipe_items: Sequence[PipeItem]) -> List[TDataItems]:
    return list(map(lambda item: item.item, pipe_items))
