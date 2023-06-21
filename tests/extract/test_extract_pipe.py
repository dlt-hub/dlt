import os
import asyncio
import inspect
from typing import List, Sequence
import time

import pytest

import dlt
from dlt.common import sleep
from dlt.common.typing import TDataItems
from dlt.extract.exceptions import CreatePipeException, ResourceExtractionError
from dlt.extract.typing import DataItemWithMeta, FilterItem, MapItem, YieldMapItem
from dlt.extract.pipe import ManagedPipeIterator, Pipe, PipeItem, PipeIterator

# from tests.utils import preserve_environ


def test_next_item_mode() -> None:

    def nested_gen_level_2():
        yield from [88, None, 89]

    def nested_gen():
        yield from [55, 56, None, 77, nested_gen_level_2()]

    def source_gen1():
        yield from [1, 2, nested_gen(), 3,4]

    def source_gen2():
        yield from range(11, 16)

    def source_gen3():
        yield from range(20,22)

    def get_pipes():
        return [
            Pipe.from_data("data1", source_gen1()),
            Pipe.from_data("data2", source_gen2()),
            Pipe.from_data("data3", source_gen3()),
            ]

    # default mode is "fifo"
    _l = list(PipeIterator.from_pipes(get_pipes(), next_item_mode="fifo"))
    # items will be in order of the pipes, nested iterator items appear inline
    assert [pi.item for pi in _l] ==  [1, 2, 55, 56, 77, 88, 89,  3, 4, 11, 12, 13, 14, 15, 20, 21]

    # round robin mode
    _l = list(PipeIterator.from_pipes(get_pipes(), next_item_mode="round_robin"))
    # items will be round robin, nested iterators are fully iterated and appear inline as soon as they are encountered
    assert [pi.item for pi in _l] == [1, 11, 20, 2, 12, 21, 55, 56, 77, 88, 89, 13, 3, 14, 4, 15]


def test_rotation_on_none() -> None:

    global started
    started = time.time()

    def source_gen1():
        yield None
        while time.time() - started < 0.5:
            time.sleep(0.1)
            yield None
        yield 1

    def source_gen2():
        yield None
        while time.time() - started < 0.3:
            time.sleep(0.1)
            yield None
        yield 2

    def source_gen3():
        yield None
        while time.time() - started < 0.4:
            time.sleep(0.1)
            yield None
        yield 3

    def get_pipes():
        return [
            Pipe.from_data("data1", source_gen1()),
            Pipe.from_data("data2", source_gen2()),
            Pipe.from_data("data3", source_gen3()),
            ]

    # round robin mode
    _l = list(PipeIterator.from_pipes(get_pipes(), next_item_mode="round_robin"))
    # items will be round robin, nested iterators are fully iterated and appear inline as soon as they are encountered
    assert [pi.item for pi in _l] == [2, 3, 1]
    # jobs should have been executed in parallel
    assert started-time.time() < 0.6





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

    p.append_step(item_step)
    p.append_step(item_meta_step)
    assert p.gen is data_iter
    assert p._gen_idx == 0
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


def test_insert_remove_step() -> None:
    data = [1, 2, 3]
    # data_iter = iter(data)
    pp = Pipe.from_data("data", data)

    def tx(item):
        yield item*2

    # create pipe with transformer
    p = Pipe.from_data("tx", tx, parent=pp)

    # try to remove gen
    with pytest.raises(CreatePipeException):
        pp.remove_step(0)
    with pytest.raises(CreatePipeException):
        p.remove_step(0)

    # try to insert before pp gen (resource cannot have any transform before data is in)
    with pytest.raises(CreatePipeException):
        pp.insert_step(tx, 0)
    # but transformer can
    p.insert_step(tx, 0)
    # gen idx moved
    assert p._gen_idx == 1

    # get data: there are two tx that mul by 2
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [4, 8, 12]

    def pp_item_step(item):
        assert item in data
        return item * 0.5

    # add pp step to pp after gen
    pp.insert_step(pp_item_step, 1)
    assert pp._gen_idx == 0
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [2, 4, 6]

    # add item with meta
    def item_meta_step(item, meta):
        assert meta is None
        return item * 0.5

    p.insert_step(item_meta_step, 2)
    assert p._gen_idx == 1

    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [1, 2, 3]

    # can't remove gen
    with pytest.raises(CreatePipeException):
        p.remove_step(1)

    # can remove tx at 0
    p.remove_step(0)
    assert p._gen_idx == 0
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [0.5, 1, 3/2]
    # remove all remaining txs
    p.remove_step(1)
    pp.remove_step(1)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [2, 4, 6]

    # replaces gen
    pp.replace_gen([-1, -2, -3])
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [-2, -4, -6]

    # def tx_meta_minus(item, meta):
    #     assert meta is None
    #     yield item*-2

    # p.replace_gen(tx_meta_minus)
    # _l = list(PipeIterator.from_pipe(p))
    # assert [pi.item for pi in _l] == [2, 4, 6]

    def tx_minus(item, meta):
        assert meta is None
        yield item*-4

    p.replace_gen(tx_minus)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item for pi in _l] == [4, 8, 12]


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

    p.append_step(item_meta_step)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # pass meta through transformer
    p = Pipe.from_data("data", iter(meta_data))
    p.append_step(item_meta_step)

    # does not take meta
    def transformer(item):
        yield item*item

    def item_meta_step_trans(item: int, meta):
        # reverse all transformations on item
        meta_idx = int(item**0.5//2)
        assert _meta[meta_idx-1] == meta
        return item*2

    t = Pipe("tran", [transformer], parent=p)
    t.append_step(item_meta_step_trans)
    _l = list(PipeIterator.from_pipe(t))
    # item got propagated through transformation -> transformer -> transformation
    assert [int((pi.item//2)**0.5//2) for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # same but with the fork step
    p = Pipe.from_data("data", iter(meta_data))
    p.append_step(item_meta_step)
    t = Pipe("tran", [transformer], parent=p)
    t.append_step(item_meta_step_trans)
    # do not yield parents
    _l = list(PipeIterator.from_pipes([p, t], yield_parents=False))
    # same result
    assert [int((pi.item//2)**0.5//2) for pi in _l] == data
    assert [pi.meta for pi in _l] == _meta

    # same but yield parents
    p = Pipe.from_data("data", iter(meta_data))
    p.append_step(item_meta_step)
    t = Pipe("tran", [transformer], parent=p)
    t.append_step(item_meta_step_trans)
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

    p.append_step(item_meta_step)
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
    p.append_step(item_meta_step_defer)
    _l = list(PipeIterator.from_pipe(p))
    assert [pi.item / 2 for pi in _l] == data
    assert [pi.meta for pi in _l] == ["X1", "X2", "X3"]

    # also works for yielding transformations
    def item_meta_step_flat(item: int, meta):
        assert _meta[item-1] == meta
        # return meta, it should overwrite existing one
        yield DataItemWithMeta("X" + str(item), item*2)

    p = Pipe.from_data("data", iter(meta_data))
    p.append_step(item_meta_step_flat)
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
    p.append_step(item_meta_step_async)
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
    p.append_step(FilterItem(lambda item, _: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, 4]
    # also should work on the list which if fully filtered must become None
    p = Pipe.from_data("data", [[1, 3], 2, [3, 4]])
    p.append_step(FilterItem(lambda item, _: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, [4]]
    # also should filter based on meta
    data = [1, 2, 3]
    meta = [True, True, False]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(meta, data)]
    p = Pipe.from_data("data", meta_data)
    p.append_step(FilterItem(lambda _, meta: bool(meta)))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [1, 2]

    # try the lambda that takes only item (no meta)
    p = Pipe.from_data("data", [1, 2, 3, 4])
    p.append_step(FilterItem(lambda item: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, 4]
    # also should work on the list which if fully filtered must become None
    p = Pipe.from_data("data", [[1, 3], 2, [3, 4]])
    p.append_step(FilterItem(lambda item: item % 2 == 0))
    assert _f_items(list(PipeIterator.from_pipe(p))) == [2, [4]]


def test_map_step() -> None:
    p = Pipe.from_data("data", ["A", "B", "C"])
    # doubles all letters
    p.append_step(MapItem(lambda item, _: item * 2))
    assert _f_items(list(PipeIterator.from_pipe(p))) == ["AA", "BB", "CC"]
    # lists and items
    p = Pipe.from_data("data", ["A", ["B", "C"]])
    # doubles all letters
    p.append_step(MapItem(lambda item: item * 2))
    assert _f_items(list(PipeIterator.from_pipe(p))) == ["AA", ["BB", "CC"]]
    # pass meta
    data = ["A", "B", "C"]
    meta = [1, 2, 3]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(meta, data)]
    p = Pipe.from_data("data", meta_data)
    p.append_step(MapItem(lambda item, meta: item * meta))
    assert _f_items(list(PipeIterator.from_pipe(p))) == ["A", "BB", "CCC"]


def test_yield_map_step() -> None:
    p = Pipe.from_data("data", [1, 2, 3])
    # this creates number of rows as passed by the data
    p.append_step(YieldMapItem(lambda item: (yield from [f"item_{x}" for x in range(item)])))
    assert _f_items(list(PipeIterator.from_pipe(p))) == ["item_0", "item_0", "item_1", "item_0", "item_1", "item_2"]
    data = [1, 2, 3]
    meta = ["A", "B", "C"]
    # package items into meta wrapper
    meta_data = [DataItemWithMeta(m, d) for m, d in zip(meta, data)]
    p = Pipe.from_data("data", meta_data)
    p.append_step(YieldMapItem(lambda item, meta: (yield from [f"item_{meta}_{x}" for x in range(item)])))
    assert _f_items(list(PipeIterator.from_pipe(p))) == ["item_A_0", "item_B_0", "item_B_1", "item_C_0", "item_C_1", "item_C_2"]


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


def test_clone_pipes() -> None:

    def pass_gen(item, meta):
        yield item*2

    data = [1, 2, 3]
    p1 = Pipe("p1", [data])
    p2 = Pipe("p2", [data])
    p1_p3 = Pipe("p1_p3", [pass_gen], parent=p1)
    p1_p4 = Pipe("p1_p4", [pass_gen], parent=p1)
    p2_p5 = Pipe("p2_p5", [pass_gen], parent=p2)
    p5_p6 = Pipe("p5_p6", [pass_gen], parent=p2_p5)

    # pass all pipes explicitly
    pipes = [p1, p2, p1_p3, p1_p4, p2_p5, p5_p6]
    cloned_pipes = PipeIterator.clone_pipes(pipes)
    assert_cloned_pipes(pipes, cloned_pipes)

    # clone only two top end pipes, still all parents must be cloned as well
    pipes = [p1_p4, p5_p6]
    cloned_pipes = PipeIterator.clone_pipes(pipes)
    assert_cloned_pipes(pipes, cloned_pipes)
    c_p5_p6 = cloned_pipes[-1]
    assert c_p5_p6.parent.parent is not p2
    assert c_p5_p6.parent.parent._pipe_id == p2._pipe_id

    # try circular deps



def assert_cloned_pipes(pipes: List[Pipe], cloned_pipes: List[Pipe]):
    # clones pipes must be separate instances but must preserve pipe id and names
    for pipe, cloned_pipe in zip(pipes, cloned_pipes):
        while True:
            assert pipe is not cloned_pipe
            assert pipe.name == cloned_pipe.name
            assert pipe._pipe_id == cloned_pipe._pipe_id
            assert pipe.has_parent == cloned_pipe.has_parent

            # check all the parents
            if not pipe.has_parent:
                break
            pipe = pipe.parent
            cloned_pipe = cloned_pipe.parent

    # must yield same data
    for pipe, cloned_pipe in zip(pipes, cloned_pipes):
        assert _f_items(list(PipeIterator.from_pipe(pipe))) == _f_items(list(PipeIterator.from_pipe(cloned_pipe)))


def test_circular_deps() -> None:

    def pass_gen(item, meta):
        yield item*2

    c_p1_p3 = Pipe("c_p1_p3", [pass_gen])
    c_p1_p4 = Pipe("c_p1_p4", [pass_gen], parent=c_p1_p3)
    c_p1_p3.parent = c_p1_p4
    pipes = [c_p1_p3, c_p1_p4]

    # can be cloned
    cloned_pipes = PipeIterator.clone_pipes(pipes)

    # cannot be evaluated
    with pytest.raises(RecursionError):
        _f_items(list(PipeIterator.from_pipe(pipes[-1])))
    with pytest.raises(RecursionError):
        _f_items(list(PipeIterator.from_pipe(cloned_pipes[-1])))
    with pytest.raises(RecursionError):
        _f_items(list(PipeIterator.from_pipes(pipes)))


close_pipe_got_exit = False
close_pipe_yielding = False


def test_close_on_async_exception() -> None:
    def long_gen():
        global close_pipe_got_exit, close_pipe_yielding

        async def _next_item(p: int) -> int:
            return p

        # will be closed by PipeIterator
        try:
            close_pipe_yielding = True
            for i in range(0, 10000):
                yield _next_item(i)
            close_pipe_yielding = False
        except GeneratorExit:
            close_pipe_got_exit = True

    # execute in a thread
    async def raise_gen(item: int):
        if item == 10:
            raise RuntimeError("we fail")
        return item

    assert_pipes_closed(raise_gen, long_gen)


def test_close_on_thread_pool_exception() -> None:
    def long_gen():
        global close_pipe_got_exit, close_pipe_yielding

        @dlt.defer
        def _next_item(p: int) -> int:
            return p

        # will be closed by PipeIterator
        try:
            close_pipe_yielding = True
            for i in range(0, 10000):
                yield _next_item(i)
            close_pipe_yielding = False
        except GeneratorExit:
            close_pipe_got_exit = True

    # execute in a thread
    @dlt.defer
    def raise_gen(item: int):
        if item == 10:
            raise RuntimeError("we fail")
        return item

    assert_pipes_closed(raise_gen, long_gen)


def test_close_on_sync_exception() -> None:

    def long_gen():
        global close_pipe_got_exit, close_pipe_yielding

        # will be closed by PipeIterator
        try:
            close_pipe_yielding = True
            yield from range(0, 10000)
            close_pipe_yielding = False
        except GeneratorExit:
            close_pipe_got_exit = True

    def raise_gen(item: int):
        if item == 10:
            raise RuntimeError("we fail")
        yield item

    assert_pipes_closed(raise_gen, long_gen)


def assert_pipes_closed(raise_gen, long_gen) -> None:
    global close_pipe_got_exit, close_pipe_yielding

    close_pipe_got_exit = False
    close_pipe_yielding = False

    pit: PipeIterator = None
    with PipeIterator.from_pipe(Pipe.from_data("failing", raise_gen, parent=Pipe.from_data("endless", long_gen()))) as pit:
        with pytest.raises(ResourceExtractionError) as py_ex:
            list(pit)
        assert isinstance(py_ex.value.__cause__, RuntimeError)
    # it got closed
    assert pit._sources == []
    assert close_pipe_got_exit is True
    # while long gen was still yielding
    assert close_pipe_yielding is True

    close_pipe_got_exit = False
    close_pipe_yielding = False
    pit = ManagedPipeIterator.from_pipe(Pipe.from_data("failing", raise_gen, parent=Pipe.from_data("endless", long_gen())))
    with pytest.raises(ResourceExtractionError) as py_ex:
        list(pit)
    assert isinstance(py_ex.value.__cause__, RuntimeError)
    assert pit._sources == []
    assert close_pipe_got_exit is True
    # while long gen was still yielding
    assert close_pipe_yielding is True


def _f_items(pipe_items: Sequence[PipeItem]) -> List[TDataItems]:
    return list(map(lambda item: item.item, pipe_items))
