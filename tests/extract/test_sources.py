import itertools
from typing import Iterator

import pytest
import asyncio

import dlt, os
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import DictValidationException, PipelineStateNotAvailable
from dlt.common.pipeline import StateInjectableContext, source_state
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnProp, TColumnSchema
from dlt.common.schema import utils
from dlt.common.typing import TDataItems

from dlt.extract import DltResource, DltSource, Incremental
from dlt.extract.items import TableNameMeta
from dlt.extract.source import DltResourceDict
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
    InconsistentTableTemplate,
    InvalidParentResourceDataType,
    InvalidParentResourceIsAFunction,
    InvalidResourceDataTypeMultiplePipes,
    InvalidTransformerDataTypeGeneratorFunctionRequired,
    InvalidTransformerGeneratorFunction,
    ParametrizedResourceUnbound,
    ResourceNotATransformer,
    ResourcesNotFoundError,
)
from dlt.extract.pipe import Pipe


@pytest.fixture(autouse=True)
def switch_to_fifo():
    """most of the following tests rely on the old default fifo next item mode"""
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    yield
    del os.environ["EXTRACT__NEXT_ITEM_MODE"]


def test_basic_source() -> None:
    def basic_gen():
        yield 1

    schema = Schema("test")
    s = DltSource.from_data(schema, "section", basic_gen)
    assert s.name == "test"
    assert s.section == "section"
    assert s.max_table_nesting is None
    assert s.root_key is False
    assert s.schema_contract is None
    assert s.exhausted is False
    assert s.schema is schema
    assert len(s.resources) == 1
    assert s.resources == s.selected_resources

    # set some props
    s.max_table_nesting = 10
    assert s.max_table_nesting == 10
    s.root_key = True
    assert s.root_key is True
    s.schema_contract = "evolve"
    assert s.schema_contract == "evolve"

    s.max_table_nesting = None
    s.root_key = False
    s.schema_contract = None

    assert s.max_table_nesting is None
    assert s.root_key is False
    assert s.schema_contract is None


def test_call_data_resource() -> None:
    with pytest.raises(TypeError):
        DltResource.from_data([1], name="t")()


def test_parametrized_resource() -> None:
    def parametrized(p1, /, p2, *, p3=None):
        assert p1 == "p1"
        assert p2 == 1
        assert p3 is None
        yield 1

    r = DltResource.from_data(parametrized)
    info = str(r)

    # contains parametrized info
    assert "This resource is parametrized and takes the following arguments" in info
    # does not contain iterate info
    assert "If you want to see the data items in the resource you" not in info

    # iterating the source will raise unbound exception
    with pytest.raises(ParametrizedResourceUnbound) as py_ex:
        list(r)
    assert py_ex.value.func_name == "parametrized"
    assert py_ex.value.resource_name == "parametrized"

    r = DltResource.from_data(parametrized)
    # bind
    assert list(r("p1", 1, p3=None)) == [1]
    # take info from bound resource
    info = str(r("p1", 1, p3=None))
    assert "If you want to see the data items in the resource you" in info

    # as part of the source
    r = DltResource.from_data(parametrized)
    s = DltSource(Schema("source"), "module", [r])

    with pytest.raises(ParametrizedResourceUnbound) as py_ex:
        list(s)

    # call the resource
    assert list(s.parametrized("p1", 1, p3=None)) == [1]
    # the resource in the source is still unbound
    with pytest.raises(ParametrizedResourceUnbound) as py_ex:
        list(s)
    # so bind it
    s.parametrized.bind("p1", 1)
    assert list(s) == [1]


def test_parametrized_transformer() -> None:
    def good_transformer(item, /, p1, p2, *, p3=None):
        assert p1 == "p1"
        assert p2 == 2
        assert p3 is None

        for i in range(p2):
            yield {"wrap": item, "mark": p1, "iter": i}

    def bad_transformer():
        yield 2

    def bad_transformer_2(item, p1, /):
        yield 2

    def bad_transformer_3(*, item):
        yield 2

    r = dlt.resource(["itemX", "itemY"], name="items")

    # transformer must be created on a callable with at least one argument
    with pytest.raises(InvalidTransformerDataTypeGeneratorFunctionRequired):
        dlt.transformer(data_from=r)("a")  # type: ignore[arg-type]
    with pytest.raises(InvalidTransformerDataTypeGeneratorFunctionRequired):
        dlt.transformer(data_from=r)(bad_transformer())

    # transformer must take at least one arg
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(data_from=r)(bad_transformer)  # type: ignore[arg-type]
    assert py_ex.value.code == 1
    # transformer may have only one positional argument and it must be first
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(data_from=r)(bad_transformer_2)
    assert py_ex.value.code == 2
    # first argument cannot be kw only
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(data_from=r)(bad_transformer_3)  # type: ignore[arg-type]
    assert py_ex.value.code == 3

    # transformer must take data from a resource
    with pytest.raises(InvalidParentResourceIsAFunction):
        dlt.transformer(data_from=bad_transformer)(good_transformer)
    with pytest.raises(InvalidParentResourceDataType):
        dlt.transformer(data_from=bad_transformer())(good_transformer)

    # transformer is unbound
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(data_from=r)(good_transformer)
    with pytest.raises(ParametrizedResourceUnbound):
        list(t)

    # pass wrong arguments
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(data_from=r)(good_transformer)
    with pytest.raises(TypeError):
        list(t("p1", 1, 2, 3, 4))

    # pass arguments that fully bind the item
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(data_from=r)(good_transformer)
    with pytest.raises(TypeError):
        t(item={}, p1="p2", p2=1)

    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(data_from=r)(good_transformer)
    items = list(t(p1="p1", p2=2))

    def assert_items(_items: TDataItems) -> None:
        # 2 items yielded * p2=2
        assert len(_items) == 2 * 2
        assert _items[0] == {"wrap": "itemX", "mark": "p1", "iter": 0}
        assert _items[3] == {"wrap": "itemY", "mark": "p1", "iter": 1}

    assert_items(items)

    # parameters passed as args
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(data_from=r)(good_transformer)
    items = list(t("p1", 2))
    assert_items(items)


def test_resource_bind_when_in_source() -> None:
    @dlt.resource
    def parametrized(_range: int):
        yield list(range(_range))

    # binding the resource creates new instance
    r1 = parametrized(6)
    r2 = parametrized(7)
    assert r1 is not r2 is not parametrized
    assert r1.source_name is r2.source_name is None

    # add parametrized to source
    @dlt.source
    def test_source():
        return parametrized

    s = test_source()
    # we cloned the instance
    assert s.resources["parametrized"] is not parametrized
    assert s.resources["parametrized"].source_name == "test_source"
    cloned_r = s.resources["parametrized"]
    # calling resources always create a copy
    cr_1 = cloned_r(10)
    # does not have source_name set
    assert cr_1.source_name is None
    # different instance
    assert cloned_r is not cr_1
    # call bind directly to replace resource in place
    s.parametrized.bind(10)
    # will raise
    with pytest.raises(TypeError):
        # already bound
        cloned_r(10)
    assert list(s) == list(range(10))


def test_resource_bind_call_forms() -> None:
    @dlt.resource
    def returns_res(_input):
        # resource returning resource
        return dlt.resource(_input, name="internal_res")

    @dlt.resource
    def returns_pipe(_input):
        # returns pipe
        return Pipe.from_data("internal_pipe", _input)

    @dlt.resource
    def regular(_input):
        yield from _input

    # add filtering lambda
    regular.add_filter(lambda x: x == "A")
    assert len(regular._pipe) == 2
    # binding via call preserves filter (only gen replaced, other steps remain)
    b_regular = regular("ABCA")
    assert len(b_regular._pipe) == 2
    # filter works
    assert list(b_regular) == ["A", "A"]
    # resource is different instance
    assert regular is not b_regular
    assert regular._pipe is not b_regular._pipe

    # pipe is replaced on resource returning resource (new pipe created)
    returns_res.add_filter(lambda x: x == "A")
    assert len(returns_res._pipe) == 2
    b_returns_res = returns_res(["A", "A", "B", "B"])
    assert len(b_returns_res._pipe) == 1
    assert returns_res is not b_returns_res
    assert returns_res._pipe is not b_returns_res._pipe

    # pipe is replaced on resource returning pipe
    returns_pipe.add_filter(lambda x: x == "A")
    assert len(returns_pipe._pipe) == 2
    b_returns_pipe = returns_pipe("ABCA")
    assert len(b_returns_pipe._pipe) == 1

    @dlt.source
    def test_source():
        return returns_res, returns_pipe, regular

    # similar rests within sources
    s = test_source()

    # clone of resource in the source, including pipe
    assert s.regular is not regular
    assert s.regular._pipe is not regular._pipe

    # will repeat each string 3 times
    s.regular.add_map(lambda i: i * 3)
    assert len(regular._pipe) == 2
    assert len(s.regular._pipe) == 3

    # call
    assert list(s.regular(["A", "A", "B", "B"])) == ["AAA", "AAA"]
    # bind
    s.regular.bind([["A"], ["A"], ["B", "A"], ["B", "C"]])
    assert list(s.regular) == ["AAA", "AAA", "AAA"]

    # binding resource that returns resource will replace the object content, keeping the object id
    s.returns_res.add_map(lambda i: i * 3)
    s.returns_res.bind(["X", "Y", "Z"])
    # got rid of all mapping and filter functions
    assert len(s.returns_res._pipe) == 1
    assert list(s.returns_res) == ["X", "Y", "Z"]

    # same for resource returning pipe
    s.returns_pipe.add_map(lambda i: i * 3)
    s.returns_pipe.bind(["X", "Y", "M"])
    # got rid of all mapping and filter functions
    assert len(s.returns_pipe._pipe) == 1
    assert list(s.returns_pipe) == ["X", "Y", "M"]

    # s.regular is exhausted so set it again
    # add lambda that after filtering for A, will multiply it by 4
    s.resources["regular"] = regular.add_map(lambda i: i * 4)(["A", "Y"])
    assert list(s) == ["X", "Y", "Z", "X", "Y", "M", "AAAA"]


def test_call_clone_separate_pipe() -> None:
    all_yields = []

    def some_data_gen(param: str):
        all_yields.append(param)
        yield param

    @dlt.resource
    def some_data(param: str):
        yield from some_data_gen(param)

    # create two resource instances and extract in single ad hoc resource
    data1 = some_data("state1")
    data1._pipe.name = "state1_data"
    dlt.pipeline(dev_mode=True).extract([data1, some_data("state2")], schema=Schema("default"))
    # both should be extracted. what we test here is the combination of binding the resource by calling it that clones the internal pipe
    # and then creating a source with both clones. if we keep same pipe id when cloning on call, a single pipe would be created shared by two resources
    assert all_yields == ["state1", "state2"]


def test_resource_bind_lazy_eval() -> None:
    @dlt.resource
    def needs_param(param):
        yield from range(param)

    @dlt.transformer(data_from=needs_param(3))
    def tx_form(item, multi):
        yield item * multi

    @dlt.transformer(data_from=tx_form(2))
    def tx_form_fin(item, div):
        yield item / div

    @dlt.transformer(data_from=needs_param)
    def tx_form_dir(item, multi):
        yield item * multi

    # tx_form takes data from needs_param(3) which is lazily evaluated
    assert list(tx_form(2)) == [0, 2, 4]
    # so it will not get exhausted
    assert list(tx_form(2)) == [0, 2, 4]

    # same for tx_form_fin
    assert list(tx_form_fin(3)) == [0, 2 / 3, 4 / 3]
    assert list(tx_form_fin(3)) == [0, 2 / 3, 4 / 3]

    # binding `needs_param`` in place will not affect the tx_form and tx_form_fin (they operate on copies)
    needs_param.bind(4)
    assert list(tx_form(2)) == [0, 2, 4]
    # eval needs_param
    assert list(needs_param) == [0, 1, 2, 3]
    assert list(tx_form(2)) == [0, 2, 4]

    # this also works, evaluations always happen on copies
    assert list(tx_form_dir(3)) == [0, 3, 6, 9]


def test_transformer_preliminary_step() -> None:
    def yield_twice(item):
        yield item.upper()
        yield item.upper()

    tx_stage = dlt.transformer()(yield_twice)()
    # filter out small caps and insert this before the head
    tx_stage.add_filter(lambda letter: letter.isupper(), 0)
    # be got filtered out before duplication
    assert list(dlt.resource(["A", "b", "C"], name="data") | tx_stage) == ["A", "A", "C", "C"]

    # filter after duplication
    tx_stage = dlt.transformer()(yield_twice)()
    tx_stage.add_filter(lambda letter: letter.isupper())
    # nothing is filtered out: on duplicate we also capitalize so filter does not trigger
    assert list(dlt.resource(["A", "b", "C"], name="data") | tx_stage) == [
        "A",
        "A",
        "B",
        "B",
        "C",
        "C",
    ]


def test_set_table_name() -> None:
    r = dlt.resource(["A", "b", "C"], name="data")
    assert r.table_name == "data"
    r.table_name = "letters"
    assert r.table_name == "letters"
    r.table_name = lambda letter: letter
    assert callable(r.table_name)


def test_select_resources() -> None:
    @dlt.source
    def test_source(no_resources):
        for i in range(no_resources):
            yield dlt.resource(["A"] * i, name="resource_" + str(i))

    s = test_source(10)
    all_resource_names = ["resource_" + str(i) for i in range(10)]
    assert list(s.resources.keys()) == all_resource_names
    # by default all resources are selected
    assert list(s.selected_resources) == all_resource_names
    assert list(s.resources.selected) == all_resource_names
    # select non existing resource
    with pytest.raises(ResourcesNotFoundError) as py_ex:
        s.with_resources("resource_10", "resource_1", "unknown")
    assert py_ex.value.available_resources == set(all_resource_names)
    assert py_ex.value.not_found_resources == set(("resource_10", "unknown"))
    # make sure the selected list was not changed
    assert list(s.selected_resources) == all_resource_names
    info = str(s)
    assert "resource resource_1 is selected" in info

    # successful select
    s_sel = s.with_resources("resource_1", "resource_7")
    # returns a clone
    assert s is not s_sel
    assert (
        list(s_sel.selected_resources)
        == ["resource_1", "resource_7"]
        == list(s_sel.resources.selected)
    )
    assert list(s_sel.resources) == all_resource_names
    info = str(s_sel)
    assert "resource resource_0 is not selected" in info
    # original is not affected
    assert list(s.selected_resources) == all_resource_names

    # reselect
    assert list(s.with_resources("resource_8").selected_resources) == ["resource_8"]
    # nothing selected
    assert list(s.with_resources().selected_resources) == []
    # nothing is selected so nothing yielded
    assert list(s.with_resources()) == []


def test_clone_source() -> None:
    @dlt.source
    def test_source(no_resources):
        def _gen(i):
            yield "A" * i

        for i in range(no_resources):
            yield dlt.resource(_gen(i), name="resource_" + str(i))

    s = test_source(4)
    all_resource_names = ["resource_" + str(i) for i in range(4)]
    clone_s = s.clone()
    assert len(s.resources) == len(clone_s.resources) == len(all_resource_names)
    assert s.schema is not clone_s.schema
    for name in all_resource_names:
        # resource is a clone
        assert s.resources[name] is not clone_s.resources[name]
        assert s.resources[name]._pipe is not clone_s.resources[name]._pipe
        # but we keep pipe names
        assert s.resources[name].name == clone_s.resources[name].name

    assert list(s) == ["", "A", "AA", "AAA"]
    # we expired generators
    assert list(clone_s) == []

    # clone parametrized generators

    @dlt.source  # type: ignore[no-redef]
    def test_source(no_resources):
        def _gen(i):
            yield "A" * i

        for i in range(no_resources):
            yield dlt.resource(_gen, name="resource_" + str(i))

    s = test_source(4)
    clone_s = s.clone()
    # bind resources
    for idx, name in enumerate(all_resource_names):
        s.resources[name].bind(idx)
        clone_s.resources[name].bind(idx)

    # now thanks to late eval both sources evaluate separately
    assert list(s) == ["", "A", "AA", "AAA"]
    assert list(clone_s) == ["", "A", "AA", "AAA"]


def test_multiple_parametrized_transformers() -> None:
    @dlt.source
    def _source(test_set: int = 1):
        @dlt.resource(selected=False)
        def _r1():
            yield ["a", "b", "c"]

        @dlt.transformer(data_from=_r1, selected=False)
        def _t1(items, suffix):
            yield list(map(lambda i: i + "_" + suffix, items))

        @dlt.transformer(data_from=_t1)
        def _t2(items, mul):
            yield items * mul

        if test_set == 1:
            return _r1, _t1, _t2
        if test_set == 2:
            return _t1, _t2
        if test_set == 3:
            # parametrize _t1
            return _t1("2") | _t2
        if test_set == 4:
            # true pipelining fun
            return _r1() | _t1("2") | _t2(2)

    expected_data = ["a_2", "b_2", "c_2", "a_2", "b_2", "c_2"]

    # this s contains all resources
    s = _source(1)
    # all resources will be extracted (even if just the _t2 is selected)
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    assert set(s.resources.selected_dag) == {("_r1", "_t1"), ("_t1", "_t2")}
    components = s.decompose("scc")
    # only one isolated component
    assert len(components) == 1
    # only _t2 is selected so component has only this node explicitly
    assert set(components[0].selected_resources.keys()) == {"_t2"}

    # parametrize now
    s.resources["_t1"].bind("2")
    s._t2.bind(2)
    assert list(s) == expected_data
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    # deselect _t2 and now nothing is selected
    s._t2.selected = False
    assert set(s.resources.extracted.keys()) == set()
    assert set(s.resources.selected_dag) == set()
    assert s.decompose("scc") == []

    s._r1.selected = True
    # now only _r1
    assert set(s.resources.extracted.keys()) == {"_r1"}
    assert set(s.resources.selected_dag) == {("_r1", "_r1")}
    assert set(s.decompose("scc")[0].selected_resources.keys()) == {"_r1"}

    # this s contains only transformers
    s2 = _source(2)
    # the _r will be extracted but it is not in the resources list so we create a mock resource for it
    assert set(s2.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    s2._t1.bind("2")
    s2._t2.bind(2)
    assert list(s2) == expected_data
    # also dag has all the edges - including those outside of resources in the source
    assert set(s2.resources.selected_dag) == {("_r1", "_t1"), ("_t1", "_t2")}
    assert set(s2.decompose("scc")[0].selected_resources.keys()) == {"_t2"}
    # select the _t1
    s2._t1.selected = True
    assert set(s2.decompose("scc")[0].selected_resources.keys()) == {"_t2", "_t1"}

    s3 = _source(3)
    # here _t1 and _r1 are not in the source
    assert set(s3.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    s3._t2.bind(2)
    assert list(s3) == expected_data
    assert set(s3.resources.selected_dag) == {("_r1", "_t1"), ("_t1", "_t2")}

    s4 = _source(4)
    # here we return a pipe
    assert set(s4.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    assert list(s4) == expected_data
    assert set(s4.resources.selected_dag) == {("_r1", "_t1"), ("_t1", "_t2")}


def test_extracted_resources_selector() -> None:
    @dlt.source
    def _source(test_set: int = 1):
        @dlt.resource(selected=False, write_disposition="append")
        def _r1():
            yield ["a", "b", "c"]

        @dlt.transformer(data_from=_r1, selected=False, write_disposition="replace")
        def _t1(items, suffix):
            yield list(map(lambda i: i + "_" + suffix, items))

        @dlt.transformer(data_from=_r1, write_disposition="merge")
        def _t2(items, mul):
            yield items * mul

        if test_set == 1:
            return _r1, _t1, _t2
        if test_set == 2:
            return _t1, _t2

    s = _source(1)
    # t1 not selected
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1"}
    # append and replace
    assert s.resources.extracted["_r1"].write_disposition == "append"
    assert s.resources.extracted["_t2"].write_disposition == "merge"
    # # select _t1
    s._t1.bind("x").selected = True
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    assert s.resources.extracted["_t1"].write_disposition == "replace"

    s2 = _source(1)
    # are we a clone?
    assert s2._t1.selected is False

    s3 = _source(2)
    # only _t2 is enabled
    assert set(s3.resources.extracted.keys()) == {"_t2", "_r1"}
    # _r1 is merge (inherits from _t2)
    assert s3.resources.extracted["_r1"].write_disposition == "merge"
    # we have _r1 as parent for _t1 with "replace" and _t2 with "merge", the write disposition of _r1 is de facto undefined...
    s3._t1.selected = True
    assert set(s3.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    assert s3.resources.extracted["_r1"].write_disposition == "merge"
    s3._t2.selected = False
    assert set(s3.resources.extracted.keys()) == {"_r1", "_t1"}
    # inherits from _t1
    assert s3.resources.extracted["_r1"].write_disposition == "replace"


def test_source_decompose() -> None:
    @dlt.source
    def _source():
        @dlt.resource(selected=True)
        def _r_init():
            yield ["-", "x", "!"]

        @dlt.resource(selected=False)
        def _r1():
            yield ["a", "b", "c"]

        @dlt.transformer(data_from=_r1, selected=True)
        def _t1(items, suffix):
            yield list(map(lambda i: i + "_" + suffix, items))

        @dlt.transformer(data_from=_r1)
        def _t2(items, mul):
            yield items * mul

        @dlt.transformer(data_from=_r1)
        def _t3(items, mul):
            for item in items:
                yield item.upper() * mul

        # add something to init
        @dlt.transformer(data_from=_r_init)
        def _t_init_post(items):
            for item in items:
                yield item * 2

        @dlt.resource
        def _r_isolee():
            yield from ["AX", "CV", "ED"]

        return _r_init, _t_init_post, _r1, _t1("POST"), _t2(3), _t3(2), _r_isolee

    # when executing, we get the same data no matter the decomposition
    direct_data = list(_source())
    # no decomposition
    none_data = []
    for comp in _source().decompose("none"):
        none_data.extend(list(comp))
    assert direct_data == none_data

    scc_data = []
    for comp in _source().decompose("scc"):
        scc_data.extend(list(comp))
    assert direct_data == scc_data

    # keeps order of resources inside
    # here we didn't eliminate (_r_init, _r_init) as this not impacts decomposition, however this edge is not necessary
    assert _source().resources.selected_dag == [
        ("_r_init", "_r_init"),
        ("_r_init", "_t_init_post"),
        ("_r1", "_t1"),
        ("_r1", "_t2"),
        ("_r1", "_t3"),
        ("_r_isolee", "_r_isolee"),
    ]
    components = _source().decompose("scc")
    # first element contains _r_init
    assert "_r_init" in components[0].resources.selected.keys()
    # last is isolee
    assert "_r_isolee" in components[-1].resources.selected.keys()

    # groups isolated components
    assert len(components) == 3
    assert set(components[1].resources.selected.keys()) == {"_t1", "_t2", "_t3"}

    # keeps isolated resources
    assert list(components[-1].resources.selected.keys()) == ["_r_isolee"]


def test_illegal_double_bind() -> None:
    @dlt.resource()
    def _r1():
        yield ["a", "b", "c"]

    assert _r1.args_bound is False
    assert _r1().args_bound is True

    with pytest.raises(TypeError) as py_ex:
        _r1()()
    assert "Parametrized resource" in str(py_ex.value)

    with pytest.raises(TypeError) as py_ex:
        _r1.bind().bind()
    assert "Parametrized resource" in str(py_ex.value)

    bound_r = dlt.resource([1, 2, 3], name="rx")
    assert bound_r.args_bound is True
    with pytest.raises(TypeError):
        _r1()

    def _gen():
        yield from [1, 2, 3]

    assert dlt.resource(_gen()).args_bound is True


@dlt.resource
def res_in_res(table_name, w_d):
    def _gen(s):
        yield from s

    return dlt.resource(_gen, name=table_name, write_disposition=w_d)


def test_resource_returning_resource() -> None:
    @dlt.source
    def source_r_in_r():
        yield res_in_res

    s = source_r_in_r()
    assert s.res_in_res.name == "res_in_res"
    # this will return internal resource
    r_i = s.res_in_res("table", "merge")
    assert r_i.name == "table"
    assert r_i.compute_table_schema()["write_disposition"] == "merge"
    assert list(r_i("ABC")) == ["A", "B", "C"]


def test_source_dynamic_resource_attrs() -> None:
    # resources are also types

    @dlt.source
    def test_source(no_resources):
        for i in range(no_resources):
            yield dlt.resource(["A"] * i, name="resource_" + str(i))

    s = test_source(10)
    assert s.resource_1.name == s.resources["resource_1"].name
    assert id(s.resource_1) == id(s.resources["resource_1"])
    with pytest.raises(AttributeError):
        s.resource_30


def test_source_resource_attrs_with_conflicting_attrs() -> None:
    """Resource names that conflict with DltSource attributes do not work with attribute access"""
    dlt.pipeline(dev_mode=True)  # Create pipeline so state property can be accessed
    names = ["state", "resources", "schema", "name", "clone"]

    @dlt.source
    def test_source() -> Iterator[DltResource]:
        for name in names:
            yield dlt.resource(["A"], name=name)

    s = test_source()

    # Resources are in resource dict but attributes are not created
    assert set(s.resources.keys()) == set(names)
    for name in names:
        assert not isinstance(getattr(s, name), DltResource)


def test_add_transform_steps() -> None:
    # add all step types, using indexes. final steps
    # gen -> map that converts to str and multiplies character -> filter str of len 2 -> yield all characters in str separately
    r = (
        dlt.resource([1, 2, 3, 4], name="all")
        .add_limit(3)
        .add_yield_map(lambda i: (yield from i))
        .add_map(lambda i: str(i) * i, 1)
        .add_filter(lambda i: len(i) == 2, 2)
    )
    assert list(r) == ["2", "2"]


def test_add_transform_steps_pipe() -> None:
    r = dlt.resource([1, 2, 3], name="all") | (lambda i: str(i) * i) | (lambda i: (yield from i))
    assert list(r) == ["1", "2", "2", "3", "3", "3"]


def test_add_transformer_right_pipe() -> None:
    # def tests right hand pipe
    r = [1, 2, 3] | dlt.transformer(lambda i: i * 2, name="lambda")
    # resource was created for a list
    assert r._pipe.parent.name.startswith("iter")
    assert list(r) == [2, 4, 6]

    # works for iterators
    r = iter([1, 2, 3]) | dlt.transformer(lambda i: i * 3, name="lambda")
    assert list(r) == [3, 6, 9]

    # must be a transformer
    with pytest.raises(ResourceNotATransformer):
        iter([1, 2, 3]) | dlt.resource(lambda i: i * 3, name="lambda")


def test_limit_infinite_counter() -> None:
    r = dlt.resource(itertools.count(), name="infinity").add_limit(10)
    assert list(r) == list(range(10))


@pytest.mark.parametrize("limit", (None, -1, 0, 10))
def test_limit_edge_cases(limit: int) -> None:
    r = dlt.resource(range(20), name="infinity").add_limit(limit)  # type: ignore

    @dlt.resource()
    async def r_async():
        for i in range(20):
            await asyncio.sleep(0.01)
            yield i

    sync_list = list(r)
    async_list = list(r_async().add_limit(limit))

    if limit == 10:
        assert sync_list == list(range(10))
        # we have edge cases where the async list will have one extra item
        # possibly due to timing issues, maybe some other implementation problem
        assert (async_list == list(range(10))) or (async_list == list(range(11)))
    elif limit in [None, -1]:
        assert sync_list == async_list == list(range(20))
    elif limit == 0:
        assert sync_list == async_list == []
    else:
        raise AssertionError(f"Unexpected limit: {limit}")


def test_limit_source() -> None:
    def mul_c(item):
        yield from "A" * (item + 2)

    @dlt.source
    def infinite_source():
        for idx in range(3):
            r = dlt.resource(itertools.count(), name=f"infinity_{idx}").add_limit(10)
            yield r
            yield r | dlt.transformer(name=f"mul_c_{idx}")(mul_c)

    # transformer is not limited to 2 elements, infinite resource is, we have 3 resources
    assert list(infinite_source().add_limit(2)) == ["A", "A", 0, "A", "A", "A", 1] * 3


def test_source_state() -> None:
    @dlt.source
    def test_source(expected_state):
        assert source_state() == expected_state
        return DltResource(Pipe.from_data("pipe", [1, 2, 3]), None, False)

    with pytest.raises(PipelineStateNotAvailable):
        test_source({}).state

    dlt.pipeline(dev_mode=True)
    assert test_source({}).state == {}

    # inject state to see if what we write in state is there
    with Container().injectable_context(StateInjectableContext(state={})) as state:
        test_source({}).state["value"] = 1  # type: ignore[index]
        test_source({"value": 1})
        assert state.state == {"sources": {"test_source": {"value": 1}}}


def test_resource_state() -> None:
    @dlt.resource
    def test_resource():
        yield [1, 2, 3]

    @dlt.source(schema=Schema("schema_section"))
    def test_source():
        return test_resource

    r = test_resource()
    s = test_source()
    assert s.name == "schema_section"

    with pytest.raises(PipelineStateNotAvailable):
        r.state
    with pytest.raises(PipelineStateNotAvailable):
        s.state
    with pytest.raises(PipelineStateNotAvailable):
        s.test_resource.state

    p = dlt.pipeline(dev_mode=True)
    assert r.state == {}
    assert s.state == {}
    assert s.test_resource.state == {}

    with Container().injectable_context(StateInjectableContext(state={})) as state:
        r.state["direct"] = True  # type: ignore[index]
        s.test_resource.state["in-source"] = True  # type: ignore[index]
        # resource section is current module
        print(state.state)
        # the resource that is a part of the source will create a resource state key in the source state key
        assert state.state["sources"]["schema_section"] == {
            "resources": {"test_resource": {"in-source": True}}
        }
        assert s.state == {"resources": {"test_resource": {"in-source": True}}}
        # the standalone resource will create key which is default schema name
        assert state.state["sources"][p._make_schema_with_default_name().name] == {
            "resources": {"test_resource": {"direct": True}}
        }


# def test_add_resources_to_source_simple() -> None:
#     pass


def test_resource_dict_add() -> None:
    def input_gen():
        yield from [1, 2, 3]

    def tx_step(item):
        return item * 2

    res_dict = DltResourceDict("source", "section")
    input_r = DltResource.from_data(input_gen)
    input_r_orig_pipe = input_r._pipe
    input_tx = DltResource.from_data(tx_step, data_from=DltResource.Empty)
    input_tx_orig_pipe = input_tx._pipe

    res_dict["tx_step"] = input_r | input_tx
    # pipes cloned on setter
    assert res_dict["tx_step"] is not input_tx
    assert res_dict["tx_step"]._pipe is not input_tx._pipe
    # pipes in original resources not touched
    assert input_r_orig_pipe == input_r._pipe
    assert input_tx_orig_pipe == input_tx._pipe

    # now add the parent
    res_dict["input_gen"] = input_r
    # got cloned
    assert res_dict["input_gen"] is not input_r
    # but the clone points to existing parent
    assert res_dict["input_gen"]._pipe is res_dict["tx_step"]._pipe.parent
    assert res_dict._new_pipes == []
    assert len(res_dict._cloned_pairs) == 2
    res_dict["tx_clone"] = (input_r | input_tx).with_name("tx_clone")
    assert res_dict["tx_clone"]._pipe.parent is not res_dict["input_gen"]._pipe
    assert len(res_dict._cloned_pairs) == 4
    assert input_r_orig_pipe == input_r._pipe
    assert input_tx_orig_pipe == input_tx._pipe

    # add all together
    res_dict = DltResourceDict("source", "section")
    res_dict.add(input_r, input_r | input_tx)
    assert res_dict._new_pipes == []
    assert res_dict._suppress_clone_on_setitem is False
    assert res_dict["input_gen"]._pipe is res_dict["tx_step"]._pipe.parent
    # pipes in original resources not touched
    assert input_r_orig_pipe == input_r._pipe
    assert input_tx_orig_pipe == input_tx._pipe

    # replace existing resource which has the old pipe
    res_dict["input_gen"] = input_r
    # an existing clone got assigned
    assert res_dict["input_gen"]._pipe is res_dict["tx_step"]._pipe.parent
    # keep originals
    assert input_r_orig_pipe == input_r._pipe
    assert input_tx_orig_pipe == input_tx._pipe

    # replace existing resource which has the new pipe
    res_dict["input_gen"] = input_r()
    # we have disconnected gen and parent of tx TODO: we should handle this
    assert res_dict["input_gen"]._pipe is not res_dict["tx_step"]._pipe.parent
    # keep originals
    assert input_r_orig_pipe == input_r._pipe
    assert input_tx_orig_pipe == input_tx._pipe

    # can't set with different name than resource really has
    with pytest.raises(ValueError):
        res_dict["input_gen_x"] = input_r.with_name("uniq")

    # can't add resource with same name again
    with pytest.raises(InvalidResourceDataTypeMultiplePipes):
        res_dict.add(input_r)


@pytest.mark.parametrize("add_mode", ("add", "dict", "set"))
def test_add_transformer_to_source(add_mode: str) -> None:
    @dlt.resource(name="numbers")
    def number_gen(init):
        yield from range(init, init + 5)

    @dlt.source
    def number_source():
        return number_gen

    source = number_source()

    @dlt.transformer
    def multiplier(item):
        return item * 2

    mul_pipe = source.numbers | multiplier()

    if add_mode == "add":
        source.resources.add(mul_pipe)
    elif add_mode == "dict":
        source.resources["multiplier"] = mul_pipe
    else:
        source.multiplier = mul_pipe

    # need to bind numbers
    with pytest.raises(ParametrizedResourceUnbound):
        list(source)

    source.numbers.bind(10)
    # both numbers and multiplier are evaluated, numbers only once
    assert list(source) == [20, 10, 22, 11, 24, 12, 26, 13, 28, 14]


def test_unknown_resource_access() -> None:
    @dlt.resource(name="numbers")
    def number_gen(init):
        yield from range(init, init + 5)

    @dlt.source
    def number_source():
        return number_gen

    source = number_source()

    with pytest.raises(AttributeError):
        source.unknown

    with pytest.raises(KeyError):
        source.resources["unknown"]


def test_clone_resource_on_call():
    @dlt.resource(name="gene")
    def number_gen(init):
        yield from range(init, init + 5)

    @dlt.transformer()
    def multiplier(number, mul):
        return number * mul

    gene_clone = number_gen(10)
    assert gene_clone is not number_gen
    assert gene_clone._pipe is not number_gen._pipe
    assert gene_clone.name == number_gen.name

    pipe = number_gen | multiplier
    pipe_clone = pipe(4)
    assert pipe_clone._pipe is not pipe._pipe
    assert pipe._pipe is multiplier._pipe
    # but parents are the same
    assert pipe_clone._pipe.parent is number_gen._pipe
    with pytest.raises(ParametrizedResourceUnbound):
        list(pipe_clone)
    # bind the original directly via pipe
    pipe_clone._pipe.parent.bind_gen(10)
    assert list(pipe_clone) == [40, 44, 48, 52, 56]


def test_clone_resource_on_bind():
    @dlt.resource(name="gene")
    def number_gen():
        yield from range(1, 5)

    @dlt.transformer
    def multiplier(number, mul):
        return number * mul

    pipe = number_gen | multiplier
    bound_pipe = pipe.bind(3)
    assert bound_pipe is pipe is multiplier
    assert bound_pipe._pipe is pipe._pipe
    assert bound_pipe._pipe.parent is pipe._pipe.parent


@dlt.resource(selected=False)
def number_gen_ext(max_r=3):
    yield from range(1, max_r)


def test_clone_resource_with_rename():
    assert number_gen_ext.SPEC is not BaseConfiguration
    gene_r = number_gen_ext.with_name("gene")
    assert number_gen_ext.name == "number_gen_ext"
    assert gene_r.name == "gene"
    assert number_gen_ext.section == gene_r.section
    assert gene_r.SPEC is number_gen_ext.SPEC
    assert gene_r.selected == number_gen_ext.selected is False


def test_source_multiple_iterations() -> None:
    def some_data():
        yield [1, 2, 3]
        yield [1, 2, 3]

    s = DltSource(Schema("source"), "module", [dlt.resource(some_data())])
    assert s.exhausted is False
    assert list(s) == [1, 2, 3, 1, 2, 3]
    assert s.exhausted is True
    assert list(s) == []
    info = str(s)
    assert "Source is already iterated" in info


def test_exhausted_property() -> None:
    # this example will be exhausted after iteration
    def open_generator_data():
        yield from [1, 2, 3, 4]

    s = DltSource(Schema("source"), "module", [dlt.resource(open_generator_data())])
    assert s.exhausted is False
    assert next(iter(s)) == 1
    assert s.exhausted is True

    # lists will not exhaust
    s = DltSource(
        Schema("source"),
        "module",
        [dlt.resource([1, 2, 3, 4], table_name="table", name="resource")],
    )
    assert s.exhausted is False
    assert next(iter(s)) == 1
    assert s.exhausted is False

    # iterators will not exhaust
    s = DltSource(
        Schema("source"),
        "module",
        [dlt.resource(iter([1, 2, 3, 4]), table_name="table", name="resource")],
    )
    assert s.exhausted is False
    assert next(iter(s)) == 1
    assert s.exhausted is False

    # having on exhausted generator resource will make the whole source exhausted
    def open_generator_data():  # type: ignore[no-redef]
        yield from [1, 2, 3, 4]

    s = DltSource(
        Schema("source"),
        "module",
        [
            dlt.resource([1, 2, 3, 4], table_name="table", name="resource"),
            dlt.resource(open_generator_data()),
        ],
    )
    assert s.exhausted is False

    # execute the whole source
    list(s)
    assert s.exhausted is True

    # source with transformers also exhausts
    @dlt.source
    def mysource():
        r = dlt.resource(itertools.count(start=1), name="infinity").add_limit(5)
        yield r
        yield r | dlt.transformer(name="double")(lambda x: x * 2)

    s = mysource()
    assert s.exhausted is False
    assert next(iter(s)) == 2  # transformer is returned before resource
    assert s.exhausted is False


def test_exhausted_with_limit() -> None:
    def open_generator_data():
        yield from [1, 2, 3, 4]

    s = DltSource(
        Schema("source"),
        "module",
        [dlt.resource(open_generator_data)],
    )
    assert s.exhausted is False
    list(s)
    assert s.exhausted is False

    # use limit
    s.add_limit(1)
    list(s)
    # must still be false, limit should not open generator if it is still generator function
    assert s.exhausted is False
    assert list(s) == [1]


def test_clone_resource_with_name() -> None:
    @dlt.resource(selected=False)
    def _r1():
        yield ["a", "b", "c"]

    @dlt.transformer(selected=True)
    def _t1(items, suffix):
        yield list(map(lambda i: i + "_" + suffix, items))

    r1 = _r1()
    r1_clone = r1.with_name("r1_clone")
    # new name of resource and pipe
    assert r1_clone.name == "r1_clone"
    assert r1_clone._pipe.name == "r1_clone"
    assert r1_clone.table_name == "r1_clone"
    # original keeps old name and pipe
    assert r1._pipe is not r1_clone._pipe
    assert r1.name == "_r1"
    assert r1.table_name == "_r1"

    # clone transformer before it is bound
    bound_t1_clone = r1_clone | _t1.with_name("t1_clone")("ax")
    bound_t1_clone_2 = r1_clone | _t1("ax_2").with_name("t1_clone_2")
    assert bound_t1_clone.name == "t1_clone"
    assert bound_t1_clone_2.name == "t1_clone_2"
    assert bound_t1_clone.table_name == "t1_clone"
    assert bound_t1_clone_2.table_name == "t1_clone_2"
    # but parent is the same (we cloned only transformer - before it is bound)
    assert bound_t1_clone_2._pipe.parent is bound_t1_clone._pipe.parent

    # evaluate transformers
    assert list(bound_t1_clone) == ["a_ax", "b_ax", "c_ax"]
    assert list(bound_t1_clone_2) == ["a_ax_2", "b_ax_2", "c_ax_2"]

    # clone pipes (bound transformer)
    pipe_r1 = _r1()
    pipe_t1 = _t1("cx")
    pipe_r1_t1 = pipe_r1 | pipe_t1
    pipe_r1_t1_clone = pipe_r1_t1.with_name("pipe_clone")
    assert pipe_r1_t1_clone.name == "pipe_clone"
    # parent of the pipe also cloned and renamed
    assert pipe_r1_t1_clone._pipe.parent.name == "_r1_pipe_clone"
    # originals are not affected
    assert pipe_r1.name == "_r1"
    assert pipe_t1.name == "_t1"
    # binding a transformer is not cloning the original
    assert pipe_t1._pipe is pipe_r1_t1._pipe
    assert pipe_r1._pipe is pipe_r1_t1._pipe.parent
    # with_name clones
    assert pipe_t1._pipe is not pipe_r1_t1_clone._pipe
    assert pipe_r1._pipe is not pipe_r1_t1_clone._pipe.parent

    # rename again
    pipe_r1_t1_clone_2 = pipe_r1_t1_clone.with_name("pipe_clone_2")
    # replace previous name part (pipe_clone in _r1_pipe_clone) with pipe_clone_2
    assert pipe_r1_t1_clone_2._pipe.parent.name == "_r1_pipe_clone_2"

    # preserves table name if set
    table_t1 = _r1 | _t1
    table_t1.table_name = "Test_Table"
    table_t1_clone = table_t1.with_name("table_t1_clone")
    assert table_t1_clone.name == "table_t1_clone"
    assert table_t1_clone.table_name == "Test_Table"

    # also preserves when set the same name as resource name
    assert table_t1.name == "_t1"
    table_t1.table_name = "_t1"
    table_t1_clone = table_t1.with_name("table_t1_clone")
    assert table_t1_clone.name == "table_t1_clone"
    assert table_t1_clone.table_name == "_t1"


def test_apply_hints() -> None:
    def empty_gen():
        yield [1, 2, 3]

    empty_table_schema = {
        "name": "empty_gen",
        "columns": {},
        "resource": "empty_gen",
        "write_disposition": "append",
    }

    empty = DltResource.from_data(empty_gen)

    empty_r = empty()
    # check defaults
    assert empty_r.name == empty.name == empty_r.table_name == empty.table_name == "empty_gen"
    # assert empty_r._table_schema_template is None
    assert empty_r.compute_table_schema() == empty_table_schema
    assert empty_r.write_disposition == "append"

    empty_r.apply_hints(write_disposition="replace")
    assert empty_r.write_disposition == "replace"
    empty_r.write_disposition = "merge"
    assert empty_r.compute_table_schema()["write_disposition"] == "merge"
    # delete hint
    empty_r.apply_hints(write_disposition="")
    empty_r.write_disposition = "append"
    assert empty_r.compute_table_schema()["write_disposition"] == "append"

    empty_r.apply_hints(
        table_name="table",
        parent_table_name="parent",
        primary_key=["a", "b"],
        merge_key=["c", "a"],
        schema_contract="freeze",
        table_format="delta",
        file_format="jsonl",
    )
    table = empty_r.compute_table_schema()
    assert table["columns"]["a"] == {
        "merge_key": True,
        "name": "a",
        "nullable": False,
        "primary_key": True,
    }
    assert table["columns"]["b"] == {"name": "b", "nullable": False, "primary_key": True}
    assert table["columns"]["c"] == {"merge_key": True, "name": "c", "nullable": False}
    assert table["name"] == "table"
    assert table["parent"] == "parent"
    assert empty_r.table_name == "table"
    assert table["schema_contract"] == "freeze"
    assert table["table_format"] == "delta"
    assert table["file_format"] == "jsonl"

    # reset
    empty_r.apply_hints(
        table_name="",
        parent_table_name="",
        table_format="",
        file_format="",
        primary_key=[],
        merge_key="",
        columns={},
        incremental=Incremental.EMPTY,
        schema_contract={},
    )
    assert empty_r._hints == {
        "columns": {},
        "incremental": Incremental.EMPTY,
        "validator": None,
        "write_disposition": "append",
        "original_columns": {},
    }
    table = empty_r.compute_table_schema()
    assert table["name"] == "empty_gen"
    assert "parent" not in table
    assert table["columns"] == {}
    assert empty_r.compute_table_schema() == empty_table_schema

    # combine columns with primary key
    empty_r = empty()
    empty_r.apply_hints(
        columns={"tags": {"data_type": "json", "primary_key": False}},
        primary_key="tags",
        merge_key="tags",
    )
    # primary key not set here
    assert empty_r.columns["tags"] == {"data_type": "json", "name": "tags", "primary_key": False}
    # only in the computed table
    assert empty_r.compute_table_schema()["columns"]["tags"] == {
        "data_type": "json",
        "name": "tags",
        "nullable": False,  # NOT NULL because `tags` do not define it
        "primary_key": True,
        "merge_key": True,
    }
    # test SCD2 write disposition hint
    empty_r.apply_hints(
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "validity_column_names": ["from", "to"],
        }
    )
    assert empty_r._hints["write_disposition"] == {
        "disposition": "merge",
        "strategy": "scd2",
        "validity_column_names": ["from", "to"],
    }
    assert "from" not in empty_r._hints["columns"]
    assert "to" not in empty_r._hints["columns"]
    table = empty_r.compute_table_schema()
    assert table["write_disposition"] == "merge"
    assert table["x-merge-strategy"] == "scd2"
    assert "from" in table["columns"]
    assert "x-valid-from" in table["columns"]["from"]
    assert "to" in table["columns"]
    assert "x-valid-to" in table["columns"]["to"]

    # Test table references hint
    reference_hint = [
        dict(
            referenced_table="other_table",
            columns=["a", "b"],
            referenced_columns=["other_a", "other_b"],
        )
    ]
    empty_r.apply_hints(references=reference_hint)
    assert empty_r._hints["references"] == reference_hint
    table = empty_r.compute_table_schema()
    assert table["references"] == reference_hint

    # Apply references again, list is extended
    reference_hint_2 = [
        dict(
            referenced_table="other_table_2",
            columns=["c", "d"],
            referenced_columns=["other_c", "other_d"],
        )
    ]
    empty_r.apply_hints(references=reference_hint_2)
    assert empty_r._hints["references"] == reference_hint + reference_hint_2
    table = empty_r.compute_table_schema()
    assert table["references"] == reference_hint + reference_hint_2

    # Duplicate reference is replaced
    reference_hint_3 = [
        dict(
            referenced_table="other_table",
            columns=["a2", "b2"],
            referenced_columns=["other_a2", "other_b2"],
        )
    ]
    empty_r.apply_hints(references=reference_hint_3)
    assert empty_r._hints["references"] == reference_hint_3 + reference_hint_2
    table = empty_r.compute_table_schema()
    assert table["references"] == reference_hint_3 + reference_hint_2


def test_apply_dynamic_hints() -> None:
    def empty_gen():
        yield [1, 2, 3]

    empty = DltResource.from_data(empty_gen)

    empty_r = empty()
    with pytest.raises(InconsistentTableTemplate):
        empty_r.apply_hints(parent_table_name=lambda ev: ev["p"], write_disposition=None)

    empty_r.apply_hints(
        table_name=lambda ev: ev["t"], parent_table_name=lambda ev: ev["p"], write_disposition=None
    )
    assert empty_r._table_name_hint_fun is not None
    assert empty_r._table_has_other_dynamic_hints is True

    with pytest.raises(DataItemRequiredForDynamicTableHints):
        empty_r.compute_table_schema()
    table = empty_r.compute_table_schema({"t": "table", "p": "parent"})
    assert table["name"] == "table"
    assert table["parent"] == "parent"

    # try write disposition and primary key
    empty_r.apply_hints(primary_key=lambda ev: ev["pk"], write_disposition=lambda ev: ev["wd"])
    table = empty_r.compute_table_schema(
        {"t": "table", "p": "parent", "pk": ["a", "b"], "wd": "skip"}
    )
    assert table["write_disposition"] == "skip"
    assert "a" in table["columns"]

    # validate fails
    with pytest.raises(DictValidationException):
        empty_r.compute_table_schema(
            {"t": "table", "p": "parent", "pk": ["a", "b"], "wd": "x-skip"}
        )

    # dynamic columns
    empty_r.apply_hints(columns=lambda ev: ev["c"])
    table = empty_r.compute_table_schema(
        {"t": "table", "p": "parent", "pk": ["a", "b"], "wd": "skip", "c": [{"name": "tags"}]}
    )
    assert table["columns"]["tags"] == {"name": "tags"}


def test_apply_hints_complex_migration() -> None:
    def empty_gen():
        yield [1, 2, 3]

    empty = DltResource.from_data(empty_gen)
    empty_r = empty()

    def dyn_type(ev):
        # must return columns in one of the known formats
        return [{"name": "dyn_col", "data_type": ev["dt"]}]

    # start with static columns, update to dynamic
    empty_r.apply_hints(
        table_name=lambda ev: ev["t"], columns=[{"name": "dyn_col", "data_type": "json"}]
    )

    table = empty_r.compute_table_schema({"t": "table"})
    assert table["columns"]["dyn_col"]["data_type"] == "json"

    empty_r.apply_hints(table_name=lambda ev: ev["t"], columns=dyn_type)
    table = empty_r.compute_table_schema({"t": "table", "dt": "complex"})
    assert table["columns"]["dyn_col"]["data_type"] == "json"

    # start with dynamic
    empty_r = empty()
    empty_r.apply_hints(table_name=lambda ev: ev["t"], columns=dyn_type)
    table = empty_r.compute_table_schema({"t": "table", "dt": "complex"})
    assert table["columns"]["dyn_col"]["data_type"] == "json"


def test_apply_hints_table_variants() -> None:
    def empty_gen():
        yield [1, 2, 3]

    empty = DltResource.from_data(empty_gen)

    # table name must be a string
    with pytest.raises(ValueError):
        empty.apply_hints(write_disposition="append", create_table_variant=True)
    with pytest.raises(ValueError):
        empty.apply_hints(
            table_name=lambda ev: ev["t"], write_disposition="append", create_table_variant=True
        )

    # table a with replace
    empty.apply_hints(table_name="table_a", write_disposition="replace", create_table_variant=True)
    table_a = empty.compute_table_schema(meta=TableNameMeta("table_a"))
    assert table_a["name"] == "table_a"
    assert table_a["write_disposition"] == "replace"

    # unknown table (without variant) - created out resource hints
    table_unk = empty.compute_table_schema(meta=TableNameMeta("table_unk"))
    assert table_unk["name"] == "empty_gen"
    assert table_unk["write_disposition"] == "append"

    # resource hints are base for table variants
    empty.apply_hints(
        primary_key="id",
        incremental=dlt.sources.incremental(cursor_path="x"),
        columns=[{"name": "id", "data_type": "bigint"}],
    )
    empty.apply_hints(table_name="table_b", write_disposition="merge", create_table_variant=True)
    table_b = empty.compute_table_schema(meta=TableNameMeta("table_b"))
    assert table_b["name"] == "table_b"
    assert table_b["write_disposition"] == "merge"
    assert len(table_b["columns"]) == 1
    assert table_b["columns"]["id"]["primary_key"] is True
    # overwrite table_b, remove column def and primary_key
    empty.apply_hints(table_name="table_b", columns=[], primary_key=(), create_table_variant=True)
    table_b = empty.compute_table_schema(meta=TableNameMeta("table_b"))
    assert table_b["name"] == "table_b"
    assert table_b["write_disposition"] == "merge"
    assert len(table_b["columns"]) == 0

    # dyn hints not allowed
    with pytest.raises(InconsistentTableTemplate):
        empty.apply_hints(
            table_name="table_b", write_disposition=lambda ev: ev["wd"], create_table_variant=True
        )


@pytest.mark.parametrize("key_prop", ("primary_key", "merge_key"))
def test_apply_hints_keys(key_prop: TColumnProp) -> None:
    def empty_gen():
        yield [1, 2, 3]

    key_columns = ["id_1", "id_2"]

    empty = DltResource.from_data(empty_gen)
    # apply compound key
    empty.apply_hints(**{key_prop: key_columns})  # type: ignore
    table = empty.compute_table_schema()
    actual_keys = utils.get_columns_names_with_prop(table, key_prop, include_incomplete=True)
    assert actual_keys == key_columns
    # nullable is false
    actual_keys = utils.get_columns_names_with_prop(table, "nullable", include_incomplete=True)
    assert actual_keys == key_columns

    # apply new key
    key_columns_2 = ["id_1", "id_3"]
    empty.apply_hints(**{key_prop: key_columns_2})  # type: ignore
    table = empty.compute_table_schema()
    actual_keys = utils.get_columns_names_with_prop(table, key_prop, include_incomplete=True)
    assert actual_keys == key_columns_2
    actual_keys = utils.get_columns_names_with_prop(table, "nullable", include_incomplete=True)
    assert actual_keys == key_columns_2

    # if column is present for a key, it get merged and nullable should be preserved
    id_2_col: TColumnSchema = {
        "name": "id_2",
        "data_type": "bigint",
    }

    empty.apply_hints(**{key_prop: key_columns}, columns=[id_2_col])  # type: ignore
    table = empty.compute_table_schema()
    actual_keys = utils.get_columns_names_with_prop(table, key_prop, include_incomplete=True)
    assert set(actual_keys) == set(key_columns)
    # nullable not set in id_2_col so NOT NULL is set
    actual_keys = utils.get_columns_names_with_prop(table, "nullable", include_incomplete=True)
    assert set(actual_keys) == set(key_columns)

    id_2_col["nullable"] = True
    empty.apply_hints(**{key_prop: key_columns}, columns=[id_2_col])  # type: ignore
    table = empty.compute_table_schema()
    actual_keys = utils.get_columns_names_with_prop(table, key_prop, include_incomplete=True)
    assert set(actual_keys) == set(key_columns)
    # id_2 set to NULL
    actual_keys = utils.get_columns_names_with_prop(table, "nullable", include_incomplete=True)
    assert set(actual_keys) == {"id_1"}

    # apply key via schema
    key_columns_3 = ["id_2", "id_1", "id_3"]
    id_2_col[key_prop] = True

    empty = DltResource.from_data(empty_gen)
    empty.apply_hints(**{key_prop: key_columns_2}, columns=[id_2_col])  # type: ignore
    table = empty.compute_table_schema()
    # all 3 columns have the compound key. we do not prevent setting keys via schema
    actual_keys = utils.get_columns_names_with_prop(table, key_prop, include_incomplete=True)
    assert actual_keys == key_columns_3
    actual_keys = utils.get_columns_names_with_prop(table, "nullable", include_incomplete=True)
    assert actual_keys == key_columns_2


def test_resource_no_template() -> None:
    empty = DltResource.from_data([1, 2, 3], name="table")
    assert empty.write_disposition == "append"
    assert empty.compute_table_schema()["write_disposition"] == "append"
    empty.apply_hints()
    assert empty.write_disposition == "append"
    assert empty.compute_table_schema()["write_disposition"] == "append"


def test_selected_pipes_with_duplicates():
    def input_gen():
        yield from [1, 2, 3]

    def tx_step(item):
        return item * 2

    input_r = DltResource.from_data(input_gen)
    input_r_clone = input_r.with_name("input_gen_2")

    # separate resources have separate pipe instances
    source = DltSource(Schema("dupes"), "module", [input_r, input_r_clone])
    pipes = source.resources.pipes
    assert len(pipes) == 2
    assert pipes[0].name == "input_gen"
    assert source.resources[pipes[0].name] == source.input_gen
    selected_pipes = source.resources.selected_pipes
    assert len(selected_pipes) == 2
    assert selected_pipes[0].name == "input_gen"
    assert list(source) == [1, 2, 3, 1, 2, 3]

    # cloned from fresh resource
    source = DltSource(
        Schema("dupes"),
        "module",
        [DltResource.from_data(input_gen), DltResource.from_data(input_gen).with_name("gen_2")],
    )
    assert list(source) == [1, 2, 3, 1, 2, 3]

    # clone transformer
    input_r = DltResource.from_data(input_gen)
    input_tx = DltResource.from_data(tx_step, data_from=DltResource.Empty)
    source = DltSource(
        Schema("dupes"), "module", [input_r, (input_r | input_tx).with_name("tx_clone")]
    )
    pipes = source.resources.pipes
    assert len(pipes) == 2
    assert source.resources[pipes[0].name] == source.input_gen
    assert source.resources[pipes[1].name] == source.tx_clone
    selected_pipes = source.resources.selected_pipes
    assert len(selected_pipes) == 2
    assert list(source) == [1, 2, 3, 2, 4, 6]
