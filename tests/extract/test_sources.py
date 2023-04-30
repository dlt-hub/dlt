import itertools
import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.exceptions import PipelineStateNotAvailable
from dlt.common.pipeline import StateInjectableContext, source_state
from dlt.common.schema import Schema
from dlt.common.typing import TDataItems
from dlt.extract.exceptions import InvalidParentResourceDataType, InvalidParentResourceIsAFunction, InvalidTransformerDataTypeGeneratorFunctionRequired, InvalidTransformerGeneratorFunction, ParametrizedResourceUnbound, ResourcesNotFoundError
from dlt.extract.pipe import Pipe
from dlt.extract.typing import FilterItem, MapItem
from dlt.extract.source import DltResource, DltSource

from tests.pipeline.utils import drop_pipeline


def test_call_data_resource() -> None:
    with pytest.raises(TypeError):
        DltResource.from_data([1], name="t")()


def test_parametrized_resource() -> None:

    def parametrized(p1, /, p2, *, p3 = None):
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
    s = DltSource("source", "module", Schema("source"), [r])

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

    def good_transformer(item, /, p1, p2, *, p3 = None):
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
        dlt.transformer(r)("a")
    with pytest.raises(InvalidTransformerDataTypeGeneratorFunctionRequired):
        dlt.transformer(r)(bad_transformer())

    # transformer must take at least one arg
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(r)(bad_transformer)
    assert py_ex.value.code == 1
    # transformer may have only one positional argument and it must be first
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(r)(bad_transformer_2)
    assert py_ex.value.code == 2
    # first argument cannot be kw only
    with pytest.raises(InvalidTransformerGeneratorFunction) as py_ex:
        dlt.transformer(r)(bad_transformer_3)
    assert py_ex.value.code == 3

    # transformer must take data from a resource
    with pytest.raises(InvalidTransformerGeneratorFunction):
        dlt.transformer(bad_transformer)(good_transformer)
    with pytest.raises(InvalidParentResourceDataType):
        dlt.transformer(bad_transformer())(good_transformer)

    # transformer is unbound
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(r)(good_transformer)
    with pytest.raises(ParametrizedResourceUnbound):
        list(t)

    # pass wrong arguments
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(r)(good_transformer)
    with pytest.raises(TypeError):
        list(t("p1", 1, 2, 3, 4))

    # pass arguments that fully bind the item
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(r)(good_transformer)
    with pytest.raises(TypeError):
        t(item={}, p1="p2", p2=1)

    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(r)(good_transformer)
    items = list(t(p1="p1", p2=2))

    def assert_items(_items: TDataItems) -> None:
        # 2 items yielded * p2=2
        assert len(_items) == 2*2
        assert _items[0] == {'wrap': 'itemX', 'mark': 'p1', 'iter': 0}
        assert _items[3] == {'wrap': 'itemY', 'mark': 'p1', 'iter': 1}

    assert_items(items)

    # parameters passed as args
    r = dlt.resource(["itemX", "itemY"], name="items")
    t = dlt.transformer(r)(good_transformer)
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

    # add parametrized to source
    @dlt.source
    def test_source():
        return parametrized

    s = test_source()
    # we cloned the instance
    assert s.resources["parametrized"] is not parametrized
    cloned_r = s.resources["parametrized"]
    # calling resources always create a copy
    cr_1 = cloned_r(10)
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
    # pipe has different id
    assert regular._pipe._pipe_id != b_regular._pipe._pipe_id

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
    s.regular.add_map(lambda i: i*3)
    assert len(regular._pipe) == 2
    assert len(s.regular._pipe) == 3

    # call
    assert list(s.regular(["A", "A", "B", "B"])) == ["AAA", "AAA"]
    # bind
    s.regular.bind([["A"], ["A"], ["B", "A"], ["B", "C"]])
    assert list(s.regular) == ["AAA", "AAA", "AAA"]

    # binding resource that returns resource will replace the object content, keeping the object id
    s.returns_res.add_map(lambda i: i*3)
    s.returns_res.bind(["X", "Y", "Z"])
    # got rid of all mapping and filter functions
    assert len(s.returns_res._pipe) == 1
    assert list(s.returns_res) == ["X", "Y", "Z"]

    # same for resource returning pipe
    s.returns_pipe.add_map(lambda i: i*3)
    s.returns_pipe.bind(["X", "Y", "M"])
    # got rid of all mapping and filter functions
    assert len(s.returns_pipe._pipe) == 1
    assert list(s.returns_pipe) == ["X", "Y", "M"]

    # s.regular is exhausted so set it again
    # add lambda that after filtering for A, will multiply it by 4
    s.resources["regular"] = regular.add_map(lambda i: i*4)(["A", "Y"])
    assert list(s) == ['X', 'Y', 'Z', 'X', 'Y', 'M', 'AAAA']


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
    data1._name = "state1_data"
    dlt.pipeline(full_refresh=True).extract([data1, some_data("state2")], schema=Schema("default"))
    # both should be extracted. what we test here is the combination of binding the resource by calling it that clones the internal pipe
    # and then creating a source with both clones. if we keep same pipe id when cloning on call, a single pipe would be created shared by two resources
    assert all_yields == ["state1", "state2"]


def test_resource_bind_lazy_eval() -> None:

    @dlt.resource
    def needs_param(param):
        yield from range(param)

    @dlt.transformer(needs_param(3))
    def tx_form(item, multi):
        yield item*multi

    @dlt.transformer(tx_form(2))
    def tx_form_fin(item, div):
        yield item / div

    @dlt.transformer(needs_param)
    def tx_form_dir(item, multi):
        yield item*multi

    # tx_form takes data from needs_param(3) which is lazily evaluated
    assert list(tx_form(2)) == [0, 2, 4]
    # so it will not get exhausted
    assert list(tx_form(2)) == [0, 2, 4]

    # same for tx_form_fin
    assert list(tx_form_fin(3)) == [0, 2/3, 4/3]
    assert list(tx_form_fin(3)) == [0, 2/3, 4/3]

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
    tx_stage.add_filter(FilterItem(lambda letter: letter.isupper()), 0)
    # be got filtered out before duplication
    assert list(dlt.resource(["A", "b", "C"], name="data") | tx_stage) == ['A', 'A', 'C', 'C']

    # filter after duplication
    tx_stage = dlt.transformer()(yield_twice)()
    tx_stage.add_filter(FilterItem(lambda letter: letter.isupper()))
    # nothing is filtered out: on duplicate we also capitalize so filter does not trigger
    assert list(dlt.resource(["A", "b", "C"], name="data") | tx_stage) == ['A', 'A', 'B', 'B', 'C', 'C']


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
    # returns self
    assert s is s_sel
    assert list(s.selected_resources) == ["resource_1", "resource_7"] == list(s.resources.selected)
    assert list(s.resources) == all_resource_names
    info = str(s)
    assert "resource resource_0 is not selected" in info

    # reselect
    assert list(s.with_resources("resource_8").selected_resources) == ["resource_8"]
    # nothing selected
    assert list(s.with_resources().selected_resources) == []
    # nothing is selected so nothing yielded
    assert list(s) == []


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
            yield items*mul

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


    expected_data = ['a_2', 'b_2', 'c_2', 'a_2', 'b_2', 'c_2']

    # this s contains all resources
    s = _source(1)
    # all resources will be extracted (even if just the _t2 is selected)
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    # parametrize now
    s.resources["_t1"].bind("2")
    s._t2.bind(2)
    # print(list(s._t2))
    assert list(s) == expected_data
    assert set(s.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    # deselect _t2 and now nothing is selected
    s._t2.selected = False
    assert set(s.resources.extracted.keys()) == set()
    s._r1.selected = True
    # now only r2
    assert set(s.resources.extracted.keys()) == {"_r1"}

    # this s contains only transformers
    s2 = _source(2)
    # the _r will be extracted but it is not in the resources list so we create a mock resource for it
    assert set(s2.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    s2._t1.bind("2")
    s2._t2.bind(2)
    assert list(s2) == expected_data

    s3 = _source(3)
    # here _t1 and _r1 are not in the source
    assert set(s3.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    s3._t2.bind(2)
    assert list(s3) == expected_data

    s4 = _source(4)
    # here we return a pipe
    assert set(s4.resources.extracted.keys()) == {"_t2", "_r1", "_t1"}
    assert list(s4) == expected_data


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
            yield items*mul

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


def test_illegal_double_bind() -> None:
    @dlt.resource()
    def _r1():
        yield ["a", "b", "c"]

    with pytest.raises(TypeError) as py_ex:
        _r1()()
    assert "Bound DltResource" in str(py_ex.value)

    with pytest.raises(TypeError) as py_ex:
        _r1.bind().bind()
    assert "Bound DltResource" in str(py_ex.value)



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
    assert r_i.table_schema()["write_disposition"] == "merge"
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
    with pytest.raises(KeyError):
        s.resource_30


def test_add_transform_steps() -> None:
    # add all step types, using indexes. final steps
    # gen -> map that converts to str and multiplies character -> filter str of len 2 -> yield all characters in str separately
    r = dlt.resource([1, 2, 3, 4], name="all").add_limit(3).add_yield_map(lambda i: (yield from i)).add_map(lambda i: str(i) * i, 1).add_filter(lambda i: len(i) == 2, 2)
    assert list(r) == ["2", "2"]


def test_add_transform_steps_pipe() -> None:
    r = dlt.resource([1, 2, 3], name="all") | (lambda i: str(i) * i) | (lambda i: (yield from i))
    assert list(r) == ['1', '2', '2', '3', '3', '3']


def test_limit_infinite_counter() -> None:
    r = dlt.resource(itertools.count(), name="infinity").add_limit(10)
    assert list(r) == list(range(10))


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
    assert list(infinite_source().add_limit(2)) == ['A', 'A', 0, 'A', 'A', 'A', 1] * 3


def test_source_state() -> None:

    @dlt.source
    def test_source(expected_state):
        assert source_state() == expected_state
        return DltResource(Pipe.from_data("pipe", [1, 2, 3]), None, False)

    with pytest.raises(PipelineStateNotAvailable):
        test_source({}).state

    dlt.pipeline(full_refresh=True)
    assert test_source({}).state  == {}

    # inject state to see if what we write in state is there
    with Container().injectable_context(StateInjectableContext(state={})) as state:
        test_source({}).state["value"] = 1
        test_source({"value": 1})
        assert state.state == {'sources': {'test_sources': {'value': 1}}}


def test_resource_state() -> None:

    @dlt.resource
    def test_resource():
        yield [1, 2, 3]

    @dlt.source(section="source_section")
    def test_source():
        return test_resource

    r = test_resource()
    s = test_source()

    with pytest.raises(PipelineStateNotAvailable):
        r.state
    with pytest.raises(PipelineStateNotAvailable):
        s.state
    with pytest.raises(PipelineStateNotAvailable):
        s.test_resource.state

    dlt.pipeline(full_refresh=True)
    assert r.state == {}
    assert s.state == {}
    assert s.test_resource.state == {}

    with Container().injectable_context(StateInjectableContext(state={})) as state:
        r.state["direct"] = True
        s.test_resource.state["in-source"] = True
        # resource section is current module
        assert state.state["sources"]["test_sources"] == {'resources': {'test_resource': {'direct': True}}}
        # in source resource is part of the source state
        assert s.state == {'resources': {'test_resource': {'in-source': True}}}


# def test_add_resources_to_source_simple() -> None:
#     pass


@pytest.mark.skip("not implemented")
def test_resource_dict() -> None:
    # the dict of resources in source
    # test clone
    # test delete

    pass


def test_source_multiple_iterations() -> None:

    def some_data():
        yield [1, 2, 3]
        yield [1, 2, 3]

    s = DltSource("source", "module", Schema("default"), [dlt.resource(some_data())])
    assert s.exhausted is False
    assert list(s) == [1, 2, 3, 1, 2, 3]
    assert s.exhausted is True
    assert list(s) == []
    info = str(s)
    assert "Source is already iterated" in info
