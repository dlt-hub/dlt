import pytest

import dlt
from dlt.common.schema import Schema
from dlt.common.typing import TDataItems
from dlt.extract.exceptions import InvalidParentResourceDataType, InvalidParentResourceIsAFunction, InvalidTransformerDataTypeGeneratorFunctionRequired, InvalidTransformerGeneratorFunction, ParametrizedResourceUnbound, ResourcesNotFoundError
from dlt.extract.pipe import FilterItem
from dlt.extract.source import DltResource, DltSource


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
    # iterating the source will raise unbound exception
    with pytest.raises(ParametrizedResourceUnbound) as py_ex:
        list(r)
    assert py_ex.value.func_name == "parametrized"
    assert py_ex.value.resource_name == "parametrized"

    r = DltResource.from_data(parametrized)
    # bind
    assert list(r("p1", 1, p3=None)) == [1]

    # as part of the source
    r = DltResource.from_data(parametrized)
    s = DltSource("source", Schema("source"), [r])

    with pytest.raises(ParametrizedResourceUnbound) as py_ex:
        list(s)

    # bind
    assert list(s.resources["parametrized"]("p1", 1, p3=None)) == [1]


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
    cr_1 = cloned_r(10)
    assert cloned_r is cr_1
    # will raise
    with pytest.raises(TypeError):
        # already bound
        cloned_r(10)


def test_resource_double_bind() -> None:
    pass


def test_transformer_preliminary_step() -> None:

    def yield_twice(item):
        yield item.upper()
        yield item.upper()

    tx_stage = dlt.transformer()(yield_twice)()
    # filter out small caps and insert this before the head
    tx_stage._pipe._steps.insert(0, FilterItem(lambda letter: letter.isupper()))
    # be got filtered out before duplication
    assert list(dlt.resource(["A", "b", "C"], name="data") | tx_stage) == ['A', 'A', 'C', 'C']

    # filter after duplication
    tx_stage = dlt.transformer()(yield_twice)()
    tx_stage.filter(FilterItem(lambda letter: letter.isupper()))
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

    # successful select
    s_sel = s.with_resources("resource_1", "resource_7")
    # returns self
    assert s is s_sel
    assert list(s.selected_resources) == ["resource_1", "resource_7"] == list(s.resources.selected)
    assert list(s.resources) == all_resource_names

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

    expected_data = [['a_2', 'b_2', 'c_2', 'a_2', 'b_2', 'c_2']]

    # this s contains all resources
    s = _source(1)
    # parametrize now
    s._t1("2")
    s._t2(2)
    assert list(s) == expected_data

    # this s contains only transformers
    s2 = _source(2)
    s2._t1("2")
    s2._t2(2)
    assert list(s2) == expected_data

    s3 = _source(3)
    s3._t2(2)
    assert list(s3) == expected_data

    s4 = _source(4)
    assert list(s4) == expected_data


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


@pytest.mark.skip("not implemented")
def test_resource_dict() -> None:
    # test clone
    # test delete

    pass

@pytest.mark.skip("not implemented")
def test_resource_multiple_iterations() -> None:
    pass