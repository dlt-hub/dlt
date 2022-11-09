import pytest

import dlt
from dlt.extract.exceptions import InvalidResourceDataTypeFunctionNotAGenerator, InvalidResourceDataTypeIsNone, ParametrizedResourceUnbound, PipeNotBoundToData, ResourceFunctionExpected, SourceDataIsNone, SourceNotAFunction
from dlt.extract.source import DltResource


def test_none_returning_source() -> None:
    with pytest.raises(SourceNotAFunction):
        dlt.source("data")()

    def empty() -> None:
        pass

    with pytest.raises(SourceDataIsNone):
        dlt.source(empty)()


    @dlt.source
    def deco_empty() -> None:
        pass


    with pytest.raises(SourceDataIsNone):
        deco_empty()


def test_none_returning_resource() -> None:
    with pytest.raises(ResourceFunctionExpected):
        dlt.resource(None)(None)

    def empty() -> None:
        pass

    with pytest.raises(InvalidResourceDataTypeFunctionNotAGenerator):
        dlt.resource(empty)()

    with pytest.raises(InvalidResourceDataTypeFunctionNotAGenerator):
        dlt.resource(None)(empty)()

    with pytest.raises(InvalidResourceDataTypeIsNone):
        DltResource.from_data(None, name="test")


def test_load_schema_for_callable() -> None:
    from tests.extract.cases.eth_source.source import ethereum

    s = ethereum()
    schema = s.discover_schema()
    assert schema.name == "ethereum"
    # the schema in the associated file has this hash
    assert schema.stored_version_hash == "njJAySgJRs2TqGWgQXhP+3pCh1A1hXcqe77BpM7JtOU="


def test_unbound_transformer() -> None:

    empty_pipe = DltResource.Empty._pipe
    assert empty_pipe.is_empty
    assert not empty_pipe.is_data_bound
    assert not empty_pipe.has_parent

    bound_r = dlt.resource([1, 2, 3], name="data")
    assert bound_r._pipe.is_data_bound
    assert not bound_r._pipe.has_parent
    assert not bound_r._pipe.is_empty
    bound_r._pipe.evaluate_head()

    @dlt.transformer()
    def empty_t_1(items, _meta):
        yield [1, 2, 3]

    empty_r = empty_t_1("meta")
    assert empty_r._pipe.parent.is_empty
    assert empty_r._pipe.is_data_bound is False
    with pytest.raises(PipeNotBoundToData):
        empty_r._pipe.evaluate_head()
    # create bound pipe
    (bound_r | empty_r)._pipe.evaluate_head()

    assert empty_t_1._pipe.parent.is_empty
    assert empty_t_1._pipe.is_data_bound is False
    with pytest.raises(PipeNotBoundToData):
        empty_t_1._pipe.evaluate_head()
    with pytest.raises(ParametrizedResourceUnbound):
        (bound_r | empty_t_1)._pipe.evaluate_head()

    assert list(empty_t_1("_meta")) == [[1, 2, 3], [1, 2, 3], [1, 2, 3]]

    with pytest.raises(ParametrizedResourceUnbound):
        list(empty_t_1)


@pytest.mark.skip("not implemented")
def test_source_name_is_invalid_schema_name() -> None:
    # both inferred from function/class name or explicit
    pass


@pytest.mark.skip("not implemented")
def test_resource_name_is_invalid_table_name() -> None:
    pass


@pytest.mark.skip("not implemented")
def test_class_source() -> None:

    class _source:
        def __init__(self, elems: int) -> None:
            self.elems = elems

        def __call__(self):
            return dlt.resource(["A", "V"] * self.elems, name="_list")


    s = dlt.source(_source(4))
    print(list(s()))
