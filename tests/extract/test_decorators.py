import pytest

import dlt
from dlt.extract.exceptions import InvalidResourceDataTypeFunctionNotAGenerator, InvalidResourceDataTypeIsNone, ParametrizedResourceUnbound, PipeNotBoundToData, ResourceFunctionExpected, SourceDataIsNone, SourceIsAClassTypeError, SourceNotAFunction
from dlt.extract.source import DltResource

from tests.common.utils import IMPORTED_VERSION_HASH_ETH_V5


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
    assert schema.stored_version_hash == IMPORTED_VERSION_HASH_ETH_V5


def test_unbound_parametrized_transformer() -> None:

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

    assert list(empty_t_1("_meta")) == [1, 2, 3, 1, 2, 3, 1, 2, 3]

    with pytest.raises(ParametrizedResourceUnbound):
        list(empty_t_1)


def test_source_name_is_invalid_schema_name() -> None:

    # inferred from function name, names must be small caps etc.

    def camelCase():
        return dlt.resource([1, 2, 3], name="resource")

    s = dlt.source(camelCase)()
    assert s.name == "camelCase"
    schema = s.discover_schema()
    assert schema.name == "camel_case"
    assert list(s) == [1, 2, 3]

    # explicit name
    s = dlt.source(camelCase, name="source!")()
    assert s.name == "source!"
    schema = s.discover_schema()
    assert schema.name == "source_"
    assert list(s) == [1, 2, 3]


def test_resource_name_is_invalid_table_name_and_columns() -> None:

    @dlt.source
    def camelCase():
        return dlt.resource([1, 2, 3], name="Resource !", columns={"KA!AX": {"name": "DIF!", "nullable": False, "data_type": "text"}})

    s = camelCase()
    assert s.resources["Resource !"].selected
    assert hasattr(s, "Resource !")

    # get schema and check table name
    schema = s.discover_schema()
    assert "resource_" in schema._schema_tables
    # has the column with identifiers normalized
    assert "ka_ax" in schema.get_table("resource_")["columns"]


def test_resource_name_from_generator() -> None:
    def some_data():
        yield [1, 2, 3]

    r = dlt.resource(some_data())
    assert r.name == "some_data"


@pytest.mark.skip
def test_resource_sets_invalid_write_disposition() -> None:
    # write_disposition="xxx" # this will fail schema
    pass


def test_class_source() -> None:

    class _Source:
        def __init__(self, elems: int) -> None:
            self.elems = elems

        def __call__(self, more: int = 1):
            return dlt.resource(["A", "V"] * self.elems * more, name="_list")

    s = dlt.source(_Source(4))(more=1)
    assert s.name == "_Source"
    schema = s.discover_schema()
    assert schema.name == "_source"
    assert "_list" in schema._schema_tables
    assert list(s) == ['A', 'V', 'A', 'V', 'A', 'V', 'A', 'V']

    with pytest.raises(SourceIsAClassTypeError):
        @dlt.source(name="planB")
        class _SourceB:
            def __init__(self, elems: int) -> None:
                self.elems = elems

            def __call__(self, more: int = 1):
                return dlt.resource(["A", "V"] * self.elems * more, name="_list")


@pytest.mark.skip("Not implemented")
def test_class_resource() -> None:
    pass
