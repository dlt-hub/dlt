import pytest

import dlt
from dlt.extract.exceptions import InvalidResourceDataTypeFunctionNotAGenerator, InvalidResourceDataTypeIsNone, ResourceFunctionExpected, SourceDataIsNone, SourceNotAFunction
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
