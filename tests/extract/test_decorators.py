import os
import pytest

import dlt
from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.inject import get_fun_spec
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.exceptions import ArgumentsOverloadException, DictValidationException
from dlt.common.pipeline import StateInjectableContext, TPipelineState
from dlt.common.source import _SOURCES
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table

from dlt.cli.source_detection import detect_source_configs
from dlt.extract.exceptions import ExplicitSourceNameInvalid, InvalidResourceDataTypeFunctionNotAGenerator, InvalidResourceDataTypeIsNone, ParametrizedResourceUnbound, PipeNotBoundToData, ResourceFunctionExpected, ResourceInnerCallableConfigWrapDisallowed, SourceDataIsNone, SourceIsAClassTypeError, SourceNotAFunction, SourceSchemaNotAvailable
from dlt.extract.source import DltResource, DltSource
from dlt.common.schema.exceptions import InvalidSchemaName

from tests.common.utils import IMPORTED_VERSION_HASH_ETH_V6


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
    schema = s.schema
    assert schema.name == "ethereum" == s.name
    # the schema in the associated file has this hash
    assert schema.stored_version_hash == IMPORTED_VERSION_HASH_ETH_V6


def test_unbound_parametrized_transformer() -> None:

    empty_pipe = DltResource.Empty._pipe
    assert empty_pipe.is_empty
    assert not empty_pipe.is_data_bound
    assert not empty_pipe.has_parent

    bound_r = dlt.resource([1, 2, 3], name="data")
    assert bound_r._pipe.is_data_bound
    assert not bound_r._pipe.has_parent
    assert not bound_r._pipe.is_empty
    bound_r._pipe.evaluate_gen()

    @dlt.transformer()
    def empty_t_1(items, _meta):
        yield [1, 2, 3]

    empty_r = empty_t_1("meta")
    assert empty_r._pipe.parent.is_empty
    assert empty_r._pipe.is_data_bound is False
    with pytest.raises(PipeNotBoundToData):
        empty_r._pipe.evaluate_gen()
    # create bound pipe
    (bound_r | empty_r)._pipe.evaluate_gen()

    assert empty_t_1._pipe.parent.is_empty
    assert empty_t_1._pipe.is_data_bound is False
    with pytest.raises(PipeNotBoundToData):
        empty_t_1._pipe.evaluate_gen()

    # here we bind the bound_r to empty_t_1 and then evaluate gen on a clone pipe which fails
    with pytest.raises(ParametrizedResourceUnbound):
        (bound_r | empty_t_1)._pipe._clone(keep_pipe_id=False).evaluate_gen()

    # here we still have original (non cloned) pipe where gen was not evaluated
    assert list(empty_t_1("_meta")) == [1, 2, 3, 1, 2, 3, 1, 2, 3]

    with pytest.raises(ParametrizedResourceUnbound):
        list(empty_t_1)


def test_transformer_no_parens() -> None:
    bound_r = dlt.resource([1, 2, 3], name="data")

    @dlt.transformer
    def empty_t_1(item, meta = None):
        yield "a" * item

    assert list(bound_r | empty_t_1) == ["a", "aa", "aaa"]

    def empty_t_2(item, _meta):
        yield _meta * item

    # create dynamic transformer with explicit func
    t = dlt.transformer(empty_t_2, data_from=bound_r)
    assert list(t("m")) == ["m", "mm", "mmm"]


def test_source_name_is_invalid_schema_name() -> None:

    def camelCase():
        return dlt.resource([1, 2, 3], name="resource")

    s = dlt.source(camelCase)()
    # source name will be normalized
    assert s.name == "camelCase"
    assert s.schema.name == "camelCase"
    schema = s.discover_schema()
    assert schema.name == "camelCase"
    assert list(s) == [1, 2, 3]

    # explicit name
    with pytest.raises(InvalidSchemaName) as py_ex:
        s = dlt.source(camelCase, name="source!")()
    assert py_ex.value.name == "source!"

    # explicit name and schema mismatch
    with pytest.raises(ArgumentsOverloadException) as py_ex:
        s = dlt.source(camelCase, name="source_ovr", schema=Schema("compat"))()
    # overload exception applies to dlt.source
    assert py_ex.value.func_name == "source"


def test_resource_name_is_invalid_table_name_and_columns() -> None:

    @dlt.source
    def camelCase():
        return dlt.resource([1, 2, 3], name="Resource !", columns={"KA!AX": {"name": "DIF!", "nullable": False, "data_type": "text"}})

    s = camelCase()
    assert s.resources["Resource !"].selected
    assert hasattr(s, "Resource !")

    # get schema and check table name
    schema = s.discover_schema()
    assert "resourcex" in schema.tables
    # has the column with identifiers normalized
    assert "ka_ax" in schema.get_table("resourcex")["columns"]
    assert schema.get_table("resourcex")["columns"]["ka_ax"]["name"] == "ka_ax"


def test_columns_argument() -> None:

    @dlt.resource(name="user", columns={"tags": {"data_type": "complex", "x-extra": "x-annotation"}})
    def get_users():
        yield {"u": "u", "tags": [1, 2 ,3]}

    t = get_users().table_schema()
    # nullable is added
    assert t["columns"]["tags"]["nullable"] is True
    assert t["columns"]["tags"]["data_type"] == "complex"
    assert t["columns"]["tags"]["x-extra"] == "x-annotation"

    r = get_users()
    r.apply_hints(columns={"invalid": {"data_type": "unk", "wassup": False}})
    with pytest.raises(DictValidationException):
        r.table_schema()

    r = get_users()
    r.apply_hints(columns={"tags": {"x-second-extra": "x-second-annotation"}})
    t = r.table_schema()

    assert t["columns"]["tags"]["x-second-extra"] == "x-second-annotation"


def test_resource_name_from_generator() -> None:
    def some_data():
        yield [1, 2, 3]

    r = dlt.resource(some_data())
    assert r.name == "some_data"
    assert r.section == "test_decorators"


def test_source_sections() -> None:
    # source in __init__.py of module
    from tests.extract.cases.section_source import init_source_f_1, init_resource_f_2
    # source in file module with name override
    from tests.extract.cases.section_source.named_module import source_f_1, resource_f_2

    # we crawl the sections from the most general (no section) to full path

    # values without section
    os.environ["VAL"] = "TOP LEVEL"
    assert list(init_source_f_1()) == ["TOP LEVEL"]
    assert list(init_resource_f_2()) == ["TOP LEVEL"]
    assert list(source_f_1()) == ["TOP LEVEL"]
    assert list(resource_f_2()) == ["TOP LEVEL"]

    # values in sources section
    os.environ[f"{known_sections.SOURCES.upper()}__VAL"] = "SOURCES LEVEL"
    assert list(init_source_f_1()) == ["SOURCES LEVEL"]
    assert list(init_resource_f_2()) == ["SOURCES LEVEL"]
    assert list(source_f_1()) == ["SOURCES LEVEL"]
    assert list(resource_f_2()) == ["SOURCES LEVEL"]

    # values in module section
    os.environ[f"{known_sections.SOURCES.upper()}__SECTION_SOURCE__VAL"] = "SECTION SOURCE LEVEL"
    assert list(init_source_f_1()) == ["SECTION SOURCE LEVEL"]
    assert list(init_resource_f_2()) == ["SECTION SOURCE LEVEL"]
    # here overridden by __source_name__
    os.environ[f"{known_sections.SOURCES.upper()}__NAME_OVERRIDDEN__VAL"] = "NAME OVERRIDDEN LEVEL"
    assert list(source_f_1()) == ["NAME OVERRIDDEN LEVEL"]
    assert list(resource_f_2()) == ["NAME OVERRIDDEN LEVEL"]

    # values in function name section
    os.environ[f"{known_sections.SOURCES.upper()}__SECTION_SOURCE__INIT_SOURCE_F_1__VAL"] = "SECTION INIT_SOURCE_F_1 LEVEL"
    assert list(init_source_f_1()) == ["SECTION INIT_SOURCE_F_1 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__SECTION_SOURCE__INIT_RESOURCE_F_2__VAL"] = "SECTION INIT_RESOURCE_F_2 LEVEL"
    assert list(init_resource_f_2()) == ["SECTION INIT_RESOURCE_F_2 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__NAME_OVERRIDDEN__SOURCE_F_1__VAL"] = "NAME SOURCE_F_1 LEVEL"
    assert list(source_f_1()) == ["NAME SOURCE_F_1 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__NAME_OVERRIDDEN__RESOURCE_F_2__VAL"] = "NAME RESOURCE_F_2 LEVEL"
    assert list(resource_f_2()) == ["NAME RESOURCE_F_2 LEVEL"]


def test_source_explicit_section() -> None:

    @dlt.source(section="custom_section", schema=Schema("custom_section"))
    def with_section(secret=dlt.secrets.value):

        @dlt.resource
        def mod_state():
            dlt.current.source_state()["val"] = secret
            yield 1

        return mod_state

    os.environ["SOURCES__CUSTOM_SECTION__SECRET"] = "CUSTOM"
    state: TPipelineState = {}
    with Container().injectable_context(StateInjectableContext(state=state)):
        assert list(with_section()) == [1]
        # source state key is still source name
        assert state["sources"]["custom_section"]["val"] == "CUSTOM"


def test_resource_section() -> None:

    r = dlt.resource([1, 2, 3], name="T")
    assert r.name == "T"
    assert r.section is None

    def _inner_gen():
        yield from [1, 2, 3]

    r = dlt.resource(_inner_gen)()
    assert r.name == "_inner_gen"
    assert r.section == "test_decorators"

    from tests.extract.cases.section_source.external_resources import init_resource_f_2
    assert init_resource_f_2.name == "init_resource_f_2"
    assert init_resource_f_2.section == "section_source"


def test_resources_injected_sections() -> None:
    from tests.extract.cases.section_source.external_resources import with_external, with_bound_external, init_resource_f_2, resource_f_2
    # standalone resources must accept the injected sections for lookups
    os.environ["SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL"] = "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL"
    os.environ["SOURCES__EXTERNAL_RESOURCES__VAL"] = "SOURCES__EXTERNAL_RESOURCES__VAL"
    os.environ["SOURCES__SECTION_SOURCE__VAL"] = "SOURCES__SECTION_SOURCE__VAL"
    os.environ["SOURCES__NAME_OVERRIDDEN__VAL"] = "SOURCES__NAME_OVERRIDDEN__VAL"

    # the external resources use their standalone sections: no section context is injected
    assert list(init_resource_f_2()) == ["SOURCES__SECTION_SOURCE__VAL"]
    assert list(resource_f_2()) == ["SOURCES__NAME_OVERRIDDEN__VAL"]

    # the source returns: it's own argument, same via inner resource, and two external resources that are not bound
    # the iterator in the source will force its sections so external resource sections are not used
    assert list(with_external()) == [
        "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
        "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
        "SOURCES__EXTERNAL_RESOURCES__VAL",
        "SOURCES__EXTERNAL_RESOURCES__VAL"
    ]
    # this source will bind external resources before returning them (that is: calling them and obtaining generators)
    # the iterator in the source will force its sections so external resource sections are not used
    s = with_bound_external()
    assert list(s) == list([
        "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
        "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
        "SOURCES__EXTERNAL_RESOURCES__VAL",
        "SOURCES__EXTERNAL_RESOURCES__VAL"
    ])

    # inject the source sections like the Pipeline object would
    s = with_external()
    assert s.name == "with_external"
    assert s.section == "external_resources"  # from module name hosting the function
    with inject_section(ConfigSectionContext(pipeline_name="injected_external", sections=("sources", s.section, s.name))):
        # now the external sources must adopt the injected namespace
        assert(list(s)) == [
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL"
        ]

    # now with environ values that specify source/resource name: the module of the source, the name of the resource
    os.environ["SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL"] = "SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL"
    os.environ["SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL"] = "SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL"
    s = with_external()
    with inject_section(ConfigSectionContext(pipeline_name="injected_external", sections=("sources", s.section, s.name))):
        # now the external sources must adopt the injected namespace
        assert(list(s)) == [
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL",
            "SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL"
        ]

def test_source_schema_context() -> None:
    import dlt

    # global schema directly in the module
    global_schema = Schema("global")

    # not called from the source
    with pytest.raises(SourceSchemaNotAvailable):
        dlt.current.source_schema()

    def _assert_source_schema(s: DltSource, expected_name: str) -> None:
        assert list(s) == [1, 2, 3]
        assert s.discover_schema().name == expected_name
        assert "source_table" in s.discover_schema().tables

    # schema created by the source
    @dlt.source
    def created_ad_hoc():
        schema = dlt.current.source_schema()
        assert schema.name == "created_ad_hoc"
        # modify schema in place
        schema.update_schema(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_ad_hoc(), "created_ad_hoc")

    # schema created directly
    @dlt.source(schema=Schema("explicit"))
    def created_explicit():
        schema = dlt.current.source_schema()
        assert schema.name == "explicit"
        # modify schema in place
        schema.update_schema(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_explicit(), "explicit")

    # schema instance from a module
    @dlt.source(schema=global_schema)
    def created_global():
        schema = dlt.current.source_schema()
        assert schema.name == "global"
        # modify schema in place
        schema.update_schema(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_global(), "global")


def test_source_state_context() -> None:

    @dlt.resource(selected=False)
    def main():
        state = dlt.current.state()
        mark = state.setdefault("mark", 1)
        # increase the multiplier each time state is obtained
        state["mark"] *= 2
        yield [1, 2, 3]
        assert dlt.state()["mark"] == mark*2

    @dlt.transformer(data_from=main)
    def feeding(item):
        # we must have state
        assert dlt.current.source_state()["mark"] > 1
        mark = dlt.current.source_state()["mark"]
        yield from map(lambda i: i*mark, item)

    @dlt.source
    def pass_the_state():
        return main, feeding

    # must enumerate source correctly and preserve the state between the sources
    assert list(pass_the_state()) == [2, 4, 6]
    # state is discarded after iteration
    assert list(pass_the_state()) == [2, 4, 6]

    # must enumerate single resource
    assert list(feeding) == [2, 4, 6]


def test_source_schema_modified() -> None:

    @dlt.source
    def schema_test():
        return dlt.resource(["A", "B"], name="alpha")

    s = schema_test()
    schema = s.discover_schema()
    schema.update_schema(new_table("table"))
    s = schema_test()
    assert "table" not in s.discover_schema().tables


@dlt.resource
def standalone_resource(secret=dlt.secrets.value, config=dlt.config.value, opt: str = "A"):
    yield 1


def test_spec_generation() -> None:

    # inner resource cannot take configuration

    with pytest.raises(ResourceInnerCallableConfigWrapDisallowed) as py_ex:

        @dlt.resource(write_disposition="merge", primary_key="id")
        def inner_resource(initial_id = dlt.config.value):
            yield [{"id": 1, "name": "row1"}, {"id": 1, "name": "row2"}]

    assert py_ex.value.resource_name == "inner_resource"

    # outer resource does not take default params
    SPEC = get_fun_spec(standalone_resource._pipe.gen)
    fields = SPEC.get_resolvable_fields()

    # args with defaults are ignored
    assert len(fields) == 2
    assert "secret" in fields
    assert "config" in fields

    @dlt.source
    def inner_source(secret=dlt.secrets.value, config=dlt.config.value, opt: str = "A"):
        return standalone_resource

    SPEC = _SOURCES[inner_source.__qualname__].SPEC
    fields = SPEC.get_resolvable_fields()
    assert {"secret", "config", "opt"} == set(fields.keys())


@dlt.resource
def not_args_r():
    yield from [1, 2, 3]


def test_sources_no_arguments() -> None:

    @dlt.source
    def no_args():
        return dlt.resource([1, 2], name="data")

    # there is no spec if no arguments
    SPEC = _SOURCES[no_args.__qualname__].SPEC
    assert SPEC is None
    _, _, checked = detect_source_configs(_SOURCES, "", ())
    assert no_args.__qualname__ in checked

    SPEC = _SOURCES[no_args.__qualname__].SPEC
    assert SPEC is None
    _, _, checked = detect_source_configs(_SOURCES, "", ())
    assert not_args_r.__qualname__ in checked

    @dlt.resource
    def not_args_r_i():
        yield from [1, 2, 3]

    assert not_args_r_i.__qualname__ not in _SOURCES

    # you can call those
    assert list(no_args()) == [1, 2]
    assert list(not_args_r()) == [1, 2, 3]
    assert list(not_args_r_i()) == [1, 2, 3]


def test_resource_sets_invalid_write_disposition() -> None:

    @dlt.resource(write_disposition="xxxx")
    def invalid_disposition():
        yield from [1, 2, 3]

    r = invalid_disposition()
    with pytest.raises(DictValidationException) as py_ex:
        r.table_schema()
    assert "write_disposition" in str(py_ex.value)


def test_class_source() -> None:

    class _Source:
        def __init__(self, elems: int) -> None:
            self.elems = elems

        def __call__(self, more: int = 1):
            return dlt.resource(["A", "V"] * self.elems * more, name="_list")

    # CAN decorate callable classes
    s = dlt.source(_Source(4))(more=1)
    assert s.name == "_Source"
    schema = s.discover_schema()
    assert schema.name == "_Source"
    assert "_list" in schema.tables
    assert list(s) == ['A', 'V', 'A', 'V', 'A', 'V', 'A', 'V']

    # CAN'T decorate classes themselves
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
