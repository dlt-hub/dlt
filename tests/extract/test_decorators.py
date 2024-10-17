import inspect
import os
from typing import List, Optional, Dict, Iterator, Any, cast

import pytest
from pydantic import BaseModel

import dlt
from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.inject import get_fun_spec
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.exceptions import ArgumentsOverloadException, DictValidationException
from dlt.common.pipeline import StateInjectableContext, TPipelineState
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table, new_column
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema.exceptions import InvalidSchemaName
from dlt.common.typing import TDataItem

from dlt.cli.source_detection import detect_source_configs
from dlt.common.utils import custom_environ
from dlt.extract.decorators import DltSourceFactoryWrapper
from dlt.extract.source import SourceReference
from dlt.extract import DltResource, DltSource
from dlt.extract.exceptions import (
    DynamicNameNotStandaloneResource,
    ExplicitSourceNameInvalid,
    InvalidResourceDataTypeFunctionNotAGenerator,
    InvalidResourceDataTypeIsNone,
    InvalidResourceDataTypeMultiplePipes,
    ParametrizedResourceUnbound,
    PipeGenInvalid,
    PipeNotBoundToData,
    ResourceFunctionExpected,
    ResourceInnerCallableConfigWrapDisallowed,
    SourceDataIsNone,
    SourceIsAClassTypeError,
    SourceNotAFunction,
    CurrentSourceSchemaNotAvailable,
    InvalidParallelResourceDataType,
    UnknownSourceReference,
)
from dlt.extract.items import TableNameMeta

from tests.common.utils import load_yml_case
from tests.utils import MockableRunContext


def test_default_resource() -> None:
    @dlt.resource
    def resource():
        yield [1, 2, 3]

    # simple generated table schema
    assert resource().compute_table_schema() == {
        "columns": {},
        "name": "resource",
        "resource": "resource",
        "write_disposition": "append",
    }
    assert resource._args_bound is False
    assert resource.name == "resource"
    assert resource().args_bound is True
    assert resource().name == "resource"
    assert resource.incremental is None
    assert resource.write_disposition == "append"


def test_none_returning_source() -> None:
    with pytest.raises(SourceNotAFunction):
        dlt.source("data")()  # type: ignore[call-overload]

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
    eth_v9 = load_yml_case("schemas/eth/ethereum_schema_v9")
    # source removes processing hints so we do
    reference_schema = Schema.from_dict(eth_v9, remove_processing_hints=True)
    assert schema.stored_version_hash == reference_schema.stored_version_hash


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
        (bound_r | empty_t_1)._pipe._clone().evaluate_gen()

    # here we still have original (non cloned) pipe where gen was not evaluated
    assert list(empty_t_1("_meta")) == [1, 2, 3, 1, 2, 3, 1, 2, 3]

    with pytest.raises(ParametrizedResourceUnbound):
        list(empty_t_1)


def test_transformer_no_parens() -> None:
    bound_r = dlt.resource([1, 2, 3], name="data")

    @dlt.transformer
    def empty_t_1(item, meta=None):
        yield "a" * item

    assert list(bound_r | empty_t_1) == ["a", "aa", "aaa"]

    def empty_t_2(item, _meta):
        yield _meta * item

    # create dynamic transformer with explicit func
    t = dlt.transformer(empty_t_2, data_from=bound_r)
    assert list(t("m")) == ["m", "mm", "mmm"]


def test_transformer_kwargs() -> None:
    @dlt.resource
    def emit_tables():
        yield dlt.mark.with_table_name(1, "table_1")
        yield dlt.mark.with_table_name(2, "table_2")

    @dlt.transformer
    def ignore_meta(item, **kwargs):
        assert "meta" not in kwargs
        yield item

    @dlt.transformer
    def accept_meta(item, meta=None, **kwargs):
        assert "meta" not in kwargs
        assert isinstance(meta, TableNameMeta)
        yield item

    assert list(emit_tables | ignore_meta) == [1, 2]
    assert list(emit_tables | accept_meta) == [1, 2]


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
    with pytest.raises(ArgumentsOverloadException) as py_ex2:
        s = dlt.source(camelCase, name="source_ovr", schema=Schema("compat"))()
    # overload exception applies to dlt.source
    assert py_ex2.value.func_name == "source"


def test_resource_name_is_invalid_table_name_and_columns() -> None:
    @dlt.source
    def camelCase():
        return dlt.resource(
            [1, 2, 3],
            name="Resource !",
            columns={"KA!AX": {"name": "DIF!", "nullable": False, "data_type": "text"}},
        )

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
    @dlt.resource(name="user", columns={"tags": {"data_type": "json", "x-extra": "x-annotation"}})  # type: ignore[typeddict-unknown-key]
    def get_users():
        yield {"u": "u", "tags": [1, 2, 3]}

    t = get_users().compute_table_schema()

    assert "nullable" not in t["columns"]["tags"]
    assert t["columns"]["tags"]["data_type"] == "json"
    assert t["columns"]["tags"]["x-extra"] == "x-annotation"  # type: ignore[typeddict-item]

    r = get_users()
    r.apply_hints(columns={"invalid": {"data_type": "unk", "wassup": False}})  # type: ignore[typeddict-unknown-key, typeddict-item]
    with pytest.raises(DictValidationException):
        r.compute_table_schema()

    r = get_users()
    r.apply_hints(columns={"tags": {"x-second-extra": "x-second-annotation"}})  # type: ignore[typeddict-unknown-key]
    t = r.compute_table_schema()

    assert t["columns"]["tags"]["x-second-extra"] == "x-second-annotation"  # type: ignore[typeddict-item]
    # make sure column name was set
    assert t["columns"]["tags"]["name"] == "tags"


def test_apply_hints_columns() -> None:
    @dlt.resource(name="user", columns={"tags": {"data_type": "json", "primary_key": True}})
    def get_users():
        yield {"u": "u", "tags": [1, 2, 3]}

    users = get_users()
    assert users.columns == {"tags": {"data_type": "json", "name": "tags", "primary_key": True}}
    assert (
        cast(TTableSchemaColumns, users.columns)["tags"]
        == users.compute_table_schema()["columns"]["tags"]
    )

    # columns property can be changed in place
    cast(TTableSchemaColumns, users.columns)["tags"]["data_type"] = "text"
    assert users.compute_table_schema()["columns"]["tags"]["data_type"] == "text"

    # apply column definition - it should be merged with defaults
    users.apply_hints(
        columns={
            "tags": {"primary_key": False, "data_type": "text"},
            "things": new_column("things", nullable=False),
        }
    )
    assert cast(TTableSchemaColumns, users.columns)["tags"] == {
        "data_type": "text",
        "name": "tags",
        "primary_key": False,
    }
    assert cast(TTableSchemaColumns, users.columns)["things"] == {
        "name": "things",
        "nullable": False,
    }

    # delete columns by passing empty
    users.apply_hints(columns={})
    assert users.columns == {}


def test_apply_hints_reference() -> None:
    @dlt.resource(
        references=[
            {
                "columns": ["User ID", "user_name"],
                "referenced_table": "users",
                "referenced_columns": ["id", "name"],
            }
        ]
    )
    def campaigns():
        yield []

    table_schema = campaigns().compute_table_schema()
    assert table_schema["references"] == [
        {
            "columns": ["User ID", "user_name"],
            "referenced_table": "users",
            "referenced_columns": ["id", "name"],
        }
    ]


def test_columns_from_pydantic() -> None:
    class Columns(BaseModel):
        tags: List[str]
        name: Optional[str]

    @dlt.resource(name="user", columns=Columns)
    def get_users() -> Iterator[Dict[str, Any]]:
        yield None

    t = get_users().compute_table_schema()

    assert t["columns"]["tags"]["nullable"] is False
    assert t["columns"]["tags"]["data_type"] == "json"
    assert t["columns"]["name"]["nullable"] is True
    assert t["columns"]["name"]["data_type"] == "text"

    class Columns2(BaseModel):
        a: int
        b: str

    # New columns with apply_hints
    r = get_users()
    r.apply_hints(columns=Columns2)

    t = r.compute_table_schema()
    assert t["columns"]["a"]["nullable"] is False
    assert t["columns"]["a"]["data_type"] == "bigint"
    assert t["columns"]["b"]["nullable"] is False
    assert t["columns"]["b"]["data_type"] == "text"

    # Dynamic column hint
    class Columns3(BaseModel):
        a: List[int]
        b: float

    r = get_users()
    r.apply_hints(table_name=lambda item: "table", columns=lambda item: Columns3)
    t = r.compute_table_schema({})

    assert t["columns"]["a"]["nullable"] is False
    assert t["columns"]["a"]["data_type"] == "json"
    assert t["columns"]["b"]["nullable"] is False
    assert t["columns"]["b"]["data_type"] == "double"


def test_not_normalized_identifiers_in_hints() -> None:
    @dlt.resource(
        primary_key="ID",
        merge_key=["Month", "Day"],
        columns=[{"name": "Col1", "data_type": "bigint"}],
        table_name="🐫Camels",
    )
    def CamelResource():
        yield ["🐫"] * 10

    camels = CamelResource()
    # original names are kept
    assert camels.name == "CamelResource"
    assert camels.table_name == "🐫Camels"
    assert camels.columns == {"Col1": {"data_type": "bigint", "name": "Col1"}}
    table = camels.compute_table_schema()
    columns = table["columns"]
    assert "ID" in columns
    assert "Month" in columns
    assert "Day" in columns
    assert "Col1" in columns
    assert table["name"] == "🐫Camels"

    # define as part of a source
    camel_source = DltSource(Schema("snake_case"), "camel_section", [camels])
    schema = camel_source.discover_schema()
    # all normalized
    table = schema.get_table("_camels")
    columns = table["columns"]
    assert "id" in columns
    assert "month" in columns
    assert "day" in columns
    assert "col1" in columns


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
    os.environ[f"{known_sections.SOURCES.upper()}__SECTION_SOURCE__INIT_SOURCE_F_1__VAL"] = (
        "SECTION INIT_SOURCE_F_1 LEVEL"
    )
    assert list(init_source_f_1()) == ["SECTION INIT_SOURCE_F_1 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__SECTION_SOURCE__INIT_RESOURCE_F_2__VAL"] = (
        "SECTION INIT_RESOURCE_F_2 LEVEL"
    )
    assert list(init_resource_f_2()) == ["SECTION INIT_RESOURCE_F_2 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__NAME_OVERRIDDEN__SOURCE_F_1__VAL"] = (
        "NAME SOURCE_F_1 LEVEL"
    )
    assert list(source_f_1()) == ["NAME SOURCE_F_1 LEVEL"]
    os.environ[f"{known_sections.SOURCES.upper()}__NAME_OVERRIDDEN__RESOURCE_F_2__VAL"] = (
        "NAME RESOURCE_F_2 LEVEL"
    )
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
    from tests.extract.cases.section_source.external_resources import (
        with_external,
        with_bound_external,
        init_resource_f_2,
        resource_f_2,
    )

    # standalone resources must accept the injected sections for lookups
    os.environ["SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL"] = (
        "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL"
    )
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
        "SOURCES__EXTERNAL_RESOURCES__VAL",
    ]
    # this source will bind external resources before returning them (that is: calling them and obtaining generators)
    # the iterator in the source will force its sections so external resource sections are not used
    s = with_bound_external()
    assert list(s) == list(
        [
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL",
        ]
    )

    # inject the source sections like the Pipeline object would
    s = with_external()
    assert s.name == "with_external"
    assert s.section == "external_resources"  # from module name hosting the function
    with inject_section(
        ConfigSectionContext(
            pipeline_name="injected_external", sections=("sources", s.section, s.name)
        )
    ):
        # now the external sources must adopt the injected namespace
        assert (list(s)) == [
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL",
            "SOURCES__EXTERNAL_RESOURCES__VAL",
        ]

    # now with environ values that specify source/resource name: the module of the source, the name of the resource
    os.environ["SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL"] = (
        "SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL"
    )
    os.environ["SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL"] = (
        "SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL"
    )
    s = with_external()
    with inject_section(
        ConfigSectionContext(
            pipeline_name="injected_external", sections=("sources", s.section, s.name)
        )
    ):
        # now the external sources must adopt the injected namespace
        assert (list(s)) == [
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__SOURCE_VAL",
            "SOURCES__EXTERNAL_RESOURCES__INIT_RESOURCE_F_2__VAL",
            "SOURCES__EXTERNAL_RESOURCES__RESOURCE_F_2__VAL",
        ]


def test_source_schema_context() -> None:
    import dlt

    # global schema directly in the module
    global_schema = Schema("global")

    # not called from the source
    with pytest.raises(CurrentSourceSchemaNotAvailable):
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
        schema.update_table(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_ad_hoc(), "created_ad_hoc")

    # schema created directly
    @dlt.source(schema=Schema("explicit"))
    def created_explicit():
        schema = dlt.current.source_schema()
        assert schema.name == "explicit"
        # modify schema in place
        schema.update_table(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_explicit(), "explicit")

    # schema instance from a module
    @dlt.source(schema=global_schema)
    def created_global():
        schema = dlt.current.source_schema()
        assert schema.name == "global"
        # modify schema in place
        schema.update_table(new_table("source_table"))
        return dlt.resource([1, 2, 3], name="res")

    _assert_source_schema(created_global(), "global")


def test_source_schema_removes_processing_hints() -> None:
    eth_V9 = load_yml_case("schemas/eth/ethereum_schema_v9")
    assert "x-normalizer" in eth_V9["tables"]["blocks"]

    @dlt.source(schema=Schema.from_dict(eth_V9))
    def created_explicit():
        schema = dlt.current.source_schema()
        assert schema.name == "ethereum"
        assert "x-normalizer" not in schema.tables["blocks"]
        return dlt.resource([1, 2, 3], name="res")

    source = created_explicit()
    assert "x-normalizer" not in source.schema.tables["blocks"]


def test_source_state_context() -> None:
    @dlt.resource(selected=False)
    def main():
        state = dlt.current.state()
        mark = state.setdefault("mark", 1)
        # increase the multiplier each time state is obtained
        state["mark"] *= 2
        yield [1, 2, 3]
        assert dlt.state()["mark"] == mark * 2

    @dlt.transformer(data_from=main)
    def feeding(item):
        # we must have state
        assert dlt.current.source_state()["mark"] > 1
        mark = dlt.current.source_state()["mark"]
        yield from map(lambda i: i * mark, item)

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
    schema.update_table(new_table("table"))
    s = schema_test()
    assert "table" not in s.discover_schema().tables


@dlt.source(name="shorthand", section="shorthand")
def with_shorthand_registry(data):
    return dlt.resource(data, name="alpha")


@dlt.source
def test_decorators():
    return dlt.resource(["A", "B"], name="alpha")


@dlt.resource
def res_reg_with_secret(secretz: str = dlt.secrets.value):
    yield [secretz] * 3


def test_source_reference() -> None:
    # shorthand works when name  == section
    ref = SourceReference.from_reference("shorthand")
    assert list(ref(["A", "B"])) == ["A", "B"]
    ref = SourceReference.from_reference("shorthand.shorthand")
    assert list(ref(["A", "B"])) == ["A", "B"]
    # same for test_decorators
    ref = SourceReference.from_reference("test_decorators")
    assert list(ref()) == ["A", "B"]
    ref = SourceReference.from_reference("test_decorators.test_decorators")
    assert list(ref()) == ["A", "B"]

    # inner sources are registered
    @dlt.source
    def _inner_source():
        return dlt.resource(["C", "D"], name="beta")

    ref = SourceReference.from_reference("test_decorators._inner_source")
    assert list(ref()) == ["C", "D"]

    # duplicate section / name will replace registration
    @dlt.source(name="_inner_source")
    def _inner_source_2():
        return dlt.resource(["E", "F"], name="beta")

    ref = SourceReference.from_reference("test_decorators._inner_source")
    assert list(ref()) == ["E", "F"]

    # unknown reference
    with pytest.raises(UnknownSourceReference) as ref_ex:
        SourceReference.from_reference("$ref")
    assert ref_ex.value.ref == ["dlt.$ref.$ref"]

    @dlt.source(section="special")
    def absolute_config(init: int, mark: str = dlt.config.value, secret: str = dlt.secrets.value):
        # will need to bind secret
        return (res_reg_with_secret, dlt.resource([init, mark, secret], name="dump"))

    ref = SourceReference.from_reference("special.absolute_config")
    os.environ["SOURCES__SPECIAL__MARK"] = "ma"
    os.environ["SOURCES__SPECIAL__SECRET"] = "sourse"
    # resource when in source adopts source section
    os.environ["SOURCES__SPECIAL__RES_REG_WITH_SECRET__SECRETZ"] = "resourse"
    source = ref(init=100)
    assert list(source) == ["resourse", "resourse", "resourse", 100, "ma", "sourse"]


def test_source_reference_with_context() -> None:
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._name = "mock"
    ctx.context = mock

    with Container().injectable_context(ctx):
        # should be able to import things from dlt package
        ref = SourceReference.from_reference("shorthand")
        assert list(ref(["A", "B"])) == ["A", "B"]
        ref = SourceReference.from_reference("shorthand.shorthand")
        assert list(ref(["A", "B"])) == ["A", "B"]
        # unknown reference
        with pytest.raises(UnknownSourceReference) as ref_ex:
            SourceReference.from_reference("$ref")
        assert ref_ex.value.ref == ["mock.$ref.$ref", "dlt.$ref.$ref"]
        with pytest.raises(UnknownSourceReference) as ref_ex:
            SourceReference.from_reference("mock.$ref.$ref")
        assert ref_ex.value.ref == ["mock.$ref.$ref"]

        # create a "shorthand" source in this context
        @dlt.source(name="shorthand", section="shorthand")
        def with_shorthand_registry(data):
            return dlt.resource(list(reversed(data)), name="alpha")

        ref = SourceReference.from_reference("shorthand")
        assert list(ref(["C", "x"])) == ["x", "C"]
        ref = SourceReference.from_reference("mock.shorthand.shorthand")
        assert list(ref(["C", "x"])) == ["x", "C"]
        # from dlt package
        ref = SourceReference.from_reference("dlt.shorthand.shorthand")
        assert list(ref(["C", "x"])) == ["C", "x"]


def test_source_reference_from_module() -> None:
    ref = SourceReference.from_reference("tests.extract.test_decorators.with_shorthand_registry")
    assert list(ref(["C", "x"])) == ["C", "x"]

    # module exists but attr is not a factory
    with pytest.raises(UnknownSourceReference) as ref_ex:
        SourceReference.from_reference(
            "tests.extract.test_decorators.test_source_reference_from_module"
        )
    assert ref_ex.value.ref == ["tests.extract.test_decorators.test_source_reference_from_module"]

    # wrong module
    with pytest.raises(UnknownSourceReference) as ref_ex:
        SourceReference.from_reference(
            "test.extract.test_decorators.test_source_reference_from_module"
        )
    assert ref_ex.value.ref == ["test.extract.test_decorators.test_source_reference_from_module"]


def test_source_factory_with_args() -> None:
    # check typing - no type ignore below!
    factory = with_shorthand_registry.with_args
    # do not override anything
    source = factory()(data=["AXA"])
    assert list(source) == ["AXA"]

    # there are some overrides from decorator
    assert with_shorthand_registry.name == "shorthand"  # type: ignore
    assert with_shorthand_registry.section == "shorthand"  # type: ignore

    # with_args creates clones
    source_f_1: DltSourceFactoryWrapper[Any, DltSource] = factory(  # type: ignore
        max_table_nesting=1, root_key=True
    )
    source_f_2: DltSourceFactoryWrapper[Any, DltSource] = factory(  # type: ignore
        max_table_nesting=1, root_key=False, schema_contract="discard_value"
    )
    assert source_f_1 is not source_f_2

    # check if props are set
    assert source_f_1.name == source_f_2.name == "shorthand"
    assert source_f_1.section == source_f_2.section == "shorthand"
    assert source_f_1.max_table_nesting == source_f_2.max_table_nesting == 1
    assert source_f_1.root_key is True
    assert source_f_2.root_key is False
    assert source_f_2.schema_contract == "discard_value"

    # check if props are preserved when not set
    incompat_schema = Schema("incompat")
    with pytest.raises(ExplicitSourceNameInvalid):
        source_f_1.with_args(
            section="special", schema=incompat_schema, parallelized=True, schema_contract="evolve"
        )

    compat_schema = Schema("shorthand")
    compat_schema.tables["alpha"] = new_table("alpha")
    source_f_3 = source_f_1.with_args(
        section="special", schema=compat_schema, parallelized=True, schema_contract="evolve"
    )
    assert source_f_3.name == "shorthand"
    assert source_f_3.section == "special"
    assert source_f_3.max_table_nesting == 1
    assert source_f_3.root_key is True
    assert source_f_3.schema is compat_schema
    assert source_f_3.parallelized is True
    assert source_f_3.schema_contract == "evolve"
    source_f_3 = source_f_3.with_args()
    assert source_f_3.name == "shorthand"
    assert source_f_3.section == "special"
    assert source_f_3.max_table_nesting == 1
    assert source_f_3.root_key is True
    assert source_f_3.schema is compat_schema
    assert source_f_3.parallelized is True
    assert source_f_3.schema_contract == "evolve"

    # create source
    source = source_f_3(["A", "X"])
    assert source.root_key is True
    assert source.schema.tables["alpha"] == compat_schema.tables["alpha"]
    assert source.name == "shorthand"
    assert source.section == "special"
    assert source.max_table_nesting == 1
    assert source.schema_contract == "evolve"

    # when section / name are changed, config location follows
    @dlt.source
    def absolute_config(init: int, mark: str = dlt.config.value, secret: str = dlt.secrets.value):
        # will need to bind secret
        return (res_reg_with_secret, dlt.resource([init, mark, secret], name="dump"))

    absolute_config = absolute_config.with_args(name="absolute", section="special")
    os.environ["SOURCES__SPECIAL__ABSOLUTE__MARK"] = "ma"
    os.environ["SOURCES__SPECIAL__ABSOLUTE__SECRET"] = "sourse"
    # resource when in source adopts source section
    os.environ["SOURCES__SPECIAL__RES_REG_WITH_SECRET__SECRETZ"] = "resourse"
    source = absolute_config(init=100)
    assert list(source) == ["resourse", "resourse", "resourse", 100, "ma", "sourse"]


@dlt.resource
def standalone_resource(secret=dlt.secrets.value, config=dlt.config.value, opt: str = "A"):
    yield 1


def test_spec_generation() -> None:
    # inner resource cannot take configuration

    with pytest.raises(ResourceInnerCallableConfigWrapDisallowed) as py_ex:

        @dlt.resource(write_disposition="merge", primary_key="id")
        def inner_resource(initial_id=dlt.config.value):
            yield [{"id": 1, "name": "row1"}, {"id": 1, "name": "row2"}]

    assert py_ex.value.resource_name == "inner_resource"

    # outer resource does not take default params
    SPEC = get_fun_spec(standalone_resource._pipe.gen)  # type: ignore[arg-type]
    fields = SPEC.get_resolvable_fields()

    # args with defaults are ignored
    assert len(fields) == 2
    assert "secret" in fields
    assert "config" in fields

    @dlt.resource(standalone=True)
    def inner_standalone_resource(
        secret=dlt.secrets.value, config=dlt.config.value, opt: str = "A"
    ):
        yield 1

    SPEC = get_fun_spec(inner_standalone_resource("TS", "CFG")._pipe.gen)  # type: ignore[arg-type]
    fields = SPEC.get_resolvable_fields()
    # resources marked as standalone always inject full signature
    assert len(fields) == 3
    assert {"secret", "config", "opt"} == set(fields.keys())

    @dlt.source
    def inner_source(secret=dlt.secrets.value, config=dlt.config.value, opt: str = "A"):
        return standalone_resource

    SPEC = SourceReference.find("test_decorators.inner_source").SPEC
    fields = SPEC.get_resolvable_fields()
    assert {"secret", "config", "opt"} == set(fields.keys())


@dlt.resource
def not_args_r():
    yield from [1, 2, 3]


def test_sources_no_arguments() -> None:
    @dlt.source
    def no_args():
        return dlt.resource([1, 2], name="data")

    # there is a spec even if no arguments
    SPEC = SourceReference.find("dlt.test_decorators.no_args").SPEC
    assert SPEC

    # source names are used to index detected sources
    _, _, checked = detect_source_configs(SourceReference.SOURCES, "", ())
    assert "no_args" in checked

    SPEC = SourceReference.find("dlt.test_decorators.not_args_r").SPEC
    assert SPEC
    _, _, checked = detect_source_configs(SourceReference.SOURCES, "", ())
    assert "not_args_r" in checked

    @dlt.resource
    def not_args_r_i():
        yield from [1, 2, 3]

    assert "dlt.test_decorators.not_args_r_i" not in SourceReference.SOURCES

    # you can call those
    assert list(no_args()) == [1, 2]
    assert list(not_args_r()) == [1, 2, 3]
    assert list(not_args_r_i()) == [1, 2, 3]


def test_resource_sets_invalid_write_disposition() -> None:
    @dlt.resource(write_disposition="xxxx")  # type: ignore[call-overload]
    def invalid_disposition():
        yield from [1, 2, 3]

    r = invalid_disposition()
    with pytest.raises(DictValidationException) as py_ex:
        r.compute_table_schema()
    assert "write_disposition" in str(py_ex.value)


def test_custom_source_impl() -> None:
    class TypedSource(DltSource):
        def users(self, mode: str) -> DltResource:
            return self.resources["users"](mode)

    @dlt.source(_impl_cls=TypedSource)
    def all_users():
        @dlt.resource
        def users(mode: str):
            yield mode

        return users

    s = all_users()
    assert isinstance(s, TypedSource)
    assert list(s.users("group")) == ["group"]


class TypedResource(DltResource):
    def __call__(
        self: "TypedResource", api_key: dlt.TSecretValue = dlt.secrets.value, limit: int = 10
    ) -> "TypedResource":
        """Pass api key and limit"""
        return super().__call__(api_key, limit)


@dlt.resource(_impl_cls=TypedResource)
def inner_r(api_key: dlt.TSecretValue = dlt.secrets.value, limit: int = 10):
    yield from ["A"] * limit


def test_custom_resource_impl() -> None:
    inn_r = inner_r(dlt.TSecretValue("key"), limit=3)
    assert isinstance(inn_r, TypedResource)
    assert list(inn_r) == ["A"] * 3

    @dlt.resource(_impl_cls=TypedResource, standalone=True)
    def inner_standalone(api_key: dlt.TSecretValue = dlt.secrets.value, limit: int = 10):
        yield from range(1, limit + 1)

    std_r = inner_standalone(dlt.TSecretValue("key"), limit=4)
    assert isinstance(std_r, TypedResource)
    assert list(std_r) == [1, 2, 3, 4]


# wrapped flag will not create the resource but just simple function wrapper that must be called before use
@dlt.resource(standalone=True)
def standalone_signature(init: int, secret_end: int = dlt.secrets.value):
    """Has fine docstring"""
    yield from range(init, secret_end)


@dlt.resource
def regular_signature(init: int, secret_end: int = dlt.secrets.value):
    yield from range(init, secret_end)


def test_standalone_resource() -> None:
    # wrapped flag will not create the resource but just simple function wrapper that must be called before use
    @dlt.resource(standalone=True)
    def nice_signature(init: int):
        """Has nice signature"""
        yield from range(init, 10)

    assert not isinstance(nice_signature, DltResource)
    assert callable(nice_signature)
    assert nice_signature.__doc__ == """Has nice signature"""

    assert list(nice_signature(7)) == [7, 8, 9]
    assert nice_signature(8).args_bound is True
    with pytest.raises(TypeError):
        # bound!
        nice_signature(7)()

    # can't work in a source

    @dlt.source
    def nice_source():
        return nice_signature

    source = nice_source()
    source.nice_signature.bind(7)
    with pytest.raises(PipeGenInvalid):
        assert list(source) == [7, 8, 9]

    @dlt.source
    def many_instances():
        return nice_signature(9), nice_signature(7)

    with pytest.raises(InvalidResourceDataTypeMultiplePipes):
        source = many_instances()

    with pytest.raises(ConfigFieldMissingException):
        list(standalone_signature(1))

    # use wrong signature
    with pytest.raises(TypeError):
        nice_signature(unk_kw=1, second_unk_kw="A")  # type: ignore

    # make sure that config sections work
    os.environ["SOURCES__TEST_DECORATORS__STANDALONE_SIGNATURE__SECRET_END"] = "5"
    assert list(standalone_signature(1)) == [1, 2, 3, 4]


@pytest.mark.parametrize("res", (standalone_signature, regular_signature))
def test_reference_registered_resource(res: DltResource) -> None:
    if isinstance(res, DltResource):
        ref = res(1, 2).name
        # find reference
        res_ref = SourceReference.find(f"test_decorators.{ref}")
        assert res_ref.SPEC is res.SPEC
    else:
        ref = res.__name__
    # create source with single res.
    factory = SourceReference.from_reference(f"test_decorators.{ref}")
    # pass explicit config
    source = factory(init=1, secret_end=3)
    assert source.name == ref
    assert source.section == ""
    assert ref in source.resources
    assert list(source) == [1, 2]

    # use regular config
    os.environ[f"SOURCES__TEST_DECORATORS__{ref.upper()}__SECRET_END"] = "5"
    source = factory(init=1)
    assert list(source) == [1, 2, 3, 4]

    # use config with override
    # os.environ["SOURCES__SECTION__SIGNATURE__INIT"] = "-1"
    os.environ["SOURCES__SECTION__SIGNATURE__SECRET_END"] = "7"
    source = factory.with_args(
        name="signature",
        section="section",
        max_table_nesting=1,
        root_key=True,
        schema_contract="freeze",
        parallelized=True,
    )(-1)
    assert list(source) == [-1, 0, 1, 2, 3, 4, 5, 6]
    # use renamed name
    resource = source.signature
    assert resource.section == "section"
    assert resource.name == "signature"
    assert resource.max_table_nesting == 1
    assert resource.schema_contract == "freeze"


def test_inner_resource_not_registered() -> None:
    # inner resources are not registered
    @dlt.resource(standalone=True)
    def inner_data_std():
        yield [1, 2, 3]

    with pytest.raises(UnknownSourceReference):
        SourceReference.from_reference("test_decorators.inner_data_std")

    @dlt.resource()
    def inner_data_reg():
        yield [1, 2, 3]

    with pytest.raises(UnknownSourceReference):
        SourceReference.from_reference("test_decorators.inner_data_reg")


@dlt.transformer(standalone=True)
def standalone_transformer(item: TDataItem, init: int, secret_end: int = dlt.secrets.value):
    """Has fine transformer docstring"""
    yield from range(item + init, secret_end)


@dlt.transformer
def regular_transformer(item: TDataItem, init: int, secret_end: int = dlt.secrets.value):
    yield from range(item + init, secret_end)


@dlt.transformer(standalone=True)
def standalone_transformer_returns(item: TDataItem, init: int = dlt.config.value):
    """Has fine transformer docstring"""
    return "A" * item * init


@pytest.mark.parametrize("ref", ("standalone_transformer", "regular_transformer"))
def test_reference_registered_transformer(ref: str) -> None:
    factory = SourceReference.from_reference(f"test_decorators.{ref}")
    bound_tx = standalone_signature(1, 3) | factory(5, 10).resources.detach()
    print(bound_tx)
    assert list(bound_tx) == [6, 7, 7, 8, 8, 9, 9]

    # use regular config
    os.environ[f"SOURCES__TEST_DECORATORS__{ref.upper()}__SECRET_END"] = "7"
    bound_tx = standalone_signature(1, 3) | factory(5).resources.detach()
    assert list(bound_tx) == [6]

    # use config with override
    os.environ["SOURCES__SECTION__SIGNATURE__SECRET_END"] = "8"
    source = factory.with_args(
        name="signature",
        section="section",
        max_table_nesting=1,
        root_key=True,
        schema_contract="freeze",
        parallelized=True,
    )(5)
    bound_tx = standalone_signature(1, 3) | source.resources.detach()
    assert list(bound_tx) == [6, 7, 7]


@pytest.mark.parametrize("next_item_mode", ["fifo", "round_robin"])
def test_standalone_transformer(next_item_mode: str) -> None:
    os.environ["EXTRACT__NEXT_ITEM_MODE"] = next_item_mode

    assert not isinstance(standalone_transformer, DltResource)
    assert callable(standalone_transformer)
    assert standalone_transformer.__doc__ == """Has fine transformer docstring"""

    bound_tx = standalone_transformer(5, 10)
    # this is not really true
    assert bound_tx.args_bound is True
    with pytest.raises(TypeError):
        bound_tx(1)
    assert isinstance(bound_tx, DltResource)
    # the resource sets the start of the range of transformer + transformer init
    exp_result = [6, 7, 7, 8, 8, 9, 9] if next_item_mode == "round_robin" else [6, 7, 8, 9, 7, 8, 9]
    assert list(standalone_signature(1, 3) | bound_tx) == exp_result

    # wrong params to transformer
    with pytest.raises(TypeError):
        standalone_transformer(unk_kw="ABC")  # type: ignore

    # test transformer that returns
    bound_tx = standalone_transformer_returns(2)
    assert list(standalone_signature(1, 3) | bound_tx) == ["AA", "AAAA"]

    # test configuration
    os.environ["SOURCES__TEST_DECORATORS__STANDALONE_SIGNATURE__SECRET_END"] = "5"
    os.environ["SOURCES__TEST_DECORATORS__STANDALONE_TRANSFORMER_RETURNS__INIT"] = "2"
    assert list(standalone_signature(1) | standalone_transformer_returns()) == [
        "AA",
        "AAAA",
        "AAAAAA",
        "AAAAAAAA",
    ]


def test_transformer_required_args() -> None:
    @dlt.transformer
    def path_params(id_, workspace_id, load_id, base: bool = False):
        yield {"id": id_, "workspace_id": workspace_id, "load_id": load_id}

    data = list([1, 2, 3] | path_params(121, 343))
    assert len(data) == 3
    assert data[0] == {"id": 1, "workspace_id": 121, "load_id": 343}

    # @dlt


@dlt.transformer(standalone=True, name=lambda args: args["res_name"])
def standalone_tx_with_name(item: TDataItem, res_name: str, init: int = dlt.config.value):
    return res_name * item * init


def test_standalone_resource_with_name() -> None:
    my_tx = standalone_tx_with_name("my_tx")
    assert my_tx.section == "test_decorators"
    assert my_tx.name == "my_tx"

    # config uses the actual resource name (my_tx)
    os.environ["SOURCES__TEST_DECORATORS__MY_TX__INIT"] = "2"
    assert list(dlt.resource([1, 2, 3], name="x") | my_tx) == [
        "my_txmy_tx",
        "my_txmy_txmy_txmy_tx",
        "my_txmy_txmy_txmy_txmy_txmy_tx",
    ]

    with pytest.raises(DynamicNameNotStandaloneResource):

        @dlt.resource(standalone=False, name=lambda args: args["res_name"])  # type: ignore[call-overload]
        def standalone_name():
            yield "A"

    # we looks for non existing argument in lambda
    @dlt.resource(standalone=True, name=lambda args: args["res_name"])
    def standalone_name_2(_name: str):
        yield "A"

    # so resource will not instantiate
    with pytest.raises(KeyError):
        standalone_name_2("_N")


def test_standalone_resource_returns() -> None:
    @dlt.resource(standalone=True)
    def rv_data(name: str):
        return [name] * 10

    with pytest.raises(InvalidResourceDataTypeFunctionNotAGenerator):
        rv_data("returned")


def test_standalone_resource_returning_resource() -> None:
    @dlt.resource(standalone=True)
    def rv_resource(name: str):
        return dlt.resource([1, 2, 3], name=name, primary_key="value")

    r = rv_resource("returned")
    assert r.name == "returned"
    assert r.compute_table_schema()["columns"]["value"]["primary_key"] is True
    assert list(r) == [1, 2, 3]


def test_standalone_resource_returning_resource_exception() -> None:
    @dlt.resource(standalone=True)
    def rv_resource(uniq_name: str = dlt.config.value):
        return dlt.resource([1, 2, 3], name=uniq_name, primary_key="value")

    # pass through of the exception in `rv_resource` when it returns, not yields
    with pytest.raises(ConfigFieldMissingException) as conf_ex:
        rv_resource()
    assert conf_ex.value.fields == ["uniq_name"]


def test_standalone_resource_rename_credentials_separation():
    os.environ["SOURCES__TEST_DECORATORS__STANDALONE_SIGNATURE__SECRET_END"] = "5"
    assert list(standalone_signature(1)) == [1, 2, 3, 4]

    # os.environ["SOURCES__TEST_DECORATORS__RENAMED_SIG__SECRET_END"] = "6"
    # assert list(standalone_signature.with_name("renamed_sig")(1)) == [1, 2, 3, 4, 5]

    # bound resource will not allow for reconfig
    assert list(standalone_signature(1).with_name("renamed_sig")) == [1, 2, 3, 4]


def test_resource_rename_credentials_separation():
    os.environ["SOURCES__TEST_DECORATORS__REGULAR_SIGNATURE__SECRET_END"] = "5"
    assert list(regular_signature(1)) == [1, 2, 3, 4]

    os.environ["SOURCES__TEST_DECORATORS__RENAMED_SIG__SECRET_END"] = "6"
    assert list(regular_signature.with_name("renamed_sig")(1)) == [1, 2, 3, 4, 5]

    # bound resource will not allow for reconfig
    assert list(regular_signature(1).with_name("renamed_sig")) == [1, 2, 3, 4]


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
    assert list(s) == ["A", "V", "A", "V", "A", "V", "A", "V"]

    # CAN'T decorate classes themselves
    with pytest.raises(SourceIsAClassTypeError):

        @dlt.source(name="planB")
        class _SourceB:
            def __init__(self, elems: int) -> None:
                self.elems = elems

            def __call__(self, more: int = 1):
                return dlt.resource(["A", "V"] * self.elems * more, name="_list")


@pytest.mark.asyncio
async def test_async_source() -> None:
    @dlt.source
    async def source_rv_no_parens(reverse: bool = False):
        # test is expected context is present
        dlt.current.state()
        dlt.current.source_schema()
        data = [1, 2, 3]
        if reverse:
            data = list(reversed(data))
        return dlt.resource(data, name="data")

    @dlt.source(name="with_parens")
    async def source_rv_with_parens(reverse: bool = False):
        # test is expected context is present
        dlt.current.state()
        dlt.current.source_schema()
        data = [4, 5, 6]
        if reverse:
            data = list(reversed(data))
        return dlt.resource(data, name="data")

    @dlt.source(name="with_parens")
    async def source_yield_with_parens(reverse: bool = False):
        # test is expected context is present
        dlt.current.state()
        dlt.current.source_schema()
        data = [7, 8, 9]
        if reverse:
            data = list(reversed(data))
        return dlt.resource(data, name="data")

    # create a pipeline so current.state() works
    dlt.pipeline("async_state_pipeline")

    async def _assert_source(source_coro_f, expected_data) -> None:
        # test various forms of source decorator, parens, no parens, yield, return
        source_coro = source_coro_f()
        assert inspect.iscoroutine(source_coro)
        source = await source_coro
        assert "data" in source.resources
        assert list(source) == expected_data

        # make sure the config injection works
        with custom_environ(
            {f"SOURCES__{source.section.upper()}__{source.name.upper()}__REVERSE": "True"}
        ):
            assert list(await source_coro_f()) == list(reversed(expected_data))

    await _assert_source(source_rv_no_parens, [1, 2, 3])
    await _assert_source(source_rv_with_parens, [4, 5, 6])
    await _assert_source(source_yield_with_parens, [7, 8, 9])


@pytest.mark.skip("Not implemented")
def test_class_resource() -> None:
    pass


def test_parallelized_resource_decorator() -> None:
    """Test parallelized resources are wrapped correctly.
    Note: tests for parallel execution are in test_resource_evaluation
    """

    def some_gen():
        yield from [1, 2, 3]

    # Create resource with decorated function
    resource = dlt.resource(some_gen, parallelized=True)

    # Generator func is wrapped with parallelized gen that yields callables
    gen = resource._pipe.gen()  # type: ignore
    result = next(gen)  # type: ignore[arg-type]
    assert result() == 1
    assert list(resource) == [1, 2, 3]

    # Same but wrapping generator directly
    resource = dlt.resource(some_gen(), parallelized=True)

    result = next(resource._pipe.gen)  # type: ignore
    assert result() == 1
    # get remaining items
    assert list(resource) == [2, 3]

    # Wrap a yielding transformer
    def some_tx(item):
        yield item + 1

    resource = dlt.resource(some_gen, parallelized=True)

    transformer = dlt.transformer(some_tx, parallelized=True, data_from=resource)
    pipe_gen = transformer._pipe.gen
    # Calling transformer returns the parallel wrapper generator
    inner = pipe_gen(1)  # type: ignore
    assert next(inner)() == 2  # type: ignore
    assert list(transformer) == [2, 3, 4]  # add 1 to resource

    # Wrap a transformer function
    def some_tx_func(item):
        return list(range(item))

    transformer = dlt.transformer(some_tx_func, parallelized=True, data_from=resource)
    pipe_gen = transformer._pipe.gen
    inner = pipe_gen(3)  # type: ignore
    # this is a regular function returning list
    assert inner() == [0, 1, 2]  # type: ignore[operator]
    assert list(transformer) == [0, 0, 1, 0, 1, 2]

    # Invalid parallel resources

    # From async generator
    with pytest.raises(InvalidParallelResourceDataType):

        @dlt.resource(parallelized=True)
        async def some_data():
            yield 1
            yield 2

    # From list
    with pytest.raises(InvalidParallelResourceDataType):
        dlt.resource([1, 2, 3], name="T", parallelized=True)

    # Test that inner generator is closed when wrapper is closed
    gen_orig = some_gen()
    resource = dlt.resource(gen_orig, parallelized=True)
    gen = resource._pipe.gen

    next(gen)  # type: ignore
    gen.close()  # type: ignore

    with pytest.raises(StopIteration):
        # Inner generator is also closed
        next(gen_orig)
