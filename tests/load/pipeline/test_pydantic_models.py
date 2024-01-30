import typing as t

import dlt

from pydantic import BaseModel
from dlt.common.libs.pydantic import DltConfig


class Child(BaseModel):
    child_attribute: str
    optional_child_attribute: t.Optional[str] = None


def test_flattens_model_when_skip_complex_types_is_set() -> None:
    class Parent(BaseModel):
        child: Child
        optional_parent_attribute: t.Optional[str] = None
        dlt_config: t.ClassVar[DltConfig] = {"skip_complex_types": True}

    example_data = {
        "optional_parent_attribute": None,
        "child": {
            "child_attribute": "any string",
            "optional_child_attribute": None,
        },
    }

    @dlt.resource
    def res():
        yield [example_data]

    @dlt.source(max_table_nesting=1)
    def src():
        yield res()

    p = dlt.pipeline("example", full_refresh=True, destination="duckdb")
    p.run(src(), table_name="items", columns=Parent)

    keys = p.default_schema.tables["items"]["columns"].keys()
    assert keys == {
        "child__child_attribute",
        "child__optional_child_attribute",
        "optional_parent_attribute",
        "_dlt_load_id",
        "_dlt_id",
    }

    columns = p.default_schema.tables["items"]["columns"]
    assert columns["child__child_attribute"] == {
        "name": "child__child_attribute",
        "data_type": "text",
        "nullable": False,
    }

    assert columns["child__optional_child_attribute"] == {
        "name": "child__optional_child_attribute",
        "data_type": "text",
        "nullable": True,
    }

    assert columns["optional_parent_attribute"] == {
        "name": "optional_parent_attribute",
        "data_type": "text",
        "nullable": True,
    }


def test_flattens_model_when_skip_complex_types_is_not_set():
    class Parent(BaseModel):
        child: Child
        optional_parent_attribute: t.Optional[str] = None
        data_dictionary: t.Dict[str, t.Any] = None
        dlt_config: t.ClassVar[DltConfig] = {"skip_complex_types": False}

    example_data = {
        "optional_parent_attribute": None,
        "data_dictionary": {
            "child_attribute": "any string",
        },
        "child": {
            "child_attribute": "any string",
            "optional_child_attribute": None,
        },
    }

    @dlt.resource
    def res():
        yield [example_data]

    @dlt.source(max_table_nesting=1)
    def src():
        yield res()

    p = dlt.pipeline("example", full_refresh=True, destination="duckdb")
    p.run(src(), table_name="items", columns=Parent)

    keys = p.default_schema.tables["items"]["columns"].keys()
    assert keys == {
        "child",
        "optional_parent_attribute",
        "data_dictionary",
        "_dlt_load_id",
        "_dlt_id",
    }

    columns = p.default_schema.tables["items"]["columns"]
    assert columns["child"] == {
        "name": "child",
        "data_type": "complex",
        "nullable": False,
    }

    assert columns["optional_parent_attribute"] == {
        "name": "optional_parent_attribute",
        "data_type": "text",
        "nullable": True,
    }

    assert columns["data_dictionary"] == {
        "name": "data_dictionary",
        "data_type": "complex",
        "nullable": False,
    }
