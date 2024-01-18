import dlt
import typing as t

from pydantic import BaseModel
from dlt.common.libs.pydantic import DltConfig


def test_flattened_model() -> None:
    class Child(BaseModel):
        child_attribute: str
        optional_child_attribute: t.Optional[str] = None

    class Parent(BaseModel):
        child: Child
        optional_parent_attribute: t.Optional[str] = None
        dlt_config: t.ClassVar[DltConfig] = {"skip_complex_types": True}

    example_data = {
        "optional_parent_attribute": None,
        "child": {"child_attribute": "any string", "optional_child_attribute": None},
    }

    p = dlt.pipeline("example", full_refresh=True, destination="duckdb")
    p.run([example_data], table_name="items", columns=Parent)

    # in the parent this works
    assert "optional_parent_attribute" in p.default_schema.tables["items"]["columns"].keys()

    # this is here (because it is in the data)
    assert "child__child_attribute" in p.default_schema.tables["items"]["columns"].keys()

    # this is defined in the pydantic schema but does not end up in the resulting schema
    assert "child__optional_child_attribute" in p.default_schema.tables["items"]["columns"].keys()


def test_list_model() -> None:
    class Child(BaseModel):
        child_attribute: str
        optional_child_attribute: t.Optional[str] = None

    class Parent(BaseModel):
        children: t.List[Child]
        optional_parent_attribute: t.Optional[str] = None
        dlt_config: t.ClassVar[DltConfig] = {"skip_complex_types": True}

    example_data = {
        "optidonal_parent_attribute": None,
        "children": [{"child_attribute": "any string", "optional_child_attribute": None}],
    }

    p = dlt.pipeline("items", full_refresh=True, destination="duckdb")
    p.run([example_data], table_name="items", columns=Parent)

    # in the parent this works
    assert "optional_parent_attribute" in p.default_schema.tables["items"]["columns"].keys()

    # we now now have a child table
    assert "items__children" in p.default_schema.tables.keys()

    # but it does not have the right columns
    assert (
        "child_attribute" in p.default_schema.tables["items__children"]["columns"].keys()
    )  # works
    assert (
        "optional_child_attribute" in p.default_schema.tables["items__children"]["columns"].keys()
    )  # fails
