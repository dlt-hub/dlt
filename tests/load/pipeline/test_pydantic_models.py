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

    @dlt.resource
    def res():
        yield [example_data]

    @dlt.source(max_table_nesting=1)
    def src():
        yield res()

    p = dlt.pipeline("example", full_refresh=True, destination="duckdb")
    p.run(src(), table_name="items", columns=Parent)

    # in the parent this works
    assert "optional_parent_attribute" in p.default_schema.tables["items"]["columns"].keys()

    # this is here (because it is in the data)
    assert "child__child_attribute" in p.default_schema.tables["items"]["columns"].keys()

    # this is defined in the pydantic schema but does not end up in the resulting schema
    assert "child__optional_child_attribute" in p.default_schema.tables["items"]["columns"].keys()
