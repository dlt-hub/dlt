import dlt
import pytest


@pytest.fixture()
def nested_resource():
    resource_name = "nested_data"
    yield dlt.resource(
        [
            {
                "id": 1,
                "outer1": [
                    {"outer1_id": "2", "innerfoo": [{"innerfoo_id": "3"}]},
                ],
                "outer2": [
                    {"outer2_id": "4", "innerbar": [{"innerbar_id": "5"}]},
                ],
            }
        ],
        name=resource_name
    )


def test_apply_nested_hints_column_types(nested_resource) -> None:
    """Apply hints to a specific path, outside of the resource definition
    Check for level 1 depth and level 1+ depth; should cover recursive cases
    """
    outer1_id_new_type = "double"
    outer2_innerbar_id_new_type = "bigint"

    nested_resource.apply_hints(
        path=["outer1"],
        columns=[
            {"name": "outer1_id", "data_type": outer1_id_new_type}
        ],
    )
    nested_resource.apply_hints(
        path=["outer2", "innerbar"],
        columns=[
            {"name": "innerbar_id", "data_type": outer2_innerbar_id_new_type},
        ],
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints",
        destination="duckdb",
        dev_mode=True,
    )
    pipeline.run(nested_resource)

    schema_tables = pipeline.default_schema.tables
    assert schema_tables["nested_data__outer1"]["parent"] == "nested_data"
    assert schema_tables["nested_data__outer1"]["columns"]["outer1_id"]["data_type"] == outer1_id_new_type
    assert schema_tables["nested_data__outer2__innerbar"]["parent"] == "nested_data__outer2"
    assert schema_tables["nested_data__outer2__innerbar"]["columns"]["innerbar_id"]["data_type"] == outer2_innerbar_id_new_type


def test_apply_nested_hints_override_parent(nested_resource) -> None:
    """IMPORTANT this tests that the hints are properly applied to the schema.
    It doesn't test if data is loaded in `override_parent` instead of the initial table_name.
    """
    root_table_new_name = "override_parent"

    nested_resource.apply_hints(table_name=root_table_new_name)
    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints_2",
        destination="duckdb",
        dev_mode=True,
    )
    pipeline.run(nested_resource)

    schema_tables = pipeline.default_schema.tables
    assert not any(key.startswith("nested_data") for key in schema_tables.keys())
    assert schema_tables[f"{root_table_new_name}__outer1"]["parent"] == f"{root_table_new_name}"
    assert schema_tables[f"{root_table_new_name}__outer1__innerfoo"]["parent"] == f"{root_table_new_name}__outer1"
    assert schema_tables[f"{root_table_new_name}__outer1__innerfoo"]["name"] == f"{root_table_new_name}__outer1__innerfoo"
    assert schema_tables[f"{root_table_new_name}__outer1__innerfoo"]["columns"]["innerfoo_id"]["data_type"] == "text"


def test_apply_hints_nested_hints_override_child(nested_resource):
    """Override both levels of child tables to cover recursive case
    
    IMPORTANT this tests that the hints are properly applied to the schema.
    It doesn't test if data is loaded in `override_child_outer1` instead of the initial table_name.
    """
    outer1_new_name = "override_child_outer1"
    outer1_innerfoo_new_name = "override_child_outer1_innerfoo"

    nested_resource.apply_hints(path=["outer1"], table_name=outer1_new_name)
    nested_resource.apply_hints(path=["outer1", "innerfoo"], table_name=outer1_innerfoo_new_name)
    pipeline = dlt.pipeline(
        pipeline_name="test_apply_hints_nested_hints_3",
        destination="duckdb",
        dev_mode=True, 
    )
    pipeline.run(nested_resource)

    schema_tables = pipeline.default_schema.tables
    assert schema_tables[outer1_new_name]["name"] == outer1_new_name
    assert schema_tables[outer1_innerfoo_new_name]["parent"] == outer1_new_name
    assert schema_tables[outer1_innerfoo_new_name]["name"] == outer1_innerfoo_new_name
