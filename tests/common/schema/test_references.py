import pytest
import duckdb

import dlt
from dlt.common.schema.utils import (
    create_root_child_reference,
    get_all_root_child_references_from_root,
    create_parent_child_reference,
    get_all_parent_child_references_from_root,
    create_load_table_reference,
    create_version_and_loads_hash_reference,
    create_version_and_loads_schema_name_reference,
)

from tests.utils import preserve_environ, patch_home_dir, autouse_test_storage


@pytest.fixture(scope="module")
def schema():
    @dlt.resource(
        references=[
            {"columns": ["key1"], "referenced_table": "root_table2", "referenced_columns": ["key2"]}
        ]
    )
    def root_table1():
        child_data2 = [
            {"id": 100, "depth1": "foo"},
            {"id": 111, "depth1": "bar"},
        ]
        child_data1 = [
            {"id": 10, "depth1": "foo", "child2": child_data2},
            {"id": 11, "depth1": "bar", "child2": child_data2},
        ]
        data = [
            {"id": 0, "key1": 10.0, "child1": child_data1},
            {"id": 1, "key1": 11.0, "child1": child_data1},
        ]
        yield from data

    @dlt.resource
    def root_table2():
        data = [
            {"id": 2, "key2": 10.0},
            {"id": 3, "key2": 10.0},
            {"id": 4, "key2": 11.0},
            {"id": 5, "key2": 11.0},
        ]
        yield from data

    @dlt.source(root_key=True)
    def mock_source():
        return [root_table1(), root_table2()]

    destination = dlt.destinations.duckdb(duckdb.connect())
    pipeline = dlt.pipeline("schema_mock", destination=destination)

    pipeline.run(mock_source())

    return pipeline.default_schema


EXPECTED_NESTED_REF1 = {
    "label": "_dlt_root",
    "cardinality": "many_to_one",
    "table": "root_table1__child1",
    "columns": ["_dlt_root_id"],
    "referenced_table": "root_table1",
    "referenced_columns": ["_dlt_id"],
}
EXPECTED_NESTED_REF2 = {
    "label": "_dlt_root",
    "cardinality": "many_to_one",
    "table": "root_table1__child1__child2",
    "columns": ["_dlt_root_id"],
    "referenced_table": "root_table1",
    "referenced_columns": ["_dlt_id"],
}

EXPECTED_CHILD1_TO_ROOT1_REF = {
    "label": "_dlt_parent",
    "cardinality": "many_to_one",
    "table": "root_table1__child1",
    "columns": ["_dlt_parent_id"],
    "referenced_table": "root_table1",
    "referenced_columns": ["_dlt_id"],
}
EXPECTED_CHILD2_TO_CHILD1_REF = {
    "label": "_dlt_parent",
    "cardinality": "many_to_one",
    "table": "root_table1__child1__child2",
    "columns": ["_dlt_parent_id"],
    "referenced_table": "root_table1__child1",
    "referenced_columns": ["_dlt_id"],
}


EXPECTED_ROOT1_TO_LOAD_REF = {
    "label": "_dlt_load",
    "cardinality": "zero_to_many",
    "table": "root_table1",
    "columns": ["_dlt_load_id"],
    "referenced_table": "_dlt_loads",
    "referenced_columns": ["load_id"],
}
EXPECTED_ROOT2_TO_LOAD_REF = {
    "label": "_dlt_load",
    "cardinality": "zero_to_many",
    "table": "root_table2",
    "columns": ["_dlt_load_id"],
    "referenced_table": "_dlt_loads",
    "referenced_columns": ["load_id"],
}


def test_root_child_reference_from_root(schema: dlt.Schema) -> None:
    """`create_root_child_reference()` on root table should fail.

    Should fail whether the table has children or not.
    """
    with pytest.raises(ValueError) as e:
        create_root_child_reference(schema.tables, "root_table1")
    assert e.match("Table `root_table1` is already a root table.")

    with pytest.raises(ValueError) as e:
        create_root_child_reference(schema.tables, "root_table2")
    assert e.match("Table `root_table2` is already a root table.")


def test_root_child_reference_from_child(schema: dlt.Schema) -> None:
    """`create_root_child_reference()` is not symmetric. It requires the
    root table name as input.
    """
    nested_ref1 = create_root_child_reference(schema.tables, "root_table1__child1")
    nested_ref2 = create_root_child_reference(schema.tables, "root_table1__child1__child2")

    assert nested_ref1 == EXPECTED_NESTED_REF1
    assert nested_ref2 == EXPECTED_NESTED_REF2
    assert all(ref.get("label") == "_dlt_root" for ref in [nested_ref1, nested_ref2])


def test_get_all_root_child_references_from_root(schema: dlt.Schema) -> None:
    nested_ref1 = create_root_child_reference(schema.tables, "root_table1__child1")
    nested_ref2 = create_root_child_reference(schema.tables, "root_table1__child1__child2")
    all_children_refs = get_all_root_child_references_from_root(schema.tables, "root_table1")

    assert isinstance(all_children_refs, list)
    assert all(ref.get("label") == "_dlt_root" for ref in all_children_refs)
    assert [nested_ref1, nested_ref2] == all_children_refs


def test_create_parent_child_reference_from_root(schema: dlt.Schema) -> None:
    """`create_parent_child_reference()` doesn't work on root tables because
    they have no parent.
    """
    with pytest.raises(ValueError) as e:
        create_parent_child_reference(schema.tables, "root_table1")
    assert e.match("Table `root_table1` is a root table and has no parent.")

    with pytest.raises(ValueError) as e:
        create_parent_child_reference(schema.tables, "root_table2")
    assert e.match("Table `root_table2` is a root table and has no parent.")


def test_parent_child_reference_from_child(schema: dlt.Schema) -> None:
    child1_to_root_ref = create_parent_child_reference(schema.tables, "root_table1__child1")
    child2_to_child1_ref = create_parent_child_reference(
        schema.tables, "root_table1__child1__child2"
    )

    assert child1_to_root_ref == EXPECTED_CHILD1_TO_ROOT1_REF
    assert child2_to_child1_ref == EXPECTED_CHILD2_TO_CHILD1_REF
    assert all(
        ref.get("label") == "_dlt_parent" for ref in [child1_to_root_ref, child2_to_child1_ref]
    )


def test_get_all_parent_child_references_from_root(schema: dlt.Schema) -> None:
    child1_to_root_ref = create_parent_child_reference(schema.tables, "root_table1__child1")
    child2_to_child1_ref = create_parent_child_reference(
        schema.tables, "root_table1__child1__child2"
    )
    all_children_refs = get_all_parent_child_references_from_root(schema.tables, "root_table1")

    assert isinstance(all_children_refs, list)
    assert all(ref.get("label") == "_dlt_parent" for ref in all_children_refs)
    assert [child1_to_root_ref, child2_to_child1_ref] == all_children_refs


def test_create_load_table_reference_from_child(schema: dlt.Schema) -> None:
    """`create_parent_child_reference()` doesn't work on root tables because
    they have no parent.
    """
    with pytest.raises(ValueError) as e:
        create_load_table_reference(schema.tables["root_table1__child1"])
    assert e.match(
        "Table `root_table1__child1` is not a root table and has no `_dlt_load_id` column."
    )

    with pytest.raises(ValueError) as e:
        create_load_table_reference(schema.tables["root_table1__child1__child2"])
    assert e.match(
        "Table `root_table1__child1__child2` is not a root table and has no `_dlt_load_id` column."
    )


def test_create_load_reference_from_root(schema: dlt.Schema) -> None:
    root1_to_load_ref = create_load_table_reference(schema.tables["root_table1"])
    root2_to_load_ref = create_load_table_reference(schema.tables["root_table2"])

    assert root1_to_load_ref == EXPECTED_ROOT1_TO_LOAD_REF
    assert root2_to_load_ref == EXPECTED_ROOT2_TO_LOAD_REF
    assert all(ref.get("label") == "_dlt_load" for ref in [root1_to_load_ref, root2_to_load_ref])
