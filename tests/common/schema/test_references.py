import pytest

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


@pytest.fixture
def schema() -> dlt.Schema:
    return dlt.Schema.from_dict(
        {
            "version": 2,
            "version_hash": "rU2+hAL/KeR+Z0cVU/hTLcHJMUBRJgsC1b+3VJC/+ww=",
            "engine_version": 11,
            "name": "mock_source",
            "tables": {
                "_dlt_version": {
                    "name": "_dlt_version",
                    "columns": {
                        "version": {"name": "version", "data_type": "bigint", "nullable": False},
                        "engine_version": {
                            "name": "engine_version",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "inserted_at": {
                            "name": "inserted_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "schema_name": {
                            "name": "schema_name",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "version_hash": {
                            "name": "version_hash",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "schema": {"name": "schema", "data_type": "text", "nullable": False},
                    },
                    "write_disposition": "skip",
                    "resource": "_dlt_version",
                    "description": "Created by DLT. Tracks schema updates",
                },
                "_dlt_loads": {
                    "name": "_dlt_loads",
                    "columns": {
                        "load_id": {
                            "name": "load_id",
                            "data_type": "text",
                            "nullable": False,
                            "precision": 64,
                        },
                        "schema_name": {
                            "name": "schema_name",
                            "data_type": "text",
                            "nullable": True,
                        },
                        "status": {"name": "status", "data_type": "bigint", "nullable": False},
                        "inserted_at": {
                            "name": "inserted_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "schema_version_hash": {
                            "name": "schema_version_hash",
                            "data_type": "text",
                            "nullable": True,
                        },
                    },
                    "write_disposition": "skip",
                    "resource": "_dlt_loads",
                    "description": "Created by DLT. Tracks completed loads",
                },
                "root_table1": {
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "nullable": True},
                        "key1": {"name": "key1", "data_type": "double", "nullable": True},
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "references": [
                        {
                            "columns": ["key1"],
                            "referenced_table": "root_table2",
                            "referenced_columns": ["key2"],
                        }
                    ],
                    "name": "root_table1",
                    "resource": "root_table1",
                },
                "root_table2": {
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "nullable": True},
                        "key2": {"name": "key2", "data_type": "double", "nullable": True},
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "name": "root_table2",
                    "resource": "root_table2",
                },
                "_dlt_pipeline_state": {
                    "columns": {
                        "version": {"name": "version", "data_type": "bigint", "nullable": False},
                        "engine_version": {
                            "name": "engine_version",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "pipeline_name": {
                            "name": "pipeline_name",
                            "data_type": "text",
                            "nullable": False,
                        },
                        "state": {"name": "state", "data_type": "text", "nullable": False},
                        "created_at": {
                            "name": "created_at",
                            "data_type": "timestamp",
                            "nullable": False,
                        },
                        "version_hash": {
                            "name": "version_hash",
                            "data_type": "text",
                            "nullable": True,
                        },
                        "_dlt_load_id": {
                            "name": "_dlt_load_id",
                            "data_type": "text",
                            "nullable": False,
                            "precision": 64,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "write_disposition": "append",
                    "file_format": "preferred",
                    "name": "_dlt_pipeline_state",
                    "resource": "_dlt_pipeline_state",
                },
                "root_table1__child1": {
                    "name": "root_table1__child1",
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "nullable": True},
                        "depth1": {"name": "depth1", "data_type": "text", "nullable": True},
                        "_dlt_root_id": {
                            "name": "_dlt_root_id",
                            "data_type": "text",
                            "nullable": False,
                            "root_key": True,
                        },
                        "_dlt_parent_id": {
                            "name": "_dlt_parent_id",
                            "data_type": "text",
                            "nullable": False,
                            "parent_key": True,
                        },
                        "_dlt_list_idx": {
                            "name": "_dlt_list_idx",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "parent": "root_table1",
                },
                "root_table1__child1__child2": {
                    "name": "root_table1__child1__child2",
                    "columns": {
                        "id": {"name": "id", "data_type": "bigint", "nullable": True},
                        "depth1": {"name": "depth1", "data_type": "text", "nullable": True},
                        "_dlt_root_id": {
                            "name": "_dlt_root_id",
                            "data_type": "text",
                            "nullable": False,
                            "root_key": True,
                        },
                        "_dlt_parent_id": {
                            "name": "_dlt_parent_id",
                            "data_type": "text",
                            "nullable": False,
                            "parent_key": True,
                        },
                        "_dlt_list_idx": {
                            "name": "_dlt_list_idx",
                            "data_type": "bigint",
                            "nullable": False,
                        },
                        "_dlt_id": {
                            "name": "_dlt_id",
                            "data_type": "text",
                            "nullable": False,
                            "unique": True,
                            "row_key": True,
                        },
                    },
                    "parent": "root_table1__child1",
                },
            },
            "settings": {
                "detections": ["iso_timestamp"],
                "default_hints": {
                    "not_null": [
                        "_dlt_id",
                        "_dlt_root_id",
                        "_dlt_parent_id",
                        "_dlt_list_idx",
                        "_dlt_load_id",
                    ],
                    "parent_key": ["_dlt_parent_id"],
                    "root_key": ["_dlt_root_id"],
                    "unique": ["_dlt_id"],
                    "row_key": ["_dlt_id"],
                },
            },
            "normalizers": {
                "names": "snake_case",
                "json": {
                    "module": "dlt.common.normalizers.json.relational",
                    "config": {
                        "root_key_propagation": True,
                        "propagation": {
                            "tables": {
                                "root_table1": {"_dlt_id": "_dlt_root_id"},
                                "root_table2": {"_dlt_id": "_dlt_root_id"},
                            }
                        },
                    },
                },
            },
            "previous_hashes": [
                "o4g4bDEBBXdLCtW33Lraslp41bmO/QuutxbykBzymf8=",
                "lv4OMECSbEgjo5+tfmfTcYX4ZN7fuE1eWOoRpOBVdKM=",
            ],
        }
    )


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
    "cardinality": "many_to_one",
    "table": "root_table1",
    "columns": ["_dlt_load_id"],
    "referenced_table": "_dlt_loads",
    "referenced_columns": ["load_id"],
}
EXPECTED_ROOT2_TO_LOAD_REF = {
    "label": "_dlt_load",
    "cardinality": "many_to_one",
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


@pytest.mark.parametrize(
    "name_normalizer_ref",
    (
        "tests.common.cases.normalizers.title_case",
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.snake_no_x",
    ),
)
def test_references_parameterized_by_naming(schema: dlt.Schema, name_normalizer_ref: str) -> None:
    schema._normalizers_config["names"] = name_normalizer_ref
    schema.update_normalizers()

    expected_references = [
        {
            "label": "_dlt_load",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_identifier("_dlt_pipeline_state"),
            "columns": [schema.naming.normalize_identifier("_dlt_load_id")],
            "referenced_table": schema.naming.normalize_identifier("_dlt_loads"),
            "referenced_columns": [schema.naming.normalize_identifier("load_id")],
        },
        {
            "label": "_dlt_load",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_identifier("root_table1"),
            "columns": [schema.naming.normalize_identifier("_dlt_load_id")],
            "referenced_table": schema.naming.normalize_identifier("_dlt_loads"),
            "referenced_columns": [schema.naming.normalize_identifier("load_id")],
        },
        {
            "columns": [schema.naming.normalize_identifier("key1")],
            "referenced_table": schema.naming.normalize_identifier("root_table2"),
            "referenced_columns": [schema.naming.normalize_identifier("key2")],
            "table": schema.naming.normalize_identifier("root_table1"),
        },
        {
            "label": "_dlt_load",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_identifier("root_table2"),
            "columns": [schema.naming.normalize_identifier("_dlt_load_id")],
            "referenced_table": schema.naming.normalize_identifier("_dlt_loads"),
            "referenced_columns": [schema.naming.normalize_identifier("load_id")],
        },
        {
            "label": "_dlt_parent",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_tables_path("root_table1__child1"),
            "columns": [schema.naming.normalize_identifier("_dlt_parent_id")],
            "referenced_table": schema.naming.normalize_identifier("root_table1"),
            "referenced_columns": [schema.naming.normalize_identifier("_dlt_id")],
        },
        {
            "label": "_dlt_root",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_tables_path("root_table1__child1"),
            "columns": [schema.naming.normalize_identifier("_dlt_root_id")],
            "referenced_table": schema.naming.normalize_identifier("root_table1"),
            "referenced_columns": [schema.naming.normalize_identifier("_dlt_id")],
        },
        {
            "label": "_dlt_parent",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_tables_path("root_table1__child1__child2"),
            "columns": [schema.naming.normalize_identifier("_dlt_parent_id")],
            "referenced_table": schema.naming.normalize_tables_path("root_table1__child1"),
            "referenced_columns": [schema.naming.normalize_identifier("_dlt_id")],
        },
        {
            "label": "_dlt_root",
            "cardinality": "many_to_one",
            "table": schema.naming.normalize_tables_path("root_table1__child1__child2"),
            "columns": [schema.naming.normalize_identifier("_dlt_root_id")],
            "referenced_table": schema.naming.normalize_identifier("root_table1"),
            "referenced_columns": [schema.naming.normalize_identifier("_dlt_id")],
        },
    ]

    assert isinstance(schema.references, list)
    assert len(schema.references) == 8
    assert isinstance(schema.references[0], dict)
    # check that keys are from TStandaloneTableReference
    # can't do `isinstance(..., TStandaloneTableReference)` on a `TypedDict`
    assert set(schema.references[0]) == set(
        [
            "label",
            "table",
            "columns",
            "referenced_table",
            "referenced_columns",
            "cardinality",
        ]
    )
    assert schema.references == expected_references
