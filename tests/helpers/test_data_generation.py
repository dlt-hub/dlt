"""Tests for data generation module."""
from __future__ import annotations
import strictyaml
import json
import pytest
from typing import TYPE_CHECKING, Callable, Optional, Type, Union, Any

import pytest

import dlt
from dlt.common.schema.typing import TColumnSchema, TTableSchema
from dlt.common.data_types import DATA_TYPES
from dlt.helpers.data_generation import (
    DLT_TO_PY_TYPE_MAP, DatasetGenerator, GeneratorState, generate_data, _build_dependency_graph, generate_column_value, generate_table_batch, _select_mimesis_generator)
from mimesis import Field

if TYPE_CHECKING:
    from dlt.common.schema.schema import Schema


@pytest.fixture
def schema() -> dlt.Schema:
    schema = dlt.Schema("test")
    schema.update_table(
        {
            "name": "users",
            "columns": {
                "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "id": {
                    "name": "id",
                    "data_type": "bigint",
                    "nullable": False,
                    "primary_key": True,
                    "unique": True,
                },
                "email": {"name": "email", "data_type": "text", "nullable": False, "unique": True},
                "name": {"name": "name", "data_type": "text", "nullable": True},
                "age": {"name": "age", "data_type": "bigint", "nullable": True},
                "active": {"name": "active", "data_type": "bool", "nullable": True},
            },
        }
    )

    return schema


@pytest.fixture
def nested_schema() -> dlt.Schema:
    """Create a schema with nested tables."""
    schema = dlt.Schema("test_nested")

    # Issues table (root)
    schema.update_table(
        {
            "name": "issues",
            "columns": {
                "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "id": {"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True},
                "title": {"name": "title", "data_type": "text", "nullable": False},
                "state": {"name": "state", "data_type": "text", "nullable": True},
            },
        }
    )

    # Labels table (nested under issues)
    schema.update_table(
        {
            "name": "issues__labels",
            "parent": "issues",
            "columns": {
                "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
                "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
                "_dlt_parent_id": {
                    "name": "_dlt_parent_id",
                    "data_type": "text",
                    "nullable": False,
                },
                "_dlt_list_idx": {
                    "name": "_dlt_list_idx",
                    "data_type": "bigint",
                    "nullable": False,
                },
                "name": {"name": "name", "data_type": "text", "nullable": False},
                "color": {"name": "color", "data_type": "text", "nullable": True},
            },
        }
    )

    return schema


def test_generator_state_initialization():
    """Test GeneratorState initialization."""
    # Assemble
    # (no setup needed)

    # Act
    state = GeneratorState()

    assert False

    # Assert
    assert state.unique_values == {}
    assert state.generated_ids == {}
    assert state.generated_records == {}
    assert state.table_record_counts == {}
    assert state.load_id is not None
    assert isinstance(state.load_id, str)
    assert len(state.load_id) > 0


@pytest.mark.parametrize("data_type", DATA_TYPES)
def test_column_value_generator_basic_types(data_type: str):
    """Test generation of basic data types."""
    # Assemble
    expected_py_type = DLT_TO_PY_TYPE_MAP[data_type]
    field = Field("en", seed=42)
    state = GeneratorState()
    col_schema = {"name": "col", "data_type": data_type, "nullable": False}
    generator_func = _select_mimesis_generator("col", col_schema, field)

    # Act
    value = generate_column_value(
        state=state,
        column_name="col",
        column_schema=col_schema,
        generator_func=generator_func,
        context=None,
        unique_key_prefix=None,
        null_probability=0.3,
    )

    # Assert
    assert isinstance(value, expected_py_type)


def test_column_value_generator_nullable():
    """Test nullable constraint."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()
    col_schema = {"name": "col", "data_type": "text", "nullable": True}
    generator_func = _select_mimesis_generator("col", col_schema, field)

    # Act
    value = generate_column_value(
        state=state,
        column_name="col",
        column_schema=col_schema,
        generator_func=generator_func,
        context=None,
        unique_key_prefix=None,
        null_probability=1.0,
    )

    # Assert
    assert value is None


def test_column_value_generator_unique():
    """Test unique constraint."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()
    col_schema = {"name": "col", "data_type": "text", "nullable": False, "unique": True}
    generator_func = _select_mimesis_generator("col", col_schema, field)

    # Act
    values = [
        generate_column_value(
            state=state,
            column_name="col",
            column_schema=col_schema,
            generator_func=generator_func,
            context=None,
            unique_key_prefix="test_table",
            null_probability=0.3,
        )
        for _ in range(100)
    ]

    # Assert
    assert len(values) == len(set(values)), "Unique constraint violated"


def test_column_value_generator_primary_key():
    """Test primary key constraint (always unique and non-null)."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()
    col_schema = {
        "name": "id",
        "data_type": "bigint",
        "nullable": False,
        "primary_key": True,
        "unique": True,
    }
    generator_func = _select_mimesis_generator("id", col_schema, field)

    # Act - Even with 100% null probability, primary key should never be null
    values = [
        generate_column_value(
            state=state,
            column_name="id",
            column_schema=col_schema,
            generator_func=generator_func,
            context=None,
            unique_key_prefix="test_table",
            null_probability=1.0,
        )
        for _ in range(10)
    ]

    # Assert
    assert all(v is not None for v in values), "Primary key should never be null"
    assert len(values) == len(set(values)), "Primary key should be unique"


@pytest.mark.parametrize(
    "column_name,data_type,context,expected_value,assertion_func",
    [
        ("_dlt_id", "text", None, None, lambda v: isinstance(v, str) and len(v) > 0),
        ("_dlt_load_id", "text", None, "test_load_id", lambda v: v == "test_load_id"),
        (
            "_dlt_parent_id",
            "text",
            {"_dlt_parent_id": "parent_123"},
            "parent_123",
            lambda v: v == "parent_123",
        ),
        ("_dlt_list_idx", "bigint", {"_dlt_list_idx": 5}, 5, lambda v: v == 5),
    ],
)
def test_column_value_generator_special_dlt_fields(
    column_name: str,
    data_type: str,
    context: dict[str, str] | dict[str, int] | None,
    expected_value: int | str | None,
    assertion_func: Callable,
):
    """Test special DLT field generation."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()
    state.load_id = "test_load_id"
    col_schema = {"name": column_name, "data_type": data_type, "nullable": False}
    generator_func = _select_mimesis_generator(column_name, col_schema, field)

    # Act
    value = generate_column_value(
        state=state,
        column_name=column_name,
        column_schema=col_schema,
        generator_func=generator_func,
        context=context,
        unique_key_prefix=None,
        null_probability=0.3,
    )

    # Assert
    assert assertion_func(value)


@pytest.mark.parametrize(
    "column_name,data_type,expected_type,validation_func",
    [
        ("email", "text", str, lambda v: "@" in v),
        ("first_name", "text", str, lambda v: isinstance(v, str) and len(v) > 0),
        ("user_uuid", "text", str, lambda v: len(v) == 36 and v.count("-") == 4),
        ("id", "bigint", int, lambda v: isinstance(v, int)),
        ("user_id", "bigint", int, lambda v: isinstance(v, int)),
        ("product_id", "bigint", int, lambda v: isinstance(v, int)),
        ("video", "text", str, lambda v: isinstance(v, str)),
        ("valid", "text", str, lambda v: isinstance(v, str)),
    ],
)
def test_column_value_generator_name_patterns(
    column_name: str,
    data_type: str,
    expected_type: type[int] | type[str],
    validation_func: Callable,
):
    """Test column name pattern matching."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()
    col_schema: TColumnSchema = {"name": column_name, "data_type": data_type, "nullable": False}
    generator_func = _select_mimesis_generator(column_name, col_schema, field)

    # Act
    value = generate_column_value(
        state=state,
        column_name=column_name,
        column_schema=col_schema,
        generator_func=generator_func,
        context=None,
        unique_key_prefix=None,
        null_probability=0.3,
    )

    # Assert
    assert isinstance(value, expected_type)
    assert validation_func(value)


def test_table_data_generator_basic(schema: dlt.Schema):
    """Test basic table data generation."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()

    # Act
    records = generate_table_batch(
        n_records=10,
        table_schema=schema.tables["users"],
        table_name="users",
        field=field,
        state=state,
        parent_context=None,
        include_dlt_columns=True,
        dlt_prefix=schema._dlt_tables_prefix,
        null_probability=0.3,
    )

    # Assert
    assert len(records) == 10
    assert all(isinstance(r, dict) for r in records)
    assert all("_dlt_id" in r for r in records)
    assert all("email" in r for r in records)
    assert all("name" in r for r in records)
    assert len(state.generated_ids["users"]) == 10


def test_table_data_generator_unique_constraint(schema: dlt.Schema):
    """Test unique constraint across batches."""
    # Assemble
    field = Field("en", seed=42)
    state = GeneratorState()

    # Act - Generate two batches
    batch1 = generate_table_batch(
        n_records=5,
        table_schema=schema.tables["users"],
        table_name="users",
        field=field,
        state=state,
        parent_context=None,
        include_dlt_columns=True,
        dlt_prefix=schema._dlt_tables_prefix,
        null_probability=0.3,
    )
    batch2 = generate_table_batch(
        n_records=5,
        table_schema=schema.tables["users"],
        table_name="users",
        field=field,
        state=state,
        parent_context=None,
        include_dlt_columns=True,
        dlt_prefix=schema._dlt_tables_prefix,
        null_probability=0.3,
    )

    # Assert - Extract email values (unique constraint)
    emails = [r["email"] for r in batch1 + batch2]
    assert len(emails) == len(set(emails)), "Unique constraint violated across batches"


def test_build_dependency_graph(nested_schema: dlt.Schema):
    """Test dependency graph building."""
    # Assemble
    # (schema fixture provides the setup)

    # Act
    graph = _build_dependency_graph(nested_schema)

    # Assert
    assert "issues" in graph
    assert "issues__labels" in graph
    assert len(graph["issues"]) == 0
    assert "issues" in graph["issues__labels"]


def test_data_generator_simple_table(schema: dlt.Schema):
    """Test DataGenerator with simple table."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)

    # Act
    records = gen.generate("users", n_records=20)

    # Assert
    assert len(records) == 20
    assert all(isinstance(r, dict) for r in records)
    assert all("_dlt_id" in r for r in records)
    assert all("email" in r for r in records)
    emails = [r["email"] for r in records]
    assert len(emails) == len(set(emails))


def test_data_generator_nested_tables(nested_schema: dlt.Schema):
    """Test DataGenerator with nested tables."""
    # Assemble
    gen = DatasetGenerator(nested_schema, seed=42, auto_resolve_references=True)

    # Act - Generate child table - should auto-generate parent
    labels = gen.generate("issues__labels", n_records=20)

    # Assert
    assert len(labels) == 20
    assert all("_dlt_parent_id" in r for r in labels)
    assert all("_dlt_list_idx" in r for r in labels)
    parent_ids = set(gen.state.generated_ids["issues"])
    label_parent_ids = {r["_dlt_parent_id"] for r in labels}
    assert label_parent_ids.issubset(parent_ids), "All parent IDs should be valid"


def test_data_generator_nested_manual_order(nested_schema: dlt.Schema):
    """Test nested table generation with manual ordering."""
    # Assemble
    gen = DatasetGenerator(nested_schema, seed=42, auto_resolve_references=False)

    # Act - Generate parent first, then child
    issues = gen.generate("issues", n_records=5)
    labels = gen.generate("issues__labels", n_records=10)

    # Assert
    assert len(issues) == 5
    assert len(labels) == 10
    issue_ids = {r["_dlt_id"] for r in issues}
    label_parent_ids = {r["_dlt_parent_id"] for r in labels}
    assert label_parent_ids.issubset(issue_ids)


def test_data_generator_generate_all(nested_schema: dlt.Schema):
    """Test generate_all method."""
    # Assemble
    gen = DatasetGenerator(nested_schema, seed=42)

    # Act
    all_data = gen.generate_all(n_records={"issues": 5, "issues__labels": 15})

    # Assert
    assert "issues" in all_data
    assert "issues__labels" in all_data
    assert len(all_data["issues"]) == 5
    assert len(all_data["issues__labels"]) == 15


@pytest.mark.parametrize(
    "reset_method,table_name,check_func",
    [
        ("reset", None, lambda state: len(state.generated_ids) == 0),
        ("reset_table", "users", lambda state: "users" not in state.generated_ids),
    ],
)
def test_data_generator_reset_functionality(
    schema: dlt.Schema, reset_method: str, table_name: str | None, check_func: Callable
):
    """Test reset and reset_table functionality."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)
    gen.generate("users", n_records=10)
    assert len(gen.state.generated_ids["users"]) == 10

    # Act
    if reset_method == "reset":
        gen.reset()
    else:
        gen.reset_table(table_name)

    # Assert
    assert check_func(gen.state)
    assert len(gen.state.generated_records) == 0 if reset_method == "reset" else True


def test_data_generator_reproducibility(schema: dlt.Schema):
    """Test that same seed produces same results."""
    # Assemble
    gen1 = DatasetGenerator(schema, seed=42)
    gen2 = DatasetGenerator(schema, seed=42)

    # Act
    records1 = gen1.generate("users", n_records=5)
    records2 = gen2.generate("users", n_records=5)

    # Assert - Compare email fields (should be identical with same seed)
    emails1 = [r["email"] for r in records1]
    emails2 = [r["email"] for r in records2]
    assert emails1 == emails2


def test_data_generator_list_idx_increment(nested_schema: dlt.Schema):
    """Test that _dlt_list_idx increments correctly per parent."""
    # Assemble
    gen = DatasetGenerator(nested_schema, seed=42)
    gen.generate("issues", n_records=2)

    # Act
    labels = gen.generate("issues__labels", n_records=20)

    # Assert
    by_parent: dict[str, list[int]] = {}
    for label in labels:
        parent_id = label["_dlt_parent_id"]
        if parent_id not in by_parent:
            by_parent[parent_id] = []
        by_parent[parent_id].append(label["_dlt_list_idx"])

    for parent_id, indices in by_parent.items():
        sorted_indices = sorted(indices)
        expected = list(range(len(indices)))
        assert sorted_indices == expected, f"List indices not sequential for parent {parent_id}"


def test_data_generator_get_table_state(schema: dlt.Schema):
    """Test get_table_state method."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)

    # Act
    before_state = gen.get_table_state("users")
    gen.generate("users", n_records=15)
    after_state = gen.get_table_state("users")

    # Assert
    assert before_state["record_count"] == 0
    assert before_state["generated_ids_count"] == 0
    assert after_state["record_count"] == 15
    assert after_state["generated_ids_count"] == 15


def test_data_generator_multiple_batches_maintain_uniqueness(schema: dlt.Schema):
    """Test that generating multiple batches maintains uniqueness."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)

    # Act - Generate multiple batches
    batch1 = gen.generate("users", n_records=10)
    batch2 = gen.generate("users", n_records=10)
    batch3 = gen.generate("users", n_records=10)

    # Assert - Check email uniqueness across all batches
    all_emails = [r["email"] for r in batch1 + batch2 + batch3]
    assert len(all_emails) == 30
    assert len(set(all_emails)) == 30, "Emails should be unique across all batches"


def test_data_generator_json_type():
    """Test JSON data type generation."""
    # Assemble
    schema = dlt.Schema("test")
    schema.update_table(
        {
            "name": "test_table",
            "columns": {
                "data": {"name": "data", "data_type": "json", "nullable": False},
            },
        }
    )
    gen = DatasetGenerator(schema, seed=42)

    # Act
    records = gen.generate("test_table", n_records=5)

    # Assert
    assert len(records) == 5
    for record in records:
        assert "data" in record
        assert isinstance(record["data"], dict)


def test_data_generator_no_auto_resolve_raises_error(nested_schema: dlt.Schema):
    """Test that error is raised when parent not generated and auto_resolve is off."""
    # Assemble
    gen = DatasetGenerator(nested_schema, seed=42, auto_resolve_references=False)

    # Act & Assert
    with pytest.raises(ValueError, match="Parent table .* must be generated"):
        gen.generate("issues__labels", n_records=10)


@pytest.mark.skipif(not __name__.startswith("tests."), reason="Requires GitHub issues schema file")
def test_github_schema_integration():
    """Integration test with real GitHub issues schema."""
    # Assemble
    import os

    schema_path = "tests/common/cases/schemas/github/issues.schema.json"
    if not os.path.exists(schema_path):
        pytest.skip(f"Schema file not found: {schema_path}")

    with open(schema_path) as f:
        schema_data = json.load(f)

    schema = dlt.Schema.from_stored_schema(schema_data)
    generator = DatasetGenerator(schema, seed=42)

    # Act - Generate parent table
    issues = generator.generate("issues", n_records=10)

    # Assert
    assert len(issues) == 10
    assert all("_dlt_id" in record for record in issues)

    # Generate nested table if it exists
    if "issues__labels" in schema.tables:
        labels = generator.generate("issues__labels", n_records=20)
        assert len(labels) == 20
        assert all("_dlt_parent_id" in record for record in labels)
        issue_ids = {r["_dlt_id"] for r in issues}
        label_parent_ids = {r["_dlt_parent_id"] for r in labels}
        assert label_parent_ids.issubset(issue_ids)


@pytest.mark.parametrize(
    "null_probability,expected_null_count",
    [
        (0.0, 0),
        (1.0, 20),
    ],
)
def test_data_generator_null_probability(null_probability: float, expected_null_count: int):
    """Test null_probability parameter."""
    # Assemble
    schema = dlt.Schema("test")
    schema.update_table(
        {
            "name": "test_table",
            "columns": {
                "nullable_field": {"name": "nullable_field", "data_type": "text", "nullable": True},
            },
        }
    )
    gen = DatasetGenerator(schema, seed=42, null_probability=null_probability)

    # Act
    records = gen.generate("test_table", n_records=20)

    # Assert
    null_count = sum(1 for r in records if r["nullable_field"] is None)
    assert null_count == expected_null_count


def test_data_generator_consistency_same_load_id(schema: dlt.Schema):
    """Test that all records share the same load_id."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)

    # Act
    batch1 = gen.generate("users", n_records=5)
    batch2 = gen.generate("users", n_records=5)

    # Assert
    load_ids = {r["_dlt_load_id"] for r in batch1 + batch2}
    assert len(load_ids) == 1, "All records should have the same load_id"


@pytest.mark.parametrize(
    "schema_type,table_name,dlt_columns,regular_columns",
    [
        ("simple", "users", ["_dlt_id", "_dlt_load_id"], ["email", "name"]),
        (
            "nested_parent",
            "issues",
            ["_dlt_id", "_dlt_load_id"],
            ["title"],
        ),
        (
            "nested_child",
            "issues__labels",
            ["_dlt_id", "_dlt_load_id", "_dlt_parent_id", "_dlt_list_idx"],
            ["name"],
        ),
    ],
)
def test_data_generator_exclude_dlt_columns(
    schema: dlt.Schema,
    nested_schema: dlt.Schema,
    schema_type: str,
    table_name: str,
    dlt_columns: list[str],
    regular_columns: list[str],
):
    """Test excluding dlt columns from generated data."""
    # Assemble
    if schema_type == "simple":
        test_schema = schema
        n_records = 10
    else:
        test_schema = nested_schema
        n_records = 5

    gen = DatasetGenerator(test_schema, seed=42, include_dlt_columns=False)

    # Act - Generate parent first if needed for nested child
    if schema_type == "nested_child":
        gen.generate("issues", n_records=5)

    records = gen.generate(table_name, n_records=n_records)

    # Assert
    assert len(records) == n_records
    assert all(isinstance(r, dict) for r in records)

    # Check that dlt columns are not present or are None
    for record in records:
        for dlt_col in dlt_columns:
            assert dlt_col not in record or record[dlt_col] is None

    # Check that regular columns are still present
    for record in records:
        assert regular_columns[0] in record


def test_generate_data_convenience_function_exclude_dlt_columns(schema: dlt.Schema) -> None:
    """Test generate_data convenience function with exclude_dlt_columns."""
    # Assemble
    # (schema fixture provides the setup)

    # Act
    records = generate_data(schema, "users", n_records=5, include_dlt_columns=False)

    # Assert
    assert len(records) == 5
    for record in records:
        assert "_dlt_id" not in record or record["_dlt_id"] is None
        assert "_dlt_load_id" not in record or record["_dlt_load_id"] is None
        assert "email" in record


def test_data_generator_include_dlt_columns_default(schema: dlt.Schema) -> None:
    """Test that dlt columns are included by default."""
    # Assemble
    gen = DatasetGenerator(schema, seed=42)

    # Act
    records = gen.generate("users", n_records=5)

    # Assert
    assert len(records) == 5
    for record in records:
        assert "_dlt_id" in record
        assert record["_dlt_id"] is not None
        assert "_dlt_load_id" in record
        assert record["_dlt_load_id"] is not None
