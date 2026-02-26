"""
Tests for JSON column expansion feature.

Tests cover all acceptance criteria (AC1-AC10) for JSON string flattening.
"""

import pytest
from typing import Any, Dict, List

from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer

from tests.utils import create_schema_with_name


@pytest.fixture
def norm() -> RelationalNormalizer:
    """Create a relational normalizer for testing."""
    return Schema("default").data_item_normalizer  # type: ignore[return-value]


def test_ac1_basic_json_flatten(norm: RelationalNormalizer) -> None:
    """AC1: Basic JSON string flattening."""
    print("AC1: Basic JSON String Flattening")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "metadata", "data_type": "text", "x-json-flatten": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "metadata": '{"name": "John", "email": "john@example.com"}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=True")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["metadata__name"] == "John"
    assert flattened_row["metadata__email"] == "john@example.com"
    assert "metadata" not in flattened_row


def test_ac2_keep_original(norm: RelationalNormalizer) -> None:
    """AC2: Keep original column alongside flattened columns."""
    print("AC2: Keep Original Column")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "metadata", "data_type": "text", "x-json-flatten": True, "x-json-keep-original": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "metadata": '{"name": "John", "email": "john@example.com"}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=True, x-json-keep-original=True")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["metadata"] == '{"name": "John", "email": "john@example.com"}'
    assert flattened_row["metadata__name"] == "John"
    assert flattened_row["metadata__email"] == "john@example.com"


def test_ac3_path_based_with_keep_original(norm: RelationalNormalizer) -> None:
    """AC3: Path-based flattening with keep_original."""
    print("AC3: Keep Original + Path-Based")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "data", "data_type": "text", "x-json-flatten": ["user.name"], "x-json-keep-original": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "data": '{"user": {"name": "John", "age": 30}, "timestamp": "2024-01-01"}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=['user.name'], x-json-keep-original=True")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["data"] == '{"user": {"name": "John", "age": 30}, "timestamp": "2024-01-01"}'
    assert flattened_row["data__user__name"] == "John"
    assert "data__user__age" not in flattened_row
    assert "data__timestamp" not in flattened_row


def test_ac4_keep_original_without_flatten(norm: RelationalNormalizer) -> None:
    """AC4: Keep original without flattening."""
    print("AC4: Keep Original Without Flattening")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "raw_json", "data_type": "text", "x-json-keep-original": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "raw_json": '{"nested": {"field": "value"}}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-keep-original=True (no flatten)")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["raw_json"] == '{"nested": {"field": "value"}}'
    assert "raw_json__nested" not in flattened_row


def test_ac5_keep_original_with_dict_native(norm: RelationalNormalizer) -> None:
    """AC5: Keep original with dict (serialize to JSON string)."""
    print("AC5: Keep Original with Native Dicts")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "user_profile", "data_type": "json", "x-json-keep-original": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "user_profile": {"name": "John", "email": "john@example.com", "settings": {"theme": "dark"}}}
    print("\nINPUT:", row)
    print("CONFIG: x-json-keep-original=True (no flatten_spec)")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert "user_profile__name" in flattened_row
    assert "user_profile__email" in flattened_row
    assert "user_profile__settings__theme" in flattened_row
    assert "user_profile__original" in flattened_row
    assert isinstance(flattened_row["user_profile__original"], str)


def test_ac6_path_based_only(norm: RelationalNormalizer) -> None:
    """AC6: Path-based flattening only (no keep_original)."""
    print("AC6: Path-Based Extraction Only")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "data", "data_type": "text", "x-json-flatten": ["user.name", "user.email"]},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "data": '{"user": {"name": "John", "email": "john@example.com", "age": 30}}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=['user.name', 'user.email']")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["data__user__name"] == "John"
    assert flattened_row["data__user__email"] == "john@example.com"
    assert "data__user__age" not in flattened_row
    assert "data" not in flattened_row


def test_ac7_array_at_level_2(norm: RelationalNormalizer) -> None:
    """AC7: Array at level 2 creates nested table."""
    print("AC7: Arrays at Second Level")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "metadata", "data_type": "text", "x-json-flatten": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "metadata": '{"name": "John", "tags": [{"label": "vip"}, {"label": "premium"}]}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=True")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    main_row = next(r[1] for r in rows if r[0][0] == "test_table")
    tag_rows = [r[1] for r in rows if r[0][0] == "test_table__metadata__tags"]
    
    # Filter out DLT internal metadata columns for cleaner output
    main_row_clean = {k: v for k, v in main_row.items() if not k.startswith("_dlt_")}
    tag_rows_clean = [{k: v for k, v in tr.items() if not k.startswith("_dlt_")} for tr in tag_rows]
    
    print("OUTPUT - Main table:", main_row_clean)
    print("OUTPUT - Nested table (test_table__metadata__tags):", len(tag_rows_clean), "rows")
    for i, tag_row in enumerate(tag_rows_clean):
        print(f"  Row {i}: {tag_row}")

    assert main_row["id"] == 1
    assert main_row["metadata__name"] == "John"
    assert len(tag_rows) == 2
    assert tag_rows[0]["label"] == "vip"
    assert tag_rows[1]["label"] == "premium"
    assert "_dlt_parent_id" in tag_rows[0]
    assert "_dlt_list_idx" in tag_rows[0]
    assert tag_rows[0]["_dlt_list_idx"] == 0
    assert tag_rows[1]["_dlt_list_idx"] == 1


def test_ac8_invalid_json(norm: RelationalNormalizer) -> None:
    """AC8: Invalid JSON keeps original value."""
    print("AC8: Invalid JSON Handling")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "metadata", "data_type": "text", "x-json-flatten": True},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "metadata": "not-valid-json"}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=True")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["metadata"] == "not-valid-json"
    assert "metadata__name" not in flattened_row


def test_ac9_missing_path(norm: RelationalNormalizer) -> None:
    """AC9: Missing path silently skipped."""
    print("AC9: Missing Paths Handling")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "data", "data_type": "text", "x-json-flatten": ["user.name", "user.email"]},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "data": '{"user": {"name": "John"}}'}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=['user.name', 'user.email'] (user.email missing)")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["data__user__name"] == "John"
    assert "data__user__email" not in flattened_row


def test_ac10_arrow_parquet_struct_with_path_filter(norm: RelationalNormalizer) -> None:
    """AC10: Arrow/Parquet struct with path filtering."""

    print("AC10: Arrow/Parquet Struct Support")
    
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "struct_col", "data_type": "json", "x-json-flatten": ["name"]},
            ],
        )
    )
    norm._reset()

    row = {"id": 1, "struct_col": {"name": "John", "age": 30}}
    print("\nINPUT:", row)
    print("CONFIG: x-json-flatten=['name'] (path-based on dict)")

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]
    # Filter out DLT internal metadata columns for cleaner output comparison
    output_clean = {k: v for k, v in flattened_row.items() if not k.startswith("_dlt_")}
    print("OUTPUT:", output_clean)

    assert flattened_row["id"] == 1
    assert flattened_row["struct_col__name"] == "John"
    assert "struct_col__age" not in flattened_row


def test_multiple_json_columns(norm: RelationalNormalizer) -> None:
    """Test multiple JSON columns with different configurations."""
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {
                    "name": "metadata",
                    "data_type": "text",
                    "x-json-flatten": True,
                    "x-json-keep-original": True,
                },
                {
                    "name": "config",
                    "data_type": "text",
                    "x-json-flatten": ["settings.theme"],
                },
            ],
        )
    )
    norm._reset()

    row = {
        "id": 1,
        "metadata": '{"name": "John", "email": "john@example.com"}',
        "config": '{"settings": {"theme": "dark", "lang": "en"}}',
    }

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]

    # Assert: Both columns processed correctly
    assert flattened_row["id"] == 1
    # metadata: full flatten + keep original
    assert flattened_row["metadata"] == '{"name": "John", "email": "john@example.com"}'
    assert flattened_row["metadata__name"] == "John"
    assert flattened_row["metadata__email"] == "john@example.com"
    # config: path-based only
    assert flattened_row["config__settings__theme"] == "dark"
    assert "config__settings__lang" not in flattened_row
    assert "config" not in flattened_row  # Original removed


def test_null_value_handling(norm: RelationalNormalizer) -> None:
    """Test null values are handled correctly."""
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {
                    "name": "metadata",
                    "data_type": "text",
                    "x-json-flatten": True,
                },
            ],
        )
    )
    norm._reset()

    row = {
        "id": 1,
        "metadata": None,
    }

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]

    # Assert: Null value kept as-is, no expansion
    assert flattened_row["id"] == 1
    assert flattened_row["metadata"] is None
    assert "metadata__name" not in flattened_row


def test_empty_json_object(norm: RelationalNormalizer) -> None:
    """Test empty JSON object."""
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {
                    "name": "metadata",
                    "data_type": "text",
                    "x-json-flatten": True,
                },
            ],
        )
    )
    norm._reset()

    row = {
        "id": 1,
        "metadata": "{}",
    }

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]

    # Assert: Empty object handled gracefully
    assert flattened_row["id"] == 1
    # No flattened columns (empty dict)
    assert "metadata__name" not in flattened_row
    assert "metadata" not in flattened_row  # Original removed when empty


def test_nested_paths_deep(norm: RelationalNormalizer) -> None:
    """Test deeply nested paths."""
    norm.schema.update_table(
        new_table(
            "test_table",
            columns=[
                {"name": "id", "data_type": "bigint"},
                {
                    "name": "data",
                    "data_type": "text",
                    "x-json-flatten": ["a.b.c.d", "x.y"],
                },
            ],
        )
    )
    norm._reset()

    row = {
        "id": 1,
        "data": '{"a": {"b": {"c": {"d": "value1"}}}, "x": {"y": "value2"}, "z": "ignored"}',
    }

    rows = list(norm.normalize_data_item(row, "load_id", "test_table"))
    flattened_row = rows[0][1]

    # Assert: Deep paths extracted correctly
    assert flattened_row["id"] == 1
    assert flattened_row["data__a__b__c__d"] == "value1"
    assert flattened_row["data__x__y"] == "value2"
    assert "data__z" not in flattened_row  # Not in paths list
