import pytest
import pyarrow as pa
import pandas as pd

from dlt.destinations.impl.lancedb.lancedb_client import align_schema


class TestAlignSchema:
    """Test cases for the align_schema function."""

    def test_identical_schemas(self) -> None:
        """Test that identical schemas return the original table."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
        records = pa.Table.from_pylist(data, schema=schema)

        result = align_schema(records, schema)

        # Should return the same table (not a copy)
        assert result is records
        assert result.schema == schema

    def test_missing_columns_in_source(self) -> None:
        """Test that missing columns are filled with NULLs."""
        target_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("extra", pa.string()),  # Missing in source
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert len(result.columns) == 4
        assert "extra" in result.column_names

        # Check that extra column is filled with NULLs
        extra_column = result["extra"]
        assert extra_column.null_count == 2  # All values should be NULL

    def test_extra_columns_in_source(self) -> None:
        """Test that extra columns in source are ignored."""
        target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("extra", pa.string()),  # Extra in source
            ]
        )

        data = [
            {"id": 1, "name": "test1", "extra": "ignored"},
            {"id": 2, "name": "test2", "extra": "ignored"},
        ]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert len(result.columns) == 2
        assert "extra" not in result.column_names

    def test_different_column_order(self) -> None:
        """Test that columns are reordered to match target schema."""
        target_schema = pa.schema(
            [
                pa.field("name", pa.string()),
                pa.field("id", pa.int64()),
                pa.field("value", pa.float64()),
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("value", pa.float64()),
                pa.field("name", pa.string()),
            ]
        )

        data = [{"id": 1, "value": 1.5, "name": "test1"}, {"id": 2, "value": 2.5, "name": "test2"}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert result.column_names == ["name", "id", "value"]

    def test_case_insensitive_matching(self) -> None:
        """Test that column names are matched case-insensitively."""
        target_schema = pa.schema(
            [
                pa.field("ID", pa.int64()),
                pa.field("Name", pa.string()),
                pa.field("VALUE", pa.float64()),
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert result.column_names == ["ID", "Name", "VALUE"]

    def test_type_casting(self) -> None:
        """Test that incompatible types are cast when possible."""
        target_schema = pa.schema(
            [
                pa.field("id", pa.int32()),  # Different type
                pa.field("name", pa.string()),
                pa.field("value", pa.float32()),  # Different type
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert result["id"].type == pa.int32()
        assert result["value"].type == pa.float32()

    def test_type_casting_failure(self) -> None:
        """Test that failed type casting results in NULL columns."""
        target_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.string()),  # Cannot cast float to string
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert result["value"].type == pa.string()
        # PyArrow can actually cast float to string, so we check the values
        assert result["value"].to_pylist() == ["1.5", "2.5"]

    def test_mixed_scenarios(self) -> None:
        """Test a complex scenario with multiple issues."""
        target_schema = pa.schema(
            [
                pa.field("Name", pa.string()),
                pa.field("ID", pa.int32()),
                pa.field("missing", pa.string()),
                pa.field("VALUE", pa.float32()),
            ]
        )

        source_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("extra", pa.string()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        data = [
            {"id": 1, "extra": "ignored", "name": "test1", "value": 1.5},
            {"id": 2, "extra": "ignored", "name": "test2", "value": 2.5},
        ]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert result.column_names == ["Name", "ID", "missing", "VALUE"]
        assert result["missing"].null_count == 2  # Missing column should be NULL
        assert "extra" not in result.column_names  # Extra column should be ignored

    def test_empty_table(self) -> None:
        """Test with an empty table."""
        target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        source_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        records = pa.Table.from_pylist([], schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert len(result) == 0

    def test_single_column(self) -> None:
        """Test with a single column."""
        target_schema = pa.schema([pa.field("id", pa.int64())])

        source_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        data = [{"id": 1, "name": "test"}]
        records = pa.Table.from_pylist(data, schema=source_schema)

        result = align_schema(records, target_schema)

        assert result.schema == target_schema
        assert len(result.columns) == 1
        assert result.column_names == ["id"]
