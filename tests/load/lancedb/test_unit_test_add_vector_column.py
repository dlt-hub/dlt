import pyarrow as pa
from datetime import datetime, date
from decimal import Decimal

from dlt.destinations.impl.lancedb.schema import add_vector_column


def test_add_vector_column() -> None:
    """
    Test case 1: Schema has vector column, records doesn't
    -> should add vector column at correct index
    """
    # Create records without vector column
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "age": 30, "score": 95.5},
            {"id": 2, "name": "Bob", "age": 25, "score": 87.2},
        ]
    )

    # Create schema with vector column at index 2 (between name and age)
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 3)),  # Index 2
            pa.field("age", pa.int64()),
            pa.field("score", pa.float64()),
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should add vector column at the correct index
    assert result is not records  # Should be a new table
    assert result.num_columns == 5
    assert "vector" in result.schema.names

    # Verify vector column is at the correct index (2)
    assert result.schema.get_field_index("vector") == 2

    # Verify vector column contains nulls
    assert result["vector"].null_count == 2

    # Verify other columns are preserved in correct order
    expected_columns = ["id", "name", "vector", "age", "score"]
    assert list(result.schema.names) == expected_columns


def test_add_vector_column_already_there() -> None:
    """Test case 2: vector exists in records and in schema -> should return unchanged"""
    # Create records with vector column
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "vector": [1.0, 2.0, 3.0]},
            {"id": 2, "name": "Bob", "vector": [4.0, 5.0, 6.0]},
        ]
    )

    # Create schema with vector column at index 2
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 3)),
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should return the original table unchanged
    assert result is records
    assert result.num_columns == 3
    assert result["vector"].null_count == 0  # Original data preserved

    # Verify vector column is at the correct index (2)
    assert result.schema.get_field_index("vector") == 2


def test_add_vector_column_no_vector_in_target_schema() -> None:
    """Test case 3: Neither records nor schema have vector column - should return unchanged."""
    # Create records without vector column
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
    )

    # Create schema without vector column
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int64()),
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should return the original table unchanged
    assert result is records
    assert result.num_columns == 3
    assert "vector" not in result.schema.names


def test_add_vector_column_records_has_schema_doesnt() -> None:
    """Test case 4: Records have vector column, schema doesn't - should return unchanged."""
    # Create records with vector column
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "vector": [1.0, 2.0, 3.0]},
            {"id": 2, "name": "Bob", "vector": [4.0, 5.0, 6.0]},
        ]
    )

    # Create schema without vector column
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should return the original table unchanged
    assert result is records
    assert result.num_columns == 3
    assert result["vector"].null_count == 0  # Original data preserved


def test_add_vector_column_schema_has_records_doesnt_complex_types() -> None:
    """Test case 4 with complex data types to ensure compatibility."""
    # Create records with various data types
    records = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "name": "Alice",
                "active": True,
                "score": 95.5,
                "birth_date": date(1990, 1, 1),
                "created_at": datetime(2023, 1, 1, 12, 0, 0),
                "data": b"binary data",
                "amount": Decimal("123.45"),
            },
            {
                "id": 2,
                "name": "Bob",
                "active": False,
                "score": 87.2,
                "birth_date": date(1995, 5, 15),
                "created_at": datetime(2023, 1, 2, 12, 0, 0),
                "data": b"more binary",
                "amount": Decimal("678.90"),
            },
        ]
    )

    # Create schema with vector column at index 5 (after score)
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("score", pa.float64()),
            pa.field("birth_date", pa.date32()),
            pa.field("vector", pa.list_(pa.float32(), 4)),  # Index 5
            pa.field("created_at", pa.timestamp("us")),
            pa.field("data", pa.binary()),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should add vector column at the correct index
    assert result is not records
    assert result.num_columns == 9
    assert "vector" in result.schema.names

    # Verify vector column is at the correct index (5)
    assert result.schema.get_field_index("vector") == 5

    # Verify vector column contains nulls
    assert result["vector"].null_count == 2

    # Verify other columns are preserved in correct order
    expected_columns = [
        "id",
        "name",
        "active",
        "score",
        "birth_date",
        "vector",
        "created_at",
        "data",
        "amount",
    ]
    assert list(result.schema.names) == expected_columns
