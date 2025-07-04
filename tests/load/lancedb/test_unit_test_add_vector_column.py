import pyarrow as pa
from datetime import datetime, date
from decimal import Decimal

from dlt.destinations.impl.lancedb.schema import add_vector_column


def test_add_vector_column_basic_types() -> None:
    """Test add_vector_column with basic PyArrow types."""
    # Create a simple table with basic types
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "age": 30, "active": True},
            {"id": 2, "name": "Bob", "age": 25, "active": False},
        ]
    )

    # Create schema with vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 3))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int64()),
            pa.field("active", pa.bool_()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Verify vector column was added
    assert "vector" in result.schema.names
    assert result.num_columns == 5
    assert result.num_rows == 2

    # Verify vector column contains nulls
    vector_col = result["vector"]
    assert vector_col.null_count == 2  # All values should be null


def test_add_vector_column_decimal_type() -> None:
    """Test add_vector_column with decimal type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "amount": Decimal("123.45")},
            {"id": 2, "amount": Decimal("678.90")},
        ]
    )

    # Create schema with decimal vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 4))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("amount", pa.decimal128(10, 2)),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_timestamp_type() -> None:
    """Test add_vector_column with timestamp type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "created_at": datetime(2023, 1, 1, 12, 0, 0)},
            {"id": 2, "created_at": datetime(2023, 1, 2, 12, 0, 0)},
        ]
    )

    # Create schema with timestamp and vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 5))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("created_at", pa.timestamp("us")),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_date_type() -> None:
    """Test add_vector_column with date type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "birth_date": date(1990, 1, 1)},
            {"id": 2, "birth_date": date(1995, 5, 15)},
        ]
    )

    # Create schema with date and vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 6))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("birth_date", pa.date32()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_binary_type() -> None:
    """Test add_vector_column with binary type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "data": b"binary data 1"},
            {"id": 2, "data": b"binary data 2"},
        ]
    )

    # Create schema with binary and vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 7))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("data", pa.binary()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_float_type() -> None:
    """Test add_vector_column with float type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "score": 3.14159},
            {"id": 2, "score": 2.71828},
        ]
    )

    # Create schema with float and vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 8))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("score", pa.float64()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_string_type() -> None:
    """Test add_vector_column with string type."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "description": "Hello World"},
            {"id": 2, "description": "Goodbye World"},
        ]
    )

    # Create schema with string and vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 9))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("description", pa.string()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 3
    assert result["vector"].null_count == 2


def test_add_vector_column_vector_already_exists() -> None:
    """Test add_vector_column when vector column already exists in records."""
    # Create table with vector column already present
    records = pa.Table.from_pylist(
        [
            {"id": 1, "vector": [1.0, 2.0, 3.0]},
            {"id": 2, "vector": [4.0, 5.0, 6.0]},
        ]
    )

    # Create schema with vector column
    vector_field = pa.field("vector", pa.list_(pa.float32(), 3))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    # Should return the original table unchanged
    assert result is records
    assert result.num_columns == 2
    assert result["vector"].null_count == 0  # Original data preserved


def test_add_vector_column_vector_not_in_schema() -> None:
    """Test add_vector_column when vector column is not in target schema."""
    records = pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
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
    assert result.num_columns == 2
    assert "vector" not in result.schema.names


def test_add_vector_column_complex_schema() -> None:
    """Test add_vector_column with complex schema containing multiple types."""
    records = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "name": "Alice",
                "age": 30,
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
                "age": 25,
                "active": False,
                "score": 87.2,
                "birth_date": date(1995, 5, 15),
                "created_at": datetime(2023, 1, 2, 12, 0, 0),
                "data": b"more binary",
                "amount": Decimal("678.90"),
            },
        ]
    )

    # Create complex schema with all types
    vector_field = pa.field("vector", pa.list_(pa.float32(), 11))
    table_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int64()),
            pa.field("active", pa.bool_()),
            pa.field("score", pa.float64()),
            pa.field("birth_date", pa.date32()),
            pa.field("created_at", pa.timestamp("us")),
            pa.field("data", pa.binary()),
            pa.field("amount", pa.decimal128(10, 2)),
            vector_field,
        ]
    )

    result = add_vector_column(records, table_schema, "vector")

    assert "vector" in result.schema.names
    assert result.num_columns == 10
    assert result.num_rows == 2
    assert result["vector"].null_count == 2

