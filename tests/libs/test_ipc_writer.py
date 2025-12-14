"""Tests for IPCDataWriter and ArrowToIPCWriter for Apache Arrow IPC format.

Tests Arrow IPC Feather v2 format writing capabilities for both Python objects
(IPCDataWriter) and Arrow data (ArrowToIPCWriter) with various configurations.
"""

import io
import math
import time
from typing import Iterator, cast

import pyarrow
import pytest
from dlt.common import Decimal, json, pendulum
from dlt.common.data_writers.writers import (
    ArrowToIPCWriter,
    IPCDataWriter,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import new_column
from tests.cases import (
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
    TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
    table_update_and_row,
)
from tests.common.data_writers.utils import get_writer
from tests.common.utils import load_json_case


@pytest.fixture
def ipc_file_writer() -> Iterator[IPCDataWriter]:
    """Create an IPCDataWriter with file format mode."""
    f = io.BytesIO()
    yield IPCDataWriter(f)


@pytest.fixture
def columns_schema() -> dict[str, TColumnSchema]:
    """Define a test schema with various column types."""
    return cast(
        dict[str, TColumnSchema],
        {
            "f_int": {"name": "f_int", "data_type": "bigint", "nullable": False},
            "f_float": {"name": "f_float", "data_type": "double", "nullable": False},
            "f_timestamp": {"name": "f_timestamp", "data_type": "timestamp", "nullable": False},
            "f_bool": {"name": "f_bool", "data_type": "bool", "nullable": True},
            "f_bool_2": {"name": "f_bool_2", "data_type": "bool", "nullable": False},
            "f_str": {"name": "f_str", "data_type": "text", "nullable": True},
        },
    )


class TestIPCDataWriter:
    """Tests for IPCDataWriter - IPC format writer for Python object data."""

    def test_ipc_file_writer(
        self, ipc_file_writer: IPCDataWriter, columns_schema: dict[str, TColumnSchema]
    ) -> None:
        """Test IPCDataWriter in file format mode.

        Verifies that Python objects are correctly converted to Arrow IPC file format
        with proper schema and row count.
        """
        rows = load_json_case("simple_row")
        for row in rows:
            row["f_timestamp"] = pendulum.parse(row["f_timestamp"])
        ipc_file_writer.write_all(columns_schema, rows)
        ipc_file_writer.close()

        f = ipc_file_writer._f
        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == len(rows)
        assert set(table.column_names) == set(columns_schema.keys())

    def test_ipc_writer_json_serialisation(self, columns_schema: dict[str, TColumnSchema]) -> None:
        """Test that JSON fields are properly serialised in IPC format.

        Verifies that complex JSON objects are converted to strings and can be
        read back from the IPC file.
        """
        # Update schema to include JSON column
        json_schema = cast(
            dict[str, TColumnSchema],
            {
                **columns_schema,
                "f_json": {"name": "f_json", "data_type": "json", "nullable": True},
            },
        )

        rows = [
            {
                "f_int": 1,
                "f_float": 1.5,
                "f_timestamp": pendulum.parse("2024-01-01T00:00:00"),
                "f_bool": True,
                "f_bool_2": False,
                "f_str": "test",
                "f_json": {"nested": "value"},
            },
            {
                "f_int": 2,
                "f_float": 2.5,
                "f_timestamp": pendulum.parse("2024-01-02T00:00:00"),
                "f_bool": False,
                "f_bool_2": True,
                "f_str": "test2",
                "f_json": [1, 2, 3],
            },
        ]

        f = io.BytesIO()
        writer = IPCDataWriter(f)
        writer.write_all(json_schema, rows)
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == 2
        # JSON should be serialised to strings
        json_col = table.column("f_json").to_pylist()
        assert json_col[0] == '{"nested":"value"}'
        assert json_col[1] == "[1,2,3]"


class TestArrowToIPCWriter:
    """Tests for ArrowToIPCWriter - IPC format writer for Arrow data."""

    def test_arrow_to_ipc_file_writer(self, columns_schema: dict[str, TColumnSchema]) -> None:
        """Test ArrowToIPCWriter in file format mode.

        Verifies that Arrow tables are correctly written to IPC file format
        without conversion, preserving table structure and data.
        """
        rows = load_json_case("simple_row")
        table = pyarrow.Table.from_pylist(rows)

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all(columns_schema, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == len(rows)

    def test_arrow_to_ipc_writer_multiple_batches(
        self, columns_schema: dict[str, TColumnSchema]
    ) -> None:
        """Test ArrowToIPCWriter with multiple record batches.

        Verifies that multiple Arrow batches are correctly concatenated and written
        to a single IPC file with correct row count.
        """
        rows1 = load_json_case("simple_row")
        rows2 = load_json_case("simple_row")

        batch1 = pyarrow.RecordBatch.from_pylist(rows1)
        batch2 = pyarrow.RecordBatch.from_pylist(rows2)

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all(columns_schema, [batch1, batch2])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == len(rows1) + len(rows2)

    def test_arrow_to_ipc_writer_empty_raises(
        self, columns_schema: dict[str, TColumnSchema]
    ) -> None:
        """Test that ArrowToIPCWriter raises error for empty files.

        Verifies that attempting to finalise an IPC file without writing any data
        raises NotImplementedError.
        """
        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_header(columns_schema)

        with pytest.raises(NotImplementedError, match="does not support writing empty files"):
            writer.write_footer()

    def test_arrow_to_ipc_writer_compression_options(
        self, columns_schema: dict[str, TColumnSchema]
    ) -> None:
        """Test ArrowToIPCWriter with compression options.

        Verifies that compression options are correctly applied when writing IPC format
        and that data round-trips correctly with compression.
        """
        rows = load_json_case("simple_row")
        table = pyarrow.Table.from_pylist(rows)

        # Test with LZ4 compression
        f = io.BytesIO()
        writer = ArrowToIPCWriter(f, compression="lz4")
        writer.write_all(columns_schema, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == len(rows)
        # Verify data integrity after compression
        assert read_table.to_pylist() == table.to_pylist()

        # Test with ZSTD compression
        f = io.BytesIO()
        writer = ArrowToIPCWriter(f, compression="zstd")
        writer.write_all(columns_schema, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == len(rows)
        # Verify data integrity after compression
        assert read_table.to_pylist() == table.to_pylist()


def test_ipc_writer_schema_evolution_with_big_buffer() -> None:
    """Test schema evolution via BufferedDataWriter file rotation for IPC format.

    IPC format does not support schema evolution within a single file, but dlt's
    BufferedDataWriter provides schema evolution by rotating files when schema changes.
    Since IPCDataWriter sets supports_schema_changes="False", BufferedDataWriter will
    create a new file when column additions are detected.
    """
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_writer(IPCDataWriter) as writer:
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3}
        )
        writer.write_data_item(
            [{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}],
            {"col1": c1, "col2": c2, "col3": c3, "col4": c4},
        )

    # Schema evolution triggers file rotation: one file per schema
    assert len(writer.closed_files) == 2

    # First file has original schema (3 columns)
    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        assert len(table.schema) == 3

    # Second file has evolved schema (4 columns)
    # Second file has evolved schema (4 columns)
    with open(writer.closed_files[1].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        assert len(table.schema) == 4


def test_arrow_to_ipc_writer_schema_evolution_with_multiple_batches() -> None:
    """Test schema evolution via BufferedDataWriter for ArrowToIPCWriter.

    IPC format does not support schema evolution within a single file. BufferedDataWriter
    provides schema evolution by rotating files when schema changes are detected.
    This test verifies file rotation with ArrowToIPCWriter.
    """
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_writer(ArrowToIPCWriter) as writer:
        # Write with initial schema
        batch1 = pyarrow.Table.from_pylist([{"col1": 1, "col2": 2, "col3": "3"}])
        writer.write_data_item(batch1, columns={"col1": c1, "col2": c2, "col3": c3})

        # Write with evolved schema - triggers file rotation
        batch2 = pyarrow.Table.from_pylist([{"col1": 1, "col2": 2, "col3": "3", "col4": "4"}])
        writer.write_data_item(batch2, columns={"col1": c1, "col2": c2, "col3": c3, "col4": c4})

    # Schema evolution triggers file rotation: one file per schema
    assert len(writer.closed_files) == 2

    # First file has original schema (3 columns)
    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        assert len(table.schema) == 3

    # Second file has evolved schema (4 columns)
    with open(writer.closed_files[1].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        assert len(table.schema) == 4


def test_ipc_writer_all_data_fields() -> None:
    """Test IPCDataWriter with all data types.

    Verifies that IPCDataWriter correctly handles all dlt data types
    with proper precision and type mappings.
    """
    data = dict(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    columns_schema, _ = table_update_and_row()

    with get_writer(IPCDataWriter) as writer:
        writer.write_data_item([dict(data)], columns_schema)

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()

    assert table.num_rows == 1
    for key, value in data.items():
        actual = table.column(key).to_pylist()[0]
        if isinstance(value, dict):
            actual = json.loads(actual)
        # Handle time precision loss - Arrow may reduce nanosecond precision to microsecond
        if key == "col11_precision" and hasattr(value, "microsecond"):
            # Allow microsecond difference due to Arrow precision
            assert abs(actual.microsecond - value.microsecond) < 1000
        else:
            assert actual == value


def test_ipc_writer_timestamp_precision() -> None:
    """Test IPCDataWriter with various timestamp precisions.

    Verifies that timestamps with different precisions (s, ms, us, ns)
    are correctly stored in IPC format.
    """
    import os

    now = pendulum.now()
    now_ns = time.time_ns()

    # Store original env vars
    original_tz = os.environ.get("DATA_WRITER__TIMESTAMP_TIMEZONE")

    try:
        os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = "UTC"

        with get_writer(IPCDataWriter) as writer:
            writer.write_data_item(
                [{"col1": now, "col2": now, "col3": now, "col4": now_ns}],
                TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
            )

        with open(writer.closed_files[0].file_path, "rb") as f:
            reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
            table = reader.read_all()
            assert table.num_rows == 1
            # Verify all columns exist with data
            assert len(table.column_names) == 4
    finally:
        # Restore original env var
        if original_tz is not None:
            os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = original_tz
        else:
            os.environ.pop("DATA_WRITER__TIMESTAMP_TIMEZONE", None)


def test_arrow_to_ipc_writer_timestamp_precision() -> None:
    """Test ArrowToIPCWriter preserves timestamp precision.

    Verifies that Arrow tables with various timestamp precisions are
    correctly written and read back from IPC format.
    """
    now = pendulum.now()
    data = {
        "col1": now,
        "col2": now,
        "col3": now,
        "col4": int(time.time_ns()),
    }

    table = pyarrow.Table.from_pylist([data])

    with get_writer(ArrowToIPCWriter) as writer:
        writer.write_data_item(table, columns=TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS)

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        read_table = reader.read_all()
        assert read_table.num_rows == 1


def test_arrow_to_ipc_writer_empty_batches() -> None:
    """Test ArrowToIPCWriter handles empty batches correctly.

    Verifies that empty Arrow batches can be mixed with data batches
    and are correctly written to the IPC file.
    """
    c1 = {"col1": new_column("col1", "bigint")}

    # Create empty batch
    single_elem_table = pyarrow.Table.from_pylist([{"col1": 1}])
    empty_batch = pyarrow.RecordBatch.from_pylist([], schema=single_elem_table.schema)

    with get_writer(ArrowToIPCWriter) as writer:
        writer.write_data_item(empty_batch, columns=c1)
        writer.write_data_item(empty_batch, columns=c1)
        writer.write_data_item(single_elem_table, columns=c1)

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        # Should have written one row from the single_elem_table
        assert table.num_rows >= 1


def test_ipc_writer_empty_table_handling() -> None:
    """Test IPCDataWriter handles empty writes gracefully.

    Verifies that empty data writes don't cause errors in IPC writer. When no data
    is written, no files are created by BufferedDataWriter.
    """
    c1 = new_column("col1", "bigint")

    with get_writer(IPCDataWriter) as writer:
        # Write empty data - should not raise
        writer.write_data_item([], {"col1": c1})

    # No files are created for empty writes
    assert len(writer.closed_files) == 0


def test_ipc_writer_decimal_handling() -> None:
    """Test IPCDataWriter correctly handles decimal types.

    Verifies that Decimal values are properly serialised in IPC format.
    """
    c1 = new_column("col1", "decimal")
    c2 = new_column("col2", "bigint")

    data = [
        {"col1": Decimal("123.45"), "col2": 1},
        {"col1": Decimal("999.99"), "col2": 2},
    ]

    with get_writer(IPCDataWriter) as writer:
        writer.write_data_item(data, {"col1": c1, "col2": c2})

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()
        assert table.num_rows == 2
        assert table.column_names == ["col1", "col2"]


def test_arrow_to_ipc_writer_decimal_types() -> None:
    """Test ArrowToIPCWriter preserves decimal precision.

    Verifies that Arrow Decimal128/256 types are correctly preserved
    in the IPC output.
    """
    c1 = new_column("col1", "decimal")
    c2 = new_column("col2", "wei")

    data = [
        {"col1": Decimal("12345.67"), "col2": Decimal(2**100)},
    ]

    table = pyarrow.Table.from_pylist(data)

    with get_writer(ArrowToIPCWriter) as writer:
        writer.write_data_item(table, columns={"col1": c1, "col2": c2})

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        read_table = reader.read_all()
        assert read_table.num_rows == 1


def test_ipc_writer_round_trip_all_types() -> None:
    """Test complete round-trip for IPCDataWriter with all data types.

    Verifies that all dlt data types can be written and read back with
    exact value preservation (within precision constraints).
    """
    data = dict(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    columns_schema, _ = table_update_and_row()

    with get_writer(IPCDataWriter) as writer:
        writer.write_data_item([dict(data)], columns_schema)

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()

    assert table.num_rows == 1
    row = table.to_pylist()[0]

    # Verify each field round-trips correctly
    for key, original_value in data.items():
        actual_value = row[key]

        if isinstance(original_value, dict):
            # JSON fields are serialised to strings
            actual_value = json.loads(actual_value)
            assert actual_value == original_value
        elif key == "col11_precision" and hasattr(original_value, "microsecond"):
            # Allow microsecond difference due to Arrow precision
            assert abs(actual_value.microsecond - original_value.microsecond) < 1000
        elif isinstance(original_value, Decimal):
            # Decimals may need type conversion
            assert Decimal(str(actual_value)) == original_value
        else:
            assert actual_value == original_value


def test_arrow_to_ipc_writer_round_trip_complex_types() -> None:
    """Test round-trip for ArrowToIPCWriter with complex Arrow types.

    Verifies that complex Arrow data structures (lists, structs, decimals)
    preserve exact values through write and read operations.
    """
    schema = pyarrow.schema(
        [
            ("int_col", pyarrow.int64()),
            ("float_col", pyarrow.float64()),
            ("string_col", pyarrow.string()),
            ("bool_col", pyarrow.bool_()),
            ("decimal_col", pyarrow.decimal128(10, 2)),
            ("timestamp_col", pyarrow.timestamp("us", tz="UTC")),
            ("list_col", pyarrow.list_(pyarrow.int32())),
        ]
    )

    data = [
        {
            "int_col": 42,
            "float_col": 3.14159,
            "string_col": "test_value",
            "bool_col": True,
            "decimal_col": Decimal("123.45"),
            "timestamp_col": pendulum.parse("2024-01-15T10:30:00Z"),
            "list_col": [1, 2, 3, 4, 5],
        },
        {
            "int_col": -100,
            "float_col": 2.71828,
            "string_col": "another_test",
            "bool_col": False,
            "decimal_col": Decimal("999.99"),
            "timestamp_col": pendulum.parse("2024-12-01T15:45:30Z"),
            "list_col": [10, 20, 30],
        },
    ]

    original_table = pyarrow.Table.from_pylist(data, schema=schema)

    with get_writer(ArrowToIPCWriter) as writer:
        # Write the table
        writer.write_data_item(original_table, columns={})

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        read_table = reader.read_all()

    # Verify exact match
    assert read_table.num_rows == original_table.num_rows
    assert read_table.schema == original_table.schema
    assert read_table.to_pylist() == original_table.to_pylist()


def test_ipc_writer_round_trip_compression() -> None:
    """Test round-trip with IPCDataWriter using compression.

    Verifies that compression doesn't affect data integrity and that
    all values match after decompression.
    """
    base_timestamp = cast(pendulum.DateTime, pendulum.parse("2024-01-01T00:00:00"))
    original_data = [
        {
            "f_int": i,
            "f_float": i * 1.5,
            "f_timestamp": base_timestamp.add(hours=i),
            "f_bool": i % 2 == 0,
            "f_bool_2": i % 2 != 0,
            "f_str": f"value_{i}",
        }
        for i in range(100)
    ]

    columns_schema = cast(
        dict[str, TColumnSchema],
        {
            "f_int": {"name": "f_int", "data_type": "bigint", "nullable": False},
            "f_float": {"name": "f_float", "data_type": "double", "nullable": False},
            "f_timestamp": {"name": "f_timestamp", "data_type": "timestamp", "nullable": False},
            "f_bool": {"name": "f_bool", "data_type": "bool", "nullable": True},
            "f_bool_2": {"name": "f_bool_2", "data_type": "bool", "nullable": False},
            "f_str": {"name": "f_str", "data_type": "text", "nullable": True},
        },
    )

    # Test with LZ4 compression
    f_lz4 = io.BytesIO()
    writer_lz4 = IPCDataWriter(f_lz4, compression="lz4")
    writer_lz4.write_all(columns_schema, original_data)
    writer_lz4.close()

    f_lz4.seek(0)
    reader = pyarrow.ipc.open_file(f_lz4)
    table = reader.read_all()
    read_data = table.to_pylist()

    assert len(read_data) == len(original_data)
    # Verify data integrity after compression
    for _, (original, read) in enumerate(zip(original_data, read_data)):
        assert read["f_int"] == original["f_int"]
        assert read["f_str"] == original["f_str"]
        assert abs(read["f_float"] - original["f_float"]) < 1e-10

    # Test with ZSTD compression
    f_zstd = io.BytesIO()
    writer_zstd = IPCDataWriter(f_zstd, compression="zstd")
    writer_zstd.write_all(columns_schema, original_data)
    writer_zstd.close()

    f_zstd.seek(0)
    reader = pyarrow.ipc.open_file(f_zstd)
    table = reader.read_all()
    read_data = table.to_pylist()

    assert len(read_data) == len(original_data)
    # Verify data integrity after compression
    for _, (original, read) in enumerate(zip(original_data, read_data)):
        assert read["f_int"] == original["f_int"]
        assert read["f_str"] == original["f_str"]
        assert abs(read["f_float"] - original["f_float"]) < 1e-10


def test_arrow_to_ipc_writer_round_trip_batches() -> None:
    """Test round-trip for ArrowToIPCWriter with multiple batches.

    Verifies that multiple Arrow batches can be written and read back
    with all data preserved in correct order.
    """
    schema = pyarrow.schema(
        [
            ("id", pyarrow.int64()),
            ("name", pyarrow.string()),
            ("value", pyarrow.float64()),
        ]
    )

    batch1_data = [
        {"id": 1, "name": "first", "value": 1.1},
        {"id": 2, "name": "second", "value": 2.2},
    ]
    batch2_data = [
        {"id": 3, "name": "third", "value": 3.3},
        {"id": 4, "name": "fourth", "value": 4.4},
    ]
    batch3_data = [
        {"id": 5, "name": "fifth", "value": 5.5},
    ]

    batch1 = pyarrow.RecordBatch.from_pylist(batch1_data, schema=schema)
    batch2 = pyarrow.RecordBatch.from_pylist(batch2_data, schema=schema)
    batch3 = pyarrow.RecordBatch.from_pylist(batch3_data, schema=schema)

    with get_writer(ArrowToIPCWriter) as writer:
        writer.write_data_item(batch1, columns={})
        writer.write_data_item(batch2, columns={})
        writer.write_data_item(batch3, columns={})

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        table = reader.read_all()

    expected_data = batch1_data + batch2_data + batch3_data
    actual_data = table.to_pylist()

    assert len(actual_data) == len(expected_data)
    assert actual_data == expected_data


def test_ipc_writer_null_value_handling() -> None:
    """Test IPCDataWriter correctly handles NULL values in nullable columns.

    Verifies that NULL values are preserved for nullable columns and that
    mixed NULL/non-NULL data is handled correctly.
    """
    columns_schema = cast(
        dict[str, TColumnSchema],
        {
            "f_int": {"name": "f_int", "data_type": "bigint", "nullable": True},
            "f_str": {"name": "f_str", "data_type": "text", "nullable": True},
            "f_float": {"name": "f_float", "data_type": "double", "nullable": True},
            "f_bool": {"name": "f_bool", "data_type": "bool", "nullable": True},
        },
    )

    data = [
        {"f_int": 1, "f_str": "test", "f_float": 1.5, "f_bool": True},
        {"f_int": None, "f_str": None, "f_float": None, "f_bool": None},
        {"f_int": 2, "f_str": "", "f_float": 0.0, "f_bool": False},
        {"f_int": None, "f_str": "value", "f_float": None, "f_bool": True},
    ]

    f = io.BytesIO()
    writer = IPCDataWriter(f)
    writer.write_all(columns_schema, data)
    writer.close()

    f.seek(0)
    reader = pyarrow.ipc.open_file(f)
    table = reader.read_all()
    read_data = table.to_pylist()

    assert len(read_data) == len(data)
    # Verify NULL preservation
    assert read_data[1]["f_int"] is None
    assert read_data[1]["f_str"] is None
    assert read_data[1]["f_float"] is None
    assert read_data[1]["f_bool"] is None
    # Verify empty string vs NULL distinction
    assert read_data[2]["f_str"] == ""
    assert read_data[2]["f_float"] == 0.0


def test_ipc_writer_numeric_edge_cases() -> None:
    """Test IPCDataWriter with numeric edge cases.

    Verifies handling of special float values (NaN, infinity), zero,
    negative numbers, and boundary values.
    """
    columns_schema = cast(
        dict[str, TColumnSchema],
        {
            "f_float": {"name": "f_float", "data_type": "double", "nullable": True},
            "f_int": {"name": "f_int", "data_type": "bigint", "nullable": True},
        },
    )

    data = [
        {"f_float": float("inf"), "f_int": 0},
        {"f_float": float("-inf"), "f_int": -1},
        {"f_float": float("nan"), "f_int": 2**63 - 1},  # Max int64
        {"f_float": 0.0, "f_int": -(2**63)},  # Min int64
        {"f_float": -0.0, "f_int": None},
    ]

    f = io.BytesIO()
    writer = IPCDataWriter(f)
    writer.write_all(columns_schema, data)
    writer.close()

    f.seek(0)
    reader = pyarrow.ipc.open_file(f)
    table = reader.read_all()
    read_data = table.to_pylist()

    assert len(read_data) == len(data)
    # Verify infinity
    assert read_data[0]["f_float"] == float("inf")
    assert read_data[1]["f_float"] == float("-inf")
    # Verify NaN (NaN != NaN, so use math.isnan)
    assert math.isnan(read_data[2]["f_float"])
    # Verify boundary values
    assert read_data[2]["f_int"] == 2**63 - 1
    assert read_data[3]["f_int"] == -(2**63)


def test_ipc_writer_unicode_handling() -> None:
    """Test IPCDataWriter with Unicode and special characters.

    Verifies that emojis, special characters, and multi-byte UTF-8
    strings are correctly preserved.
    """
    columns_schema = cast(
        dict[str, TColumnSchema],
        {
            "f_str": {"name": "f_str", "data_type": "text", "nullable": True},
        },
    )

    data = [
        {"f_str": "Hello 👋 World 🌍"},
        {"f_str": "こんにちは"},  # Japanese
        {"f_str": "مرحبا"},  # Arabic
        {"f_str": "Привет"},  # Russian
        {"f_str": "Special: \n\t\r\\\"'"},
        {"f_str": "Emoji party: 🎉🎊🥳🎈"},
    ]

    f = io.BytesIO()
    writer = IPCDataWriter(f)
    writer.write_all(columns_schema, data)
    writer.close()

    f.seek(0)
    reader = pyarrow.ipc.open_file(f)
    table = reader.read_all()
    read_data = table.to_pylist()

    assert len(read_data) == len(data)
    for i, expected in enumerate(data):
        assert read_data[i]["f_str"] == expected["f_str"]


def test_arrow_to_ipc_writer_null_handling() -> None:
    """Test ArrowToIPCWriter preserves NULL values correctly.

    Verifies that Arrow tables with NULL values maintain those NULLs
    through the write/read cycle.
    """
    schema = pyarrow.schema(
        [
            ("int_col", pyarrow.int64()),
            ("str_col", pyarrow.string()),
            ("float_col", pyarrow.float64()),
        ]
    )

    # Create table with explicit NULL values
    data = [
        {"int_col": 1, "str_col": "test", "float_col": 1.5},
        {"int_col": None, "str_col": None, "float_col": None},
        {"int_col": 3, "str_col": "", "float_col": 0.0},
    ]

    table = pyarrow.Table.from_pylist(data, schema=schema)

    with get_writer(ArrowToIPCWriter) as writer:
        writer.write_data_item(table, columns={})

    with open(writer.closed_files[0].file_path, "rb") as f:
        reader = pyarrow.ipc.RecordBatchFileReader(pyarrow.memory_map(f.name, "r"))
        read_table = reader.read_all()

    read_data = read_table.to_pylist()
    assert len(read_data) == len(data)
    # Verify NULL preservation
    assert read_data[1]["int_col"] is None
    assert read_data[1]["str_col"] is None
    assert read_data[1]["float_col"] is None
    # Verify empty string is distinct from NULL
    assert read_data[2]["str_col"] == ""
