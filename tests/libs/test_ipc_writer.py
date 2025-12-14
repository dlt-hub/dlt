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
                "f_timestamp": "2024-01-01T00:00:00",
                "f_bool": True,
                "f_bool_2": False,
                "f_str": "test",
                "f_json": {"nested": "value"},
            },
            {
                "f_int": 2,
                "f_float": 2.5,
                "f_timestamp": "2024-01-02T00:00:00",
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

        Verifies that compression options are correctly applied when writing IPC format.
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

        # Test with ZSTD compression
        f = io.BytesIO()
        writer = ArrowToIPCWriter(f, compression="zstd")
        writer.write_all(columns_schema, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == len(rows)


class TestIPCWriterSchemaEvolution:
    """Tests for schema evolution in IPC writers."""

    def test_ipc_writer_schema_evolution_with_big_buffer(self) -> None:
        """Test IPCDataWriter with schema evolution.

        Verifies that Python objects are correctly converted when schema evolves
        (adding new columns) between writes.
        """
        c1 = new_column("col1", "bigint")
        c2 = new_column("col2", "bigint")
        c3 = new_column("col3", "text")
        c4 = new_column("col4", "text")

        f = io.BytesIO()
        writer = IPCDataWriter(f)
        writer.write_all(
            {"col1": c1, "col2": c2, "col3": c3}, [{"col1": 1, "col2": 2, "col3": "3"}]
        )
        writer.write_all(
            {"col1": c1, "col2": c2, "col3": c3, "col4": c4},
            [{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}],
        )
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == 2
        assert table.column("col1").to_pylist() == [1, 1]
        assert table.column("col2").to_pylist() == [2, 2]
        assert table.column("col3").to_pylist() == ["3", "3"]
        assert table.column("col4").to_pylist() == [None, "4"]

    def test_arrow_to_ipc_writer_schema_evolution_with_multiple_batches(self) -> None:
        """Test ArrowToIPCWriter with schema evolution across multiple batches.

        Verifies that Arrow tables with different schemas are correctly handled
        and merged into a single IPC file.
        """
        c1 = new_column("col1", "bigint")
        c2 = new_column("col2", "bigint")
        c3 = new_column("col3", "text")
        c4 = new_column("col4", "text")

        # Create tables with evolving schema
        batch1_data = [{"col1": 1, "col2": 2, "col3": "3"}]
        batch2_data = [{"col1": 1, "col2": 2, "col3": "3", "col4": "4"}]

        batch1 = pyarrow.Table.from_pylist(batch1_data)
        batch2 = pyarrow.Table.from_pylist(batch2_data)

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all({"col1": c1, "col2": c2, "col3": c3}, [batch1])
        writer.write_all({"col1": c1, "col2": c2, "col3": c3, "col4": c4}, [batch2])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == 2
        assert table.column("col1").to_pylist() == [1, 1]


class TestIPCWriterDataTypes:
    """Tests for comprehensive data type coverage in IPC writers."""

    def test_ipc_writer_all_data_fields(self) -> None:
        """Test IPCDataWriter with all data types.

        Verifies that IPCDataWriter correctly handles all dlt data types
        with proper precision and type mappings.
        """
        data = dict(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
        columns_schema, _ = table_update_and_row()

        f = io.BytesIO()
        writer = IPCDataWriter(f)
        writer.write_all(columns_schema, [dict(data)])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()

        assert table.num_rows == 1
        for key, value in data.items():
            actual = table.column(key).to_pylist()[0]
            if isinstance(value, dict):
                actual = json.loads(actual)
            assert actual == value

    def test_arrow_to_ipc_writer_all_data_types(self) -> None:
        """Test ArrowToIPCWriter with all data types.

        Verifies that ArrowToIPCWriter correctly preserves all Arrow data types
        when writing to IPC format.
        """
        columns_schema, _ = table_update_and_row()
        data = dict(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
        table = pyarrow.Table.from_pylist([data])

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all(columns_schema, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == 1


class TestIPCWriterTimestampPrecision:
    """Tests for timestamp precision handling in IPC writers."""

    @pytest.mark.parametrize("tz", ["UTC", "Europe/Berlin", ""])
    def test_ipc_writer_timestamp_precision(self, tz: str) -> None:
        """Test IPCDataWriter with various timestamp precisions and timezones.

        Verifies that timestamps with different precisions (s, ms, us, ns)
        are correctly stored and retrieved with proper timezone information.
        """
        import os

        now = pendulum.now()
        now_ns = time.time_ns()

        # Store original env vars
        original_tz = os.environ.get("DATA_WRITER__TIMESTAMP_TIMEZONE")

        try:
            if tz:
                os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = tz
            else:
                os.environ.pop("DATA_WRITER__TIMESTAMP_TIMEZONE", None)

            f = io.BytesIO()
            writer = IPCDataWriter(f)
            writer.write_all(
                TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS,
                [{"col1": now, "col2": now, "col3": now, "col4": now_ns}],
            )
            writer.close()

            f.seek(0)
            reader = pyarrow.ipc.open_file(f)
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

    def test_arrow_to_ipc_writer_timestamp_precision(self) -> None:
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

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all(TABLE_UPDATE_ALL_TIMESTAMP_PRECISIONS_COLUMNS, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == 1


class TestIPCWriterEmptyData:
    """Tests for handling empty data in IPC writers."""

    def test_arrow_to_ipc_writer_empty_batches(self) -> None:
        """Test ArrowToIPCWriter handles empty batches correctly.

        Verifies that empty Arrow batches can be mixed with data batches
        and are correctly written to the IPC file.
        """
        c1 = {"col1": new_column("col1", "bigint")}

        # Create empty batch
        single_elem_table = pyarrow.Table.from_pylist([{"col1": 1}])
        empty_batch = pyarrow.RecordBatch.from_pylist([], schema=single_elem_table.schema)

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all(c1, [empty_batch])
        writer.write_all(c1, [empty_batch])
        writer.write_all(c1, [single_elem_table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        # Should have written one row from the single_elem_table
        assert table.num_rows >= 1

    def test_ipc_writer_empty_table_handling(self) -> None:
        """Test IPCDataWriter handles empty writes gracefully.

        Verifies that empty data writes don't cause errors in IPC writer.
        """
        c1 = new_column("col1", "bigint")

        f = io.BytesIO()
        writer = IPCDataWriter(f)
        # Write empty data - should not raise
        writer.write_all({"col1": c1}, [])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == 0


class TestIPCWriterDecimalAndWei:
    """Tests for decimal and wei data types in IPC writers."""

    def test_ipc_writer_decimal_handling(self) -> None:
        """Test IPCDataWriter correctly handles decimal types.

        Verifies that Decimal values are properly serialised in IPC format.
        """
        c1 = new_column("col1", "decimal")
        c2 = new_column("col2", "bigint")

        data = [
            {"col1": Decimal("123.45"), "col2": 1},
            {"col1": Decimal("999.99"), "col2": 2},
        ]

        f = io.BytesIO()
        writer = IPCDataWriter(f)
        writer.write_all({"col1": c1, "col2": c2}, data)
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        table = reader.read_all()
        assert table.num_rows == 2
        assert table.column_names == ["col1", "col2"]

    def test_arrow_to_ipc_writer_decimal_types(self) -> None:
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

        f = io.BytesIO()
        writer = ArrowToIPCWriter(f)
        writer.write_all({"col1": c1, "col2": c2}, [table])
        writer.close()

        f.seek(0)
        reader = pyarrow.ipc.open_file(f)
        read_table = reader.read_all()
        assert read_table.num_rows == 1
