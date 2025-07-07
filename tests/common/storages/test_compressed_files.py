import os
import gzip
import pytest
from typing import Type, TypeVar
from unittest.mock import patch

from dlt.common.data_writers import DataWriter, BufferedDataWriter
from dlt.common.data_writers.writers import (
    JsonlWriter,
    CsvWriter,
    ParquetDataWriter,
    FileWriterSpec,
    InsertValuesWriter,
)
from dlt.common.storages import PackageStorage, ParsedLoadJobFileName
from dlt.common.schema.utils import new_column

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage
from tests.common.data_writers.utils import get_writer

TW = TypeVar("TW", bound=DataWriter)


def get_writer_spec(writer_type: Type[DataWriter], compression: bool = True) -> FileWriterSpec:
    """Get a writer spec for the given writer type with compression settings."""
    if writer_type == JsonlWriter:
        return FileWriterSpec(
            file_format="jsonl",
            data_item_format="file",
            file_extension="jsonl",
            supports_compression=compression,
            is_binary_format=False,
            supports_schema_changes="True",
        )
    elif writer_type == CsvWriter:
        return FileWriterSpec(
            file_format="csv",
            data_item_format="file",
            file_extension="csv",
            supports_compression=compression,
            is_binary_format=False,
            supports_schema_changes="True",
        )
    elif writer_type == ParquetDataWriter:
        return FileWriterSpec(
            file_format="parquet",
            data_item_format="file",
            file_extension="parquet",
            supports_compression=compression,
            is_binary_format=True,
            supports_schema_changes="True",
        )
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")


def create_buffered_writer(
    writer_type: Type[DataWriter],
    disable_compression: bool = False,
    buffer_max_items: int = 10,
    file_name_template: str = "test_%s",
) -> BufferedDataWriter[TW]:
    """Create a buffered data writer for testing."""
    spec = get_writer_spec(writer_type, compression=not disable_compression)
    return BufferedDataWriter(
        spec,
        file_name_template,
        buffer_max_items=buffer_max_items,
        disable_compression=disable_compression,
    )


# ParsedLoadJobFileName parsing and creation tests
@pytest.mark.parametrize(
    "compressed",
    [True, False],
)
def test_parse_file_names(compressed: bool) -> None:
    """Test parsing file names with or without .gz extension."""
    if compressed:
        file_name = "table_name.file123.0.jsonl.gz"
    else:
        file_name = "table_name.file123.0.jsonl"
    parsed = ParsedLoadJobFileName.parse(file_name)
    assert parsed.table_name == "table_name"
    assert parsed.file_id == "file123"
    assert parsed.retry_count == 0
    assert parsed.file_format == "jsonl"
    assert compressed == parsed.is_compressed


@pytest.mark.parametrize(
    "compressed",
    [True, False],
)
def test_file_name_generation(compressed: bool) -> None:
    """Test generating file names with and without compression."""
    # Test compressed file name generation
    parsed = ParsedLoadJobFileName("table", "file123", 0, "jsonl", compressed)
    if compressed:
        assert parsed.file_name() == "table.file123.0.jsonl.gz"
    else:
        assert parsed.file_name() == "table.file123.0.jsonl"


def test_with_retry_preserves_compression():
    """Test that retry count increment preserves compression flag."""
    # Test compressed file retry
    parsed = ParsedLoadJobFileName("table", "file123", 0, "jsonl", True)
    retried = parsed.with_retry()
    assert retried.retry_count == 1
    assert retried.is_compressed is True
    assert retried.file_name() == "table.file123.1.jsonl.gz"

    # Test uncompressed file retry
    parsed = ParsedLoadJobFileName("table", "file123", 0, "jsonl", False)
    retried = parsed.with_retry()
    assert retried.retry_count == 1
    assert retried.is_compressed is False
    assert retried.file_name() == "table.file123.1.jsonl"


@pytest.mark.parametrize(
    "invalid_file_name",
    [
        "table.file.0.jsonl.gz.extra",
        "table.file.jsonl",
        "table.file.0.jsonl.zip",
    ],
)
def test_parse_invalid_file_names(invalid_file_name: str) -> None:
    """Test that invalid file names raise appropriate errors."""
    with pytest.raises(Exception):
        ParsedLoadJobFileName.parse(invalid_file_name)


@pytest.mark.parametrize(
    "compressed",
    [True, False],
)
def test_build_compressed_job_file_name(compressed: bool) -> None:
    """Test building job file names with compression enabled."""
    with patch("dlt.destinations.utils.is_compression_disabled", return_value=not compressed):
        file_name = PackageStorage.build_job_file_name(
            "test_table", "file123", 0, loader_file_format="jsonl"
        )
        if compressed:
            assert file_name == "test_table.file123.0.jsonl.gz"
        else:
            assert file_name == "test_table.file123.0.jsonl"


def test_build_job_file_name_no_format() -> None:
    """Test building job file names without specifying format."""
    # When no format is specified, no extension should be added
    file_name = PackageStorage.build_job_file_name(
        "test_table", "file123", 0, loader_file_format=None
    )
    assert file_name == "test_table.file123.0"


@pytest.mark.parametrize(
    "compressed",
    [True, False],
)
@pytest.mark.parametrize("writer_type", [JsonlWriter, CsvWriter, InsertValuesWriter])
def test_compression_by_data_writer(writer_type: Type[DataWriter], compressed: bool) -> None:
    """Test that compressed files get .gz extension."""
    # Create writer with compression enabled and write some data
    with get_writer(writer_type, disable_compression=not compressed) as writer:
        columns = {"id": new_column("id", "text"), "value": new_column("value", "bigint")}
        data = [{"id": "1", "value": 100}, {"id": "2", "value": 200}]
        writer.write_data_item(data, columns)

    # Check that file was created with .gz extension
    assert len(writer.closed_files) == 1
    file_path = writer.closed_files[0].file_path
    if compressed:
        assert file_path.endswith(".gz")
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            content = f.read()
            assert len(content) > 0
    else:
        assert not file_path.endswith(
            ".gz",
        )
        with open(file_path, "rt", encoding="utf-8") as f:
            content = f.read()
            assert len(content) > 0


def test_import_uncompressed_file() -> None:
    """Test importing uncompressed files with compression handling."""
    # Create a test file to import
    test_file = os.path.join(TEST_STORAGE_ROOT, "test_import.jsonl")
    with open(test_file, "w", encoding="utf-8") as f:
        f.write('{"id": 1, "value": "test"}\n')

    # Create writer with compression enabled
    with get_writer(JsonlWriter, disable_compression=False) as writer:
        # Import the file
        from dlt.common.metrics import DataWriterMetrics

        metrics = writer.import_file(test_file, DataWriterMetrics("", 1, 100, 0, 0))
        # Check that imported file doesn't have .gz extension (imports are not compressed)
        assert not metrics.file_path.endswith(".gz")

    os.remove(test_file)
