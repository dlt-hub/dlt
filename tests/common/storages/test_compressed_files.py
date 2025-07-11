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
from dlt.common.data_writers.exceptions import CompressionConfigMismatchException
from dlt.common.storages import PackageStorage, ParsedLoadJobFileName
from dlt.common.schema.utils import new_column

from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage, preserve_environ
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
    assert compressed == parsed.has_compression_ext


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
    assert retried.has_compression_ext is True
    assert retried.file_name() == "table.file123.1.jsonl.gz"

    # Test uncompressed file retry
    parsed = ParsedLoadJobFileName("table", "file123", 0, "jsonl", False)
    retried = parsed.with_retry()
    assert retried.retry_count == 1
    assert retried.has_compression_ext is False
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


def test_build_job_file_name_no_format() -> None:
    """Test building job file names without specifying format."""
    # When no format is specified, no extension should be added
    file_name = PackageStorage.build_job_file_name(
        "test_table", "file123", 0, loader_file_format=None
    )
    assert file_name == "test_table.file123.0"
