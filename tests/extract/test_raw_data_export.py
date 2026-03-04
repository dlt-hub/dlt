"""Tests for RawDataExporter and RawDataExportWrapper."""

import inspect
import json
import os
import posixpath
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.common.utils import uniq_id
from dlt.extract.raw_data_export import RawDataExporter, RawDataExportWrapper
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_file_item(
    relative_path: str, content: bytes = b"hello world", modification_date: str = "2026-01-01"
) -> FileItemDict:
    return FileItemDict(
        {
            "file_url": f"memory://source/{relative_path}",
            "file_name": posixpath.basename(relative_path),
            "relative_path": relative_path,
            "mime_type": "application/octet-stream",
            "modification_date": modification_date,
            "size_in_bytes": len(content),
            "file_content": content,
        }
    )


def _memory_fs() -> Any:
    from fsspec.implementations.memory import MemoryFileSystem

    fs = MemoryFileSystem()
    fs.store.clear()
    return fs


def _make_exporter(
    bucket_url: str, name_path: str, fs: Any = None, **kwargs: Any
) -> RawDataExporter:
    """Create a RawDataExporter with a pre-configured fsspec for testing.

    Sets fs_client directly since credentials are resolved via @configspec in production.
    """
    exp = RawDataExporter(bucket_url, name_path, **kwargs)
    if fs is not None:
        exp.fs_client = fs
    return exp


def _read_file(fs: Any, path: str) -> bytes:
    with fs.open(path, "rb") as f:
        return f.read()


def _list_files(fs: Any, path: str) -> List[str]:
    try:
        return sorted(fs.find(path))
    except FileNotFoundError:
        return []


def _pipeline(name_suffix: str = "") -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="test_rde_" + uniq_id() + name_suffix,
        destination="duckdb",
    )


# ---------------------------------------------------------------------------
# unit: RawDataExporter
# ---------------------------------------------------------------------------


def test_copy_creates_independent_instance() -> None:
    orig = RawDataExporter("s3://bucket", "relative_path", export_only=True)
    copied = orig.copy()
    assert copied.bucket_url == orig.bucket_url
    assert copied.name_path == orig.name_path
    assert copied.export_only == orig.export_only
    assert copied is not orig
    assert copied.fs_client is None
    assert copied._export_path == ""
    assert copied._disabled is False


def test_copy_preserves_fs_client() -> None:
    mock_fs = MagicMock()
    orig = RawDataExporter("s3://bucket", "relative_path")
    orig.fs_client = mock_fs
    orig._export_path = "s3://bucket/some/path"
    copied = orig.copy()
    assert copied.fs_client is mock_fs
    assert copied._export_path == ""


def test_export_path_empty_before_bind() -> None:
    exp = RawDataExporter("s3://bucket", "relative_path")
    assert exp.export_path == ""


def test_resolve_file_path_basic() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._export_path = "memory://backup/pipe/src/res"
    path = exp._resolve_file_path({"relative_path": "data/2026/file.csv", "other": "value"})
    assert path == "memory://backup/pipe/src/res/data/2026/file.csv"


def test_resolve_file_path_rejects_empty() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._export_path = "memory://backup/pipe/src/res"
    exp._resource_name = "test"
    with pytest.raises(ValueError, match="resolved to empty string"):
        exp._resolve_file_path({"relative_path": ""})


def test_resolve_file_path_rejects_traversal() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._export_path = "memory://backup/pipe/src/res"
    exp._resource_name = "test"
    with pytest.raises(ValueError, match="resolves outside export path"):
        exp._resolve_file_path({"relative_path": "../../etc/passwd"})


def test_resolve_file_path_strips_leading_slash() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._export_path = "memory://backup/pipe/src/res"
    path = exp._resolve_file_path({"relative_path": "/data/file.csv"})
    assert path == "memory://backup/pipe/src/res/data/file.csv"


def test_resolve_file_path_missing_key_raises() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._export_path = "memory://backup/pipe/src/res"
    exp._resource_name = "test"
    with pytest.raises(KeyError, match="name_path field 'relative_path' not found"):
        exp._resolve_file_path({"other_field": "value"})


def test_call_returns_none_for_none() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    assert exp(None) is None


def test_call_passes_through_when_disabled() -> None:
    exp = RawDataExporter("memory://backup", "relative_path")
    exp._disabled = True
    item = {"some": "dict"}
    assert exp(item) is item


def test_call_returns_none_when_disabled_and_export_only() -> None:
    exp = RawDataExporter("memory://backup", "relative_path", export_only=True)
    exp._disabled = True
    exp._resource_name = "test"
    assert exp({"some": "dict"}) is None


def test_call_buffers_dict_and_passes_through() -> None:
    fs = _memory_fs()
    exp = _make_exporter("memory://backup", "name", fs=fs)
    exp._resource_name = "test"
    exp._export_path = "memory://backup/test"
    exp._load_id = "123"
    item = {"name": "test_value", "data": 123}
    result = exp(item)
    assert result is item
    assert "test_value" in exp._writers
    assert len(exp._writers["test_value"]) == 1
    exp.teardown()
    files = _list_files(fs, "memory://backup/test/test_value")
    assert len(files) == 1


def test_call_consumes_dict_when_export_only() -> None:
    fs = _memory_fs()
    exp = _make_exporter("memory://backup", "name", fs=fs, export_only=True)
    exp._resource_name = "test"
    exp._export_path = "memory://backup/test"
    exp._load_id = "123"
    assert exp({"name": "test_value", "data": 123}) is None


# ---------------------------------------------------------------------------
# unit: RawDataExportWrapper
# ---------------------------------------------------------------------------


def test_should_wrap_detects_default_value() -> None:
    def my_func(export=RawDataExporter("s3://bucket", "relative_path")):
        pass

    sig = inspect.signature(my_func)
    assert RawDataExportWrapper.should_wrap(sig) is True


def test_should_wrap_returns_false_for_no_export() -> None:
    def my_func(x: int, y: str = "hello"):
        pass

    sig = inspect.signature(my_func)
    assert RawDataExportWrapper.should_wrap(sig) is False


def test_wrap_copies_default() -> None:
    default_exp = RawDataExporter("s3://bucket", "relative_path")

    def my_func(export=default_exp):
        return export

    sig = inspect.signature(my_func)
    wrapper = RawDataExportWrapper()
    wrapped = wrapper.wrap(sig, my_func)
    result = wrapped()
    assert result is not default_exp
    assert isinstance(result, RawDataExporter)
    assert result.bucket_url == "s3://bucket"


def test_wrap_accepts_explicit_exporter() -> None:
    default_exp = RawDataExporter("s3://default", "relative_path")
    explicit_exp = RawDataExporter("s3://explicit", "relative_path")

    def my_func(export=default_exp):
        return export

    sig = inspect.signature(my_func)
    wrapper = RawDataExportWrapper()
    wrapped = wrapper.wrap(sig, my_func)
    result = wrapped(export=explicit_exp)
    assert result is explicit_exp


def test_wrap_accepts_none_to_disable() -> None:
    default_exp = RawDataExporter("s3://default", "relative_path")

    def my_func(export=default_exp):
        return export

    sig = inspect.signature(my_func)
    wrapper = RawDataExportWrapper()
    wrapped = wrapper.wrap(sig, my_func)
    result = wrapped(export=None)
    assert result is None
    assert wrapper({"test": "data"}) == {"test": "data"}


def test_export_path_delegates() -> None:
    wrapper = RawDataExportWrapper()
    assert wrapper.export_path == ""
    exp = RawDataExporter("s3://bucket", "relative_path")
    exp._export_path = "s3://bucket/pipe/src/res"
    wrapper._exporter = exp
    assert wrapper.export_path == "s3://bucket/pipe/src/res"


# ---------------------------------------------------------------------------
# integration: FileItemDict byte copy
# ---------------------------------------------------------------------------


def test_basic_file_copy() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("data/file1.csv", b"content1")
        yield _make_file_item("data/file2.csv", b"content2")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2
    assert _read_file(fs, files[0]) == b"content1"
    assert _read_file(fs, files[1]) == b"content2"


def test_export_preserves_directory_structure() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("2026/01/trades.csv", b"trades")
        yield _make_file_item("2026/02/positions.csv", b"positions")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    paths = [f.split("my_files/")[1] for f in files if "my_files/" in f]
    assert "2026/01/trades.csv" in paths
    assert "2026/02/positions.csv" in paths


def test_export_only_consumes_items() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs, export_only=True)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert _read_file(fs, files[0]) == b"data"


def test_export_pass_through() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1


def test_overwrite_on_rerun() -> None:
    fs = _memory_fs()
    pipeline = _pipeline()

    exp1 = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource(name="my_files")
    def run1(export=exp1):
        yield _make_file_item("file.csv", b"run1_data")

    pipeline.extract(run1())

    files = _list_files(fs, "memory://backup")
    matching = [f for f in files if f.endswith("file.csv")]
    assert len(matching) == 1
    assert _read_file(fs, matching[0]) == b"run1_data"

    exp2 = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource(name="my_files")
    def run2(export=exp2):
        yield _make_file_item("file.csv", b"run2_data")

    pipeline.extract(run2())

    files = _list_files(fs, "memory://backup")
    matching = [f for f in files if f.endswith("file.csv")]
    assert len(matching) == 1
    assert _read_file(fs, matching[0]) == b"run2_data"


def test_zero_byte_file() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("empty.csv", b"")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert _read_file(fs, files[0]) == b""


def test_large_file_chunked_copy() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)
    large_content = b"x" * (20 * 1024 * 1024)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("large.bin", large_content)

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert _read_file(fs, files[0]) == large_content


def test_dev_mode_disables_export() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    pipeline = dlt.pipeline(
        pipeline_name="test_rde_dev_" + uniq_id(), destination="duckdb", dev_mode=True
    )
    pipeline.extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 0


def test_dev_mode_with_enable_in_dev_mode() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs, enable_in_dev_mode=True)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"dev_data")

    pipeline = dlt.pipeline(
        pipeline_name="test_rde_dev_en_" + uniq_id(), destination="duckdb", dev_mode=True
    )
    pipeline.extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert "_dev" in files[0]


def test_export_with_none_disables() -> None:
    fs = _memory_fs()
    default_exp = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export: Optional[RawDataExporter] = default_exp):
        yield _make_file_item("file.csv", b"data")

    _pipeline().extract(my_files(export=None))

    files = _list_files(fs, "memory://backup")
    assert len(files) == 0


def test_path_traversal_blocked() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("../../etc/passwd", b"evil")

    with pytest.raises(PipelineStepFailed):
        _pipeline().extract(my_files())


def test_empty_name_path_raises() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    item = FileItemDict(
        {
            "file_url": "memory://source/file.csv",
            "file_name": "file.csv",
            "relative_path": "",
            "mime_type": "application/octet-stream",
            "modification_date": "2026-01-01",
            "size_in_bytes": 0,
            "file_content": b"",
        }
    )

    @dlt.resource
    def my_files(export=exporter):
        yield item

    with pytest.raises(PipelineStepFailed):
        _pipeline().extract(my_files())


def test_bucket_url_trailing_slash_normalized() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup/", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    _pipeline().extract(my_files())

    assert "//" not in exporter.export_path.replace("memory://", "")


def test_resource_raw_data_export_property() -> None:
    exporter = RawDataExporter("memory://backup", "relative_path")

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    wrapper = my_files.raw_data_export
    assert wrapper is not None
    assert isinstance(wrapper, RawDataExportWrapper)


def test_resource_without_export_returns_none() -> None:
    @dlt.resource
    def my_files():
        yield {"data": 1}

    assert my_files.raw_data_export is None


def test_export_path_contains_pipeline_and_resource_name() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("file.csv", b"data")

    pipeline = dlt.pipeline(pipeline_name="test_rde_path_" + uniq_id(), destination="duckdb")
    pipeline.extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert pipeline.pipeline_name in files[0]
    assert "my_files" in files[0]


def test_list_of_file_items() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield [
            _make_file_item("batch/a.csv", b"a_data"),
            _make_file_item("batch/b.csv", b"b_data"),
        ]

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2


def test_multiple_resources_isolated() -> None:
    fs = _memory_fs()

    @dlt.source
    def my_source():
        exp1 = _make_exporter("memory://backup", "relative_path", fs=fs)
        exp2 = _make_exporter("memory://backup", "relative_path", fs=fs)

        @dlt.resource
        def files_a(export=exp1):
            yield _make_file_item("a.csv", b"from_a")

        @dlt.resource
        def files_b(export=exp2):
            yield _make_file_item("b.csv", b"from_b")

        return files_a, files_b

    _pipeline().extract(my_source())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2
    paths_str = " ".join(files)
    assert "files_a" in paths_str
    assert "files_b" in paths_str


# ---------------------------------------------------------------------------
# integration: dict → jsonl
# ---------------------------------------------------------------------------


def test_dict_items_exported_as_jsonl() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "category", fs=fs)

    @dlt.resource
    def my_data(export=exporter):
        yield {"id": 1, "category": "events"}
        yield {"id": 2, "category": "events"}

    _pipeline().extract(my_data())

    files = _list_files(fs, "memory://backup")
    assert len(files) >= 1
    assert any("events" in f for f in files)
    for f in files:
        content = _read_file(fs, f)
        if content:
            lines = content.strip().split(b"\n")
            for line in lines:
                parsed = json.loads(line)
                assert "id" in parsed
                assert parsed["category"] == "events"


def test_dict_items_multiple_name_path_values() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "date", fs=fs)

    @dlt.resource
    def events(export=exporter):
        yield {"id": 1, "date": "2025-02-16", "data": "a"}
        yield {"id": 2, "date": "2025-02-16", "data": "b"}
        yield {"id": 3, "date": "2025-02-17", "data": "c"}

    _pipeline().extract(events())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2
    paths_str = " ".join(files)
    assert "2025-02-16" in paths_str
    assert "2025-02-17" in paths_str

    for f in files:
        content = _read_file(fs, f)
        lines = [json.loads(line) for line in content.strip().split(b"\n")]
        if "2025-02-16" in f:
            assert len(lines) == 2
            assert all(item["date"] == "2025-02-16" for item in lines)
        elif "2025-02-17" in f:
            assert len(lines) == 1
            assert lines[0]["date"] == "2025-02-17"


def test_dict_export_only() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "category", fs=fs, export_only=True)

    @dlt.resource
    def events(export=exporter):
        yield {"id": 1, "category": "test"}

    _pipeline().extract(events())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1


def test_load_id_in_dict_file_path() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "key", fs=fs)

    @dlt.resource
    def data(export=exporter):
        yield {"key": "value1", "data": "test"}

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    filename = files[0].split("/")[-1]
    parts = filename.split(".")
    assert len(parts) >= 3
    float(parts[0])  # load_id is a timestamp


def test_dict_empty_list_no_crash() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "key", fs=fs)

    @dlt.resource
    def data(export=exporter):
        yield []
        yield {"key": "val", "data": 1}

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) >= 1


def test_dict_many_distinct_name_path_values() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "key", fs=fs)

    @dlt.resource
    def data(export=exporter):
        for i in range(100):
            yield {"key": f"key_{i:03d}", "value": i}

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 100


def test_dict_name_path_with_special_chars() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "name", fs=fs)

    @dlt.resource
    def data(export=exporter):
        yield {"name": "hello world", "value": 1}
        yield {"name": "data#2", "value": 2}

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2


# ---------------------------------------------------------------------------
# integration: Arrow / DataFrame → parquet
# ---------------------------------------------------------------------------


def test_arrow_table_export_parquet() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "month", fs=fs)

    @dlt.resource
    def reports(export=exporter):
        yield pa.table({"month": ["2025-01", "2025-01"], "value": [100, 200]})

    _pipeline().extract(reports())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert files[0].endswith(".parquet")
    assert "2025-01" in files[0]
    content = _read_file(fs, files[0])
    assert content[:4] == b"PAR1"


def test_arrow_multiple_batches() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "region", fs=fs)

    @dlt.resource
    def data(export=exporter):
        yield pa.table({"region": ["US", "US"], "value": [1, 2]})
        yield pa.table({"region": ["EU", "EU"], "value": [3, 4]})

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2
    paths_str = " ".join(files)
    assert "US" in paths_str
    assert "EU" in paths_str


def test_load_id_in_arrow_file_path() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "key", fs=fs)

    @dlt.resource
    def data(export=exporter):
        yield pa.table({"key": ["val"], "data": [1]})

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    filename = files[0].split("/")[-1]
    parts = filename.split(".")
    assert len(parts) >= 3
    float(parts[0])  # load_id is a timestamp


def test_arrow_parquet_readable() -> None:
    import pyarrow.parquet as pq

    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "group", fs=fs)
    original = pa.table({"group": ["A", "A", "A"], "val": [10, 20, 30]})

    @dlt.resource
    def data(export=exporter):
        yield original

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    content = _read_file(fs, files[0])

    # write to a local temp file and read back with pyarrow
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        table = pq.read_table(tmp_path)
        assert table.num_rows == 3
        assert table.column("val").to_pylist() == [10, 20, 30]
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# integration: file_format override
# ---------------------------------------------------------------------------


def test_file_format_override_csv() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "key", fs=fs, file_format="csv")

    @dlt.resource
    def data(export=exporter):
        yield {"key": "test", "value": 42}

    _pipeline().extract(data())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert files[0].endswith(".csv")


# ---------------------------------------------------------------------------
# integration: mixed item types
# ---------------------------------------------------------------------------


def test_mixed_file_items_and_dicts_in_separate_resources() -> None:
    fs = _memory_fs()

    @dlt.source
    def my_source():
        exp_files = _make_exporter("memory://backup", "relative_path", fs=fs)
        exp_api = _make_exporter("memory://backup", "date", fs=fs)

        @dlt.resource
        def raw_files(export=exp_files):
            yield _make_file_item("data.csv", b"file_content")

        @dlt.resource
        def api_data(export=exp_api):
            yield {"date": "2025-02-16", "value": 123}

        return raw_files, api_data

    _pipeline().extract(my_source())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2

    file_paths = [f for f in files if "raw_files" in f]
    assert len(file_paths) == 1
    assert file_paths[0].endswith("data.csv")

    api_paths = [f for f in files if "api_data" in f]
    assert len(api_paths) == 1
    assert "2025-02-16" in api_paths[0]
    assert api_paths[0].endswith(".jsonl")


# ---------------------------------------------------------------------------
# integration: source wrapper with explicit source_name
# ---------------------------------------------------------------------------


def test_source_name_in_path() -> None:
    fs = _memory_fs()

    @dlt.source(name="my_storage")
    def my_source():
        exp = _make_exporter("memory://backup", "relative_path", fs=fs)

        @dlt.resource
        def csv_files(export=exp):
            yield _make_file_item("data.csv", b"content")

        return csv_files

    _pipeline().extract(my_source())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert "my_storage" in files[0]
    assert "csv_files" in files[0]


# ---------------------------------------------------------------------------
# integration: with add_filter
# ---------------------------------------------------------------------------


def test_add_filter_before_export() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("keep.csv", b"keep_data")
        yield _make_file_item("skip.csv", b"skip_data")

    filtered = my_files().add_filter(lambda item: item["relative_path"] != "skip.csv")

    _pipeline().extract(filtered)

    files = _list_files(fs, "memory://backup")
    assert len(files) == 1
    assert files[0].endswith("keep.csv")


# ---------------------------------------------------------------------------
# integration: export_only without incremental
# ---------------------------------------------------------------------------


def test_export_only_without_incremental() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs, export_only=True)

    @dlt.resource
    def my_files(export=exporter):
        yield _make_file_item("a.csv", b"aaa")
        yield _make_file_item("b.csv", b"bbb")

    _pipeline().extract(my_files())

    files = _list_files(fs, "memory://backup")
    assert len(files) == 2


# ---------------------------------------------------------------------------
# integration: incremental + export together
# ---------------------------------------------------------------------------


def test_incremental_with_export() -> None:
    fs = _memory_fs()
    exporter = _make_exporter("memory://backup", "relative_path", fs=fs)

    @dlt.resource
    def my_files(
        modified=dlt.sources.incremental("modification_date"),
        export=exporter,
    ):
        yield _make_file_item("old.csv", b"old", modification_date="2025-01-01")
        yield _make_file_item("new.csv", b"new", modification_date="2026-06-01")

    pipeline = _pipeline()
    pipeline.extract(my_files())

    files = _list_files(fs, "memory://backup")
    # both should be exported on first run (no prior state)
    assert len(files) == 2
