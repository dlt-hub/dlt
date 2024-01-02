import os
import pytest
import time
from typing import Iterator

from dlt.common.data_writers.exceptions import BufferedDataWriterClosed
from dlt.common.data_writers.writers import DataWriterMetrics
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.schema.utils import new_column
from dlt.common.storages.file_storage import FileStorage

from dlt.common.typing import DictStrAny

from tests.common.data_writers.utils import ALL_WRITERS, get_writer


@pytest.mark.parametrize("format_", ALL_WRITERS)
def test_write_no_item(format_: TLoaderFileFormat) -> None:
    with get_writer(_format=format_) as writer:
        pass
    assert writer.closed
    with pytest.raises(BufferedDataWriterClosed):
        writer._ensure_open()
    # no files rotated
    assert writer.closed_files == []


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
def test_rotation_on_schema_change(disable_compression: bool) -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")

    t1 = {"col1": c1}
    t2 = {"col2": c2, "col1": c1}
    t3 = {"col3": c3, "col2": c2, "col1": c1}

    def c1_doc(count: int) -> Iterator[DictStrAny]:
        return map(lambda x: {"col1": x}, range(0, count))

    def c2_doc(count: int) -> Iterator[DictStrAny]:
        return map(lambda x: {"col1": x, "col2": x * 2 + 1}, range(0, count))

    def c3_doc(count: int) -> Iterator[DictStrAny]:
        return map(lambda x: {"col3": "col3_value"}, range(0, count))

    # change schema before file first flush
    with get_writer(disable_compression=disable_compression) as writer:
        writer.write_data_item(list(c1_doc(8)), t1)
        assert writer._current_columns == t1
        # but different instance
        assert writer._current_columns is not t1
        writer.write_data_item(list(c2_doc(1)), t2)
        # file name is there
        assert writer._file_name is not None
        # no file is open
        assert writer._file is None
    # writer is closed and data was written
    assert len(writer.closed_files) == 1
    assert writer.closed_files[0].items_count == 9
    assert writer.closed_files[0].file_size > 0
    # check the content, mind that we swapped the columns
    with FileStorage.open_zipsafe_ro(writer.closed_files[0].file_path, "r", encoding="utf-8") as f:
        content = f.readlines()
    assert "col2,col1" in content[0]
    assert "NULL,0" in content[2]
    # col2 first
    assert "1,0" in content[-1]

    # data would flush and schema change
    with get_writer() as writer:
        writer.write_data_item(list(c1_doc(9)), t1)
        old_file = writer._file_name
        writer.write_data_item(list(c2_doc(1)), t2)  # rotates here
        # file is open
        assert writer._file is not None
        # no files were closed
        assert len(writer.closed_files) == 0
        assert writer._file_name == old_file
        # buffer is empty
        assert writer._buffered_items == []

    # file would rotate and schema change
    with get_writer() as writer:
        writer.file_max_items = 10
        writer.write_data_item(list(c1_doc(9)), t1)
        old_file = writer._file_name
        writer.write_data_item(list(c2_doc(1)), t2)  # rotates here
        # file is not open after rotation
        assert writer._file is None
        # file was rotated
        assert len(writer.closed_files) == 1
        assert writer._file_name != old_file
        # buffer is empty
        assert writer._buffered_items == []

    # schema change after flush rotates file
    with get_writer() as writer:
        writer.write_data_item(list(c1_doc(11)), t1)
        writer.write_data_item(list(c2_doc(1)), t2)
        assert len(writer.closed_files) == 1
        # now the file is closed
        assert writer._file is None
        old_file = writer._file_name
        # so we can write schema change without rotation and flushing
        writer.write_data_item(list(c2_doc(1)), t3)
        assert writer._file is None
        assert writer._file_name == old_file
        # make it flush
        writer.file_max_items = 10
        writer.write_data_item(list(c3_doc(20)), t3)
        assert len(writer.closed_files) == 2
        assert writer._buffered_items == []
    # the last file must contain text value of the column3
    with FileStorage.open_zipsafe_ro(writer.closed_files[-1].file_path, "r", encoding="utf-8") as f:
        content = f.readlines()
    assert "(col3_value" in content[-1]
    # check metrics
    assert writer.closed_files[0].items_count == 11
    assert writer.closed_files[1].items_count == 22


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
def test_NO_rotation_on_schema_change(disable_compression: bool) -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")

    t1 = {"col1": c1}
    t2 = {"col2": c2, "col1": c1}

    def c1_doc(count: int) -> Iterator[DictStrAny]:
        return map(lambda x: {"col1": x}, range(0, count))

    def c2_doc(count: int) -> Iterator[DictStrAny]:
        return map(lambda x: {"col1": x, "col2": x * 2 + 1}, range(0, count))

    # change schema before file first flush
    with get_writer(_format="jsonl", disable_compression=disable_compression) as writer:
        writer.write_data_item(list(c1_doc(15)), t1)
        # flushed
        assert writer._file is not None
        writer.write_data_item(list(c2_doc(2)), t2)
        # no rotation
        assert len(writer._buffered_items) == 2
        # only the initial 15 items written
        assert writer._writer.items_count == 15
    # all written
    with FileStorage.open_zipsafe_ro(writer.closed_files[-1].file_path, "r", encoding="utf-8") as f:
        content = f.readlines()
    assert content[-1] == '{"col1":1,"col2":3}\n'


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
def test_writer_requiring_schema(disable_compression: bool) -> None:
    # assertion on flushing
    with pytest.raises(AssertionError):
        with get_writer(disable_compression=disable_compression) as writer:
            writer.write_data_item([{"col1": 1}], None)
    # just single schema is enough
    c1 = new_column("col1", "bigint")
    t1 = {"col1": c1}
    with get_writer(disable_compression=disable_compression) as writer:
        writer.write_data_item([{"col1": 1}], None)
        writer.write_data_item([{"col1": 1}], t1)


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
def test_writer_optional_schema(disable_compression: bool) -> None:
    with get_writer(_format="jsonl", disable_compression=disable_compression) as writer:
        writer.write_data_item([{"col1": 1}], None)
        writer.write_data_item([{"col1": 1}], None)


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
@pytest.mark.parametrize("format_", ALL_WRITERS - {"arrow"})
def test_write_empty_file(disable_compression: bool, format_: TLoaderFileFormat) -> None:
    # just single schema is enough
    c1 = new_column("col1", "bigint")
    t1 = {"col1": c1}
    now = time.time()
    with get_writer(format_, disable_compression=disable_compression) as writer:
        metrics = writer.write_empty_file(t1)
        assert len(writer.closed_files) == 1
        assert os.path.abspath(metrics.file_path)
        assert os.path.isfile(metrics.file_path)
        assert metrics.created <= metrics.last_modified
        assert metrics.created >= now
        assert metrics.items_count == 0
        assert metrics.file_size >= 0
        assert writer.closed_files[0] == metrics


@pytest.mark.parametrize("format_", ALL_WRITERS)
def test_import_file(format_: TLoaderFileFormat) -> None:
    now = time.time()
    with get_writer(format_) as writer:
        # won't destroy the original
        metrics = writer.import_file(
            "tests/extract/cases/imported.any", DataWriterMetrics("", 1, 231, 0, 0)
        )
        assert len(writer.closed_files) == 1
        assert os.path.isfile(metrics.file_path)
        assert writer.closed_files[0] == metrics
        assert metrics.created <= metrics.last_modified
        assert metrics.created >= now
        assert metrics.items_count == 1
        assert metrics.file_size == 231


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
@pytest.mark.parametrize("format_", ALL_WRITERS - {"arrow"})
def test_gather_metrics(disable_compression: bool, format_: TLoaderFileFormat) -> None:
    now = time.time()
    c1 = new_column("col1", "bigint")
    t1 = {"col1": c1}
    with get_writer(
        format_, disable_compression=disable_compression, buffer_max_items=2, file_max_items=2
    ) as writer:
        time.sleep(0.55)
        count = writer.write_data_item([{"col1": 182812}, {"col1": -1}], t1)
        assert count == 2
        # file rotated
        assert len(writer.closed_files) == 1
        metrics = writer.closed_files[0]
        assert metrics.items_count == 2
        assert metrics.last_modified - metrics.created >= 0.55
        assert metrics.created >= now
        time.sleep(0.35)
        count = writer.write_data_item([{"col1": 182812}, {"col1": -1}, {"col1": 182811}], t1)
        assert count == 3
        # file rotated
        assert len(writer.closed_files) == 2
        metrics_2 = writer.closed_files[1]
        assert metrics_2.items_count == 3
        assert metrics_2.created >= metrics.last_modified
        assert metrics_2.last_modified - metrics_2.created >= 0.35

    assert len(writer.closed_files) == 2


@pytest.mark.parametrize(
    "disable_compression", [True, False], ids=["no_compression", "compression"]
)
@pytest.mark.parametrize("format_", ALL_WRITERS - {"arrow"})
def test_special_write_rotates(disable_compression: bool, format_: TLoaderFileFormat) -> None:
    c1 = new_column("col1", "bigint")
    t1 = {"col1": c1}
    with get_writer(
        format_, disable_compression=disable_compression, buffer_max_items=100, file_max_items=100
    ) as writer:
        writer.write_data_item([{"col1": 182812}, {"col1": -1}], t1)
        assert len(writer.closed_files) == 0
        # writing empty rotates existing file
        metrics = writer.write_empty_file(t1)
        assert len(writer.closed_files) == 2
        assert writer.closed_files[1] == metrics
        assert writer.closed_files[0].items_count == 2

        # also import rotates
        assert writer.write_data_item({"col1": 182812}, t1) == 1
        metrics = writer.import_file(
            "tests/extract/cases/imported.any", DataWriterMetrics("", 1, 231, 0, 0)
        )
        assert len(writer.closed_files) == 4
        assert writer.closed_files[3] == metrics
        assert writer.closed_files[2].items_count == 1

        assert writer.write_data_item({"col1": 182812}, t1) == 1
        metrics = writer.import_file(
            "tests/extract/cases/imported.any", DataWriterMetrics("", 1, 231, 0, 0)
        )
