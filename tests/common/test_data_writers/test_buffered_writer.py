import os
import pytest
import pyarrow.parquet as pq
from dlt.common.arithmetics import Decimal  

from dlt.common.data_writers.buffered import BufferedDataWriter
from dlt.common.data_writers.exceptions import BufferedDataWriterClosed
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext
from dlt.common.schema.utils import new_column
from dlt.common.storages.file_storage import FileStorage

from dlt.common.typing import DictStrAny

from tests.utils import TEST_STORAGE_ROOT, write_version, autouse_test_storage
import datetime  # noqa: 251


def get_insert_writer(_format: TLoaderFileFormat = "insert_values", buffer_max_items: int = 10, disable_compression: bool = False) -> BufferedDataWriter:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.preferred_loader_file_format = _format
    file_template = os.path.join(TEST_STORAGE_ROOT, f"{_format}.%s")
    return BufferedDataWriter(_format, file_template, buffer_max_items=buffer_max_items, disable_compression=disable_compression, _caps=caps)


def test_write_no_item() -> None:
    with get_insert_writer() as writer:
        pass
    assert writer.closed
    with pytest.raises(BufferedDataWriterClosed):
        assert writer._ensure_open()
    # no files rotated
    assert writer.closed_files == []


@pytest.mark.parametrize("disable_compression", [True, False], ids=["no_compression", "compression"])
def test_rotation_on_schema_change(disable_compression: bool) -> None:

    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")

    t1 = {"col1": c1}
    t2 = {"col2": c2, "col1": c1}
    t3 = {"col3": c3, "col2": c2, "col1": c1}

    def c1_doc(count: int) -> DictStrAny:
        return map(lambda x: {"col1": x}, range(0, count))

    def c2_doc(count: int) -> DictStrAny:
        return map(lambda x: {"col1": x, "col2": x*2+1}, range(0, count))

    def c3_doc(count: int) -> DictStrAny:
        return map(lambda x: {"col3": "col3_value"}, range(0, count))

    # change schema before file first flush
    with get_insert_writer(disable_compression=disable_compression) as writer:
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
    # check the content, mind that we swapped the columns
    with FileStorage.open_zipsafe_ro(writer.closed_files[0], "r", encoding="utf-8") as f:
        content = f.readlines()
    assert "col2,col1" in content[0]
    assert "NULL,0" in content[2]
    # col2 first
    assert "1,0" in content[-1]

    # data would flush and schema change
    with get_insert_writer() as writer:
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
    with get_insert_writer() as writer:
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
    with get_insert_writer() as writer:
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
    with FileStorage.open_zipsafe_ro(writer.closed_files[-1], "r", encoding="utf-8") as f:
        content = f.readlines()
    assert "(col3_value" in content[-1]


@pytest.mark.parametrize("disable_compression", [True, False], ids=["no_compression", "compression"])
def test_NO_rotation_on_schema_change(disable_compression: bool) -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")

    t1 = {"col1": c1}
    t2 = {"col2": c2, "col1": c1}

    def c1_doc(count: int) -> DictStrAny:
        return map(lambda x: {"col1": x}, range(0, count))

    def c2_doc(count: int) -> DictStrAny:
        return map(lambda x: {"col1": x, "col2": x*2+1}, range(0, count))

    # change schema before file first flush
    with get_insert_writer(_format="jsonl", disable_compression=disable_compression) as writer:
        writer.write_data_item(list(c1_doc(15)), t1)
        # flushed
        assert writer._file is not None
        writer.write_data_item(list(c2_doc(2)), t2)
        # no rotation
        assert len(writer._buffered_items) == 2
        # only the initial 15 items written
        assert writer._writer.items_count == 15
    # all written
    with FileStorage.open_zipsafe_ro(writer.closed_files[-1], "r", encoding="utf-8") as f:
        content = f.readlines()
    assert content[-1] == '{"col1":1,"col2":3}\n'


@pytest.mark.parametrize("disable_compression", [True, False], ids=["no_compression", "compression"])
def test_writer_requiring_schema(disable_compression: bool) -> None:
    # assertion on flushing
    with pytest.raises(AssertionError):
        with get_insert_writer(disable_compression=disable_compression) as writer:
            writer.write_data_item([{"col1": 1}], None)
    # just single schema is enough
    c1 = new_column("col1", "bigint")
    t1 = {"col1": c1}
    with get_insert_writer(disable_compression=disable_compression) as writer:
        writer.write_data_item([{"col1": 1}], None)
        writer.write_data_item([{"col1": 1}], t1)


@pytest.mark.parametrize("disable_compression", [True, False], ids=["no_compression", "compression"])
def test_writer_optional_schema(disable_compression: bool) -> None:
    with get_insert_writer(_format="jsonl", disable_compression=disable_compression) as writer:
            writer.write_data_item([{"col1": 1}], None)
            writer.write_data_item([{"col1": 1}], None)


def test_parquet_writer_schema_evolution() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "text")
    c4 = new_column("col4", "text")

    with get_insert_writer("parquet") as writer:
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3"}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": "3", "col4": "4", "col5": {"hello": "marcin"}}], {"col1": c1, "col2": c2, "col3": c3, "col4": c4})

    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == [1, 1]
        assert table.column("col2").to_pylist() == [2, 2]
        assert table.column("col3").to_pylist() == ["3", "3"]
        assert table.column("col4").to_pylist() == [None, "4"]


def test_parquet_writer_json_serialization() -> None:
    c1 = new_column("col1", "bigint")
    c2 = new_column("col2", "bigint")
    c3 = new_column("col3", "complex")

    with get_insert_writer("parquet") as writer:
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": {"hello":"dave"}}], {"col1": c1, "col2": c2, "col3": c3})
        writer.write_data_item([{"col1": 1, "col2": 2, "col3": {"hello":"marcin"}}], {"col1": c1, "col2": c2, "col3": c3})

    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("col1").to_pylist() == [1, 1]
        assert table.column("col2").to_pylist() == [2, 2]
        assert table.column("col3").to_pylist() == ["""{"hello":"dave"}""","""{"hello":"marcin"}"""]


def test_parquet_writer_all_data_fields() -> None:

    # can be reused for other writers..
    columns = {
        "col1": new_column("col1", "text"),
        "col2": new_column("col2", "double"),
        "col3": new_column("col3", "bool"),
        "col4": new_column("col4", "timestamp"),
        "col5": new_column("col5", "bigint"),
        "col6": new_column("col6", "binary"),
        "col7": new_column("col7", "complex"),
        "col8": new_column("col8", "decimal"),
        "col9": new_column("col9", "wei"),
        "col10": new_column("col10", "date"),
    }

    data = {
        "col1": "hello",
        "col2": 1.0,
        "col3": True,
        "col4": datetime.datetime.now().replace(microsecond=0),
        "col5": 10**10,
        "col6": b"hello",
        "col7": {"hello": "dave"},
        "col8": Decimal(1),
        "col9": 1,
        "col10": datetime.date.today(),
    }

    with get_insert_writer("parquet") as writer:
        writer.write_data_item([data], columns)
        
    with open(writer.closed_files[0], "rb") as f:
        table = pq.read_table(f)
        for key, value in data.items():
            assert table.column(key).to_pylist() == [value]
