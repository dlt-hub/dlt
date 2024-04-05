from typing import Type
import pytest

from dlt.common.schema.utils import new_column
from dlt.common.data_writers import DataWriter

from tests.common.data_writers.utils import get_writer, ALL_OBJECT_WRITERS, ALL_ARROW_WRITERS


@pytest.mark.parametrize("writer_type", ALL_OBJECT_WRITERS)
def test_writer_items_count(writer_type: Type[DataWriter]) -> None:
    c1 = {"col1": new_column("col1", "bigint")}
    with get_writer(writer_type) as writer:
        assert writer._buffered_items_count == 0
        # write empty list
        writer.write_data_item([], columns=c1)
        assert writer._buffered_items_count == 0
        writer._flush_items(allow_empty_file=True)
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 0
    assert writer.closed_files[0].items_count == 0

    # single item
    with get_writer(writer_type) as writer:
        assert writer._buffered_items_count == 0
        writer.write_data_item({"col1": 1}, columns=c1)
        assert writer._buffered_items_count == 1
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 1
    assert writer.closed_files[0].items_count == 1

    # list
    with get_writer(writer_type) as writer:
        assert writer._buffered_items_count == 0
        writer.write_data_item([{"col1": 1}, {"col1": 2}], columns=c1)
        assert writer._buffered_items_count == 2
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 2
    assert writer.closed_files[0].items_count == 2


@pytest.mark.parametrize("writer_type", ALL_ARROW_WRITERS)
def test_writer_items_count_arrow(writer_type: Type[DataWriter]) -> None:
    import pyarrow as pa

    c1 = {"col1": new_column("col1", "bigint")}
    single_elem_table = pa.Table.from_pylist([{"col1": 1}])

    # empty frame
    with get_writer(writer_type) as writer:
        assert writer._buffered_items_count == 0
        writer.write_data_item(single_elem_table.schema.empty_table(), columns=c1)
        assert writer._buffered_items_count == 0
        # there's an empty frame to be written
        assert len(writer._buffered_items) == 1
        # we flush empty frame
        writer._flush_items()
        assert writer._buffered_items_count == 0
        # no items were written
        assert writer._writer.items_count == 0
    # but file was created
    assert writer.closed_files[0].items_count == 0

    # single item
    with get_writer(writer_type) as writer:
        assert writer._buffered_items_count == 0
        writer.write_data_item(pa.Table.from_pylist([{"col1": 1}]), columns=c1)
        assert writer._buffered_items_count == 1
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 1
    assert writer.closed_files[0].items_count == 1

    # single item with many rows
    with get_writer(writer_type) as writer:
        writer.write_data_item(pa.Table.from_pylist([{"col1": 1}, {"col1": 2}]), columns=c1)
        assert writer._buffered_items_count == 2
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 2
    assert writer.closed_files[0].items_count == 2

    # empty list
    with get_writer(writer_type) as writer:
        writer.write_data_item([], columns=c1)
        assert writer._buffered_items_count == 0
        writer._flush_items()
        assert writer._buffered_items_count == 0
        # no file was created
        assert writer._file is None
        assert writer._writer is None
    assert len(writer.closed_files) == 0

    # list with one item
    with get_writer(writer_type) as writer:
        writer.write_data_item([pa.Table.from_pylist([{"col1": 1}])], columns=c1)
        assert writer._buffered_items_count == 1
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 1
    assert writer.closed_files[0].items_count == 1

    # list with many items
    with get_writer(writer_type) as writer:
        writer.write_data_item(
            [pa.Table.from_pylist([{"col1": 1}]), pa.Table.from_pylist([{"col1": 1}, {"col1": 2}])],
            columns=c1,
        )
        assert writer._buffered_items_count == 3
        writer._flush_items()
        assert writer._buffered_items_count == 0
        assert writer._writer.items_count == 3
    assert writer.closed_files[0].items_count == 3
