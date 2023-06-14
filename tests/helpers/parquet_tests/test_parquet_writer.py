import os
import pytest
import pyarrow.parquet as pq
from dlt.common.arithmetics import Decimal  

from dlt.common.data_writers.buffered import BufferedDataWriter
from dlt.common.data_writers.exceptions import BufferedDataWriterClosed
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext
from dlt.common.schema.utils import new_column

from dlt.common.typing import DictStrAny

from tests.utils import TEST_STORAGE_ROOT, write_version, autouse_test_storage
import datetime  # noqa: 251


def get_insert_writer(_format: TLoaderFileFormat = "insert_values", buffer_max_items: int = 10) -> BufferedDataWriter:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.preferred_loader_file_format = _format
    file_template = os.path.join(TEST_STORAGE_ROOT, f"{_format}.%s")
    return BufferedDataWriter(_format, file_template, buffer_max_items=buffer_max_items, _caps=caps)



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
