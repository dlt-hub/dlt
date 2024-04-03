import csv
from copy import copy
import pytest
import pyarrow.parquet as pq

from dlt.common.data_writers.exceptions import InvalidDataItem
from dlt.common.data_writers.writers import ArrowToCsvWriter, ParquetDataWriter
from dlt.common.libs.pyarrow import remove_columns

from tests.common.data_writers.utils import get_writer
from tests.cases import (
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
    TABLE_ROW_ALL_DATA_TYPES,
    arrow_table_all_data_types,
)
from tests.utils import write_version, autouse_test_storage, preserve_environ


def test_csv_writer_all_data_fields() -> None:
    data = TABLE_ROW_ALL_DATA_TYPES_DATETIMES

    # write parquet and read it
    with get_writer(ParquetDataWriter) as pq_writer:
        pq_writer.write_data_item([data], TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(pq_writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)

    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item(table, TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2

    # compare headers
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    # compare row
    assert len(rows[1]) == len(list(TABLE_ROW_ALL_DATA_TYPES.values()))
    # compare none values
    for actual, expected, col_name in zip(
        rows[1], TABLE_ROW_ALL_DATA_TYPES.values(), TABLE_UPDATE_COLUMNS_SCHEMA.keys()
    ):
        if expected is None:
            assert actual == "", f"{col_name} is not recovered as None"

    # write again with several arrows
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item([table, table], TABLE_UPDATE_COLUMNS_SCHEMA)
        writer.write_data_item(table.to_batches(), TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 3 data
        assert len(rows) == 4
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    assert rows[1] == rows[2] == rows[3]

    # simulate non announced schema change
    base_table = remove_columns(table, ["col9_null"])
    base_column_schema = copy(TABLE_UPDATE_COLUMNS_SCHEMA)
    base_column_schema.pop("col9_null")

    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item([base_table, table], TABLE_UPDATE_COLUMNS_SCHEMA)

    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(table, TABLE_UPDATE_COLUMNS_SCHEMA)
            writer.write_data_item(base_table, TABLE_UPDATE_COLUMNS_SCHEMA)

    # schema change will rotate the file
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item(base_table, base_column_schema)
        writer.write_data_item([table, table], TABLE_UPDATE_COLUMNS_SCHEMA)

    assert len(writer.closed_files) == 2

    # first file
    with open(writer.closed_files[0].file_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2
    assert rows[0] == list(base_column_schema.keys())
    # second file
    with open(writer.closed_files[1].file_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 2 data
        assert len(rows) == 3
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())


def test_non_utf8_binary() -> None:
    data = TABLE_ROW_ALL_DATA_TYPES_DATETIMES
    data["col7"] += b"\x8e"  # type: ignore[operator]

    # write parquet and read it
    with get_writer(ParquetDataWriter) as pq_writer:
        pq_writer.write_data_item([data], TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(pq_writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)

    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(table, TABLE_UPDATE_COLUMNS_SCHEMA)


def test_arrow_struct() -> None:
    item, _ = arrow_table_all_data_types("table", include_json=True, include_time=False)
    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(item, TABLE_UPDATE_COLUMNS_SCHEMA)


def test_csv_writer_empty() -> None:
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_empty_file(TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # only header
        assert len(rows) == 1

    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
