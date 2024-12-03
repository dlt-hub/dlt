import csv
from copy import copy
from typing import Any, Dict, Type
import pytest
import pyarrow.csv as acsv
import pyarrow.parquet as pq

from dlt.common import json
from dlt.common.data_writers.exceptions import InvalidDataItem
from dlt.common.data_writers.writers import (
    ArrowToCsvWriter,
    CsvWriter,
    DataWriter,
    ParquetDataWriter,
)
from dlt.common.libs.pyarrow import remove_columns

from tests.common.data_writers.utils import get_writer
from tests.cases import (
    TABLE_UPDATE_COLUMNS_SCHEMA,
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
    TABLE_ROW_ALL_DATA_TYPES,
    arrow_table_all_data_types,
)
from tests.utils import TestDataItemFormat


def test_csv_arrow_writer_all_data_fields() -> None:
    data = copy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)

    # write parquet and read it
    with get_writer(ParquetDataWriter) as pq_writer:
        pq_writer.write_data_item([data], TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(pq_writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)

    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item(table, TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2

    # compare headers
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    # compare row
    assert len(rows[1]) == len(list(TABLE_ROW_ALL_DATA_TYPES.values()))

    # TODO: uncomment and fix decimal256 and None that is ""
    # with open(writer.closed_files[0].file_path, "br") as f:
    #     csv_table = acsv.read_csv(f, convert_options=acsv.ConvertOptions(column_types=table.schema))
    # for actual, expected in zip(table.to_pylist(), csv_table.to_pylist()):
    #     assert actual == expected

    # write again with several arrows
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item([table, table], TABLE_UPDATE_COLUMNS_SCHEMA)
        writer.write_data_item(table.to_batches(), TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
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
    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2
    assert rows[0] == list(base_column_schema.keys())
    # second file
    with open(writer.closed_files[1].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 2 data
        assert len(rows) == 3
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())


def test_csv_object_writer_all_data_fields() -> None:
    data = TABLE_ROW_ALL_DATA_TYPES_DATETIMES

    # always copy data on write (csv writer may modify the data)
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item(copy(data), TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        f.seek(0)
        csv_rows = list(csv.DictReader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2

    # compare headers
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    # compare row
    assert len(rows[1]) == len(list(TABLE_ROW_ALL_DATA_TYPES.values()))
    assert_csv_rows(csv_rows[0], TABLE_ROW_ALL_DATA_TYPES_DATETIMES)

    # write again with several tables
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item([copy(data), copy(data)], TABLE_UPDATE_COLUMNS_SCHEMA)
        writer.write_data_item(copy(data), TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 3 data
        assert len(rows) == 4
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())
    assert rows[1] == rows[2] == rows[3]

    base_data = copy(data)
    base_data.pop("col9_null")
    base_column_schema = copy(TABLE_UPDATE_COLUMNS_SCHEMA)
    base_column_schema.pop("col9_null")

    # schema change will rotate the file
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item(copy(base_data), base_column_schema)
        writer.write_data_item([copy(data), copy(data)], TABLE_UPDATE_COLUMNS_SCHEMA)

    assert len(writer.closed_files) == 2

    # first file
    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2
    assert rows[0] == list(base_column_schema.keys())
    # second file
    with open(writer.closed_files[1].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 2 data
        assert len(rows) == 3
    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())

    # simulate non announced schema change
    # ignored by reader: we'd need to check this per row which will slow it down
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item([copy(base_data), copy(data)], base_column_schema)


@pytest.mark.parametrize("item_type", ["object", "arrow-table"])
def test_non_utf8_binary(item_type: TestDataItemFormat) -> None:
    data = copy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    data["col7"] += b"\x8e"  # type: ignore[operator]

    if item_type == "arrow-table":
        # write parquet and read it
        with get_writer(ParquetDataWriter) as pq_writer:
            pq_writer.write_data_item([data], TABLE_UPDATE_COLUMNS_SCHEMA)

        with open(pq_writer.closed_files[0].file_path, "rb") as f:
            table = pq.read_table(f)
    else:
        table = data
    writer_type: Type[DataWriter] = ArrowToCsvWriter if item_type == "arrow-table" else CsvWriter

    with pytest.raises(InvalidDataItem) as inv_ex:
        with get_writer(writer_type, disable_compression=True) as writer:
            writer.write_data_item(table, TABLE_UPDATE_COLUMNS_SCHEMA)
    assert "Remove binary columns" in str(inv_ex.value)


def test_arrow_struct() -> None:
    item, _, _ = arrow_table_all_data_types("arrow-table", include_json=True, include_time=False)
    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(item, TABLE_UPDATE_COLUMNS_SCHEMA)


@pytest.mark.parametrize("item_type", ["object", "arrow-table"])
def test_csv_writer_empty(item_type: TestDataItemFormat) -> None:
    writer_type: Type[DataWriter] = ArrowToCsvWriter if item_type == "arrow-table" else CsvWriter
    with get_writer(writer_type, disable_compression=True) as writer:
        writer.write_empty_file(TABLE_UPDATE_COLUMNS_SCHEMA)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # only header
        assert len(rows) == 1

    assert rows[0] == list(TABLE_UPDATE_COLUMNS_SCHEMA.keys())


def assert_csv_rows(csv_row: Dict[str, Any], expected_row: Dict[str, Any]) -> None:
    for actual, expected in zip(csv_row.items(), expected_row.values()):
        if expected is None:
            expected = ""
        elif isinstance(expected, dict):
            expected = json.dumps(expected)
        else:
            # writer calls `str` on non string
            expected = expected.decode("utf-8") if isinstance(expected, bytes) else str(expected)
        assert actual[1] == expected, print(
            f"Failed on {actual[0]}: actual: {actual[1]} vs expected: {expected}"
        )
