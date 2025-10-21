import csv
from copy import copy
from typing import Any, Dict, Type
from unittest.mock import Mock, patch
import pytest
import pyarrow.parquet as pq

from dlt.common import json
from dlt.common.destination.configuration import CsvQuoting
from dlt.common.data_writers.exceptions import InvalidDataItem
from dlt.common.data_writers.writers import (
    ArrowToCsvWriter,
    CsvWriter,
    DataWriter,
    ParquetDataWriter,
)
from dlt.common.libs.pyarrow import remove_columns
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.utils import custom_environ

from tests.common.data_writers.utils import get_writer
from tests.cases import (
    TABLE_ROW_ALL_DATA_TYPES_DATETIMES,
    arrow_table_all_data_types,
    table_update_and_row,
)
from tests.utils import TestDataItemFormat


def test_csv_arrow_writer_all_data_fields() -> None:
    datetime_data = copy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    column_schemas, data_types = table_update_and_row()

    # write parquet and read it
    with get_writer(ParquetDataWriter) as pq_writer:
        pq_writer.write_data_item([datetime_data], column_schemas)

    with open(pq_writer.closed_files[0].file_path, "rb") as f:
        table = pq.read_table(f)

    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item(table, column_schemas)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2

    # compare headers
    assert rows[0] == list(column_schemas.keys())
    # compare row
    assert len(rows[1]) == len(list(data_types.values()))

    # TODO: uncomment and fix decimal256 and None that is ""
    # with open(writer.closed_files[0].file_path, "br") as f:
    #     csv_table = acsv.read_csv(f, convert_options=acsv.ConvertOptions(column_types=table.schema))
    # for actual, expected in zip(table.to_pylist(), csv_table.to_pylist()):
    #     assert actual == expected

    # write again with several arrows
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item([table, table], column_schemas)
        writer.write_data_item(table.to_batches(), column_schemas)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 3 data
        assert len(rows) == 4
    assert rows[0] == list(column_schemas.keys())
    assert rows[1] == rows[2] == rows[3]

    # simulate non announced schema change
    base_table = remove_columns(table, ["col9_null"])
    base_column_schema = copy(column_schemas)
    base_column_schema.pop("col9_null")

    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item([base_table, table], column_schemas)

    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(table, column_schemas)
            writer.write_data_item(base_table, column_schemas)

    # schema change will rotate the file
    with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
        writer.write_data_item(base_table, base_column_schema)
        writer.write_data_item([table, table], column_schemas)

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
    assert rows[0] == list(column_schemas.keys())


def test_csv_object_writer_all_data_fields() -> None:
    datetime_data = TABLE_ROW_ALL_DATA_TYPES_DATETIMES
    column_schemas, data_types = table_update_and_row()

    # always copy data on write (csv writer may modify the data)
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item(copy(datetime_data), column_schemas)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        f.seek(0)
        csv_rows = list(csv.DictReader(f, dialect=csv.unix_dialect))
        # header + 1 data
        assert len(rows) == 2

    # compare headers
    assert rows[0] == list(column_schemas.keys())
    # compare row
    assert len(rows[1]) == len(list(data_types.values()))
    assert_csv_rows(csv_rows[0], TABLE_ROW_ALL_DATA_TYPES_DATETIMES)

    # write again with several tables
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item([copy(datetime_data), copy(datetime_data)], column_schemas)
        writer.write_data_item(copy(datetime_data), column_schemas)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # header + 3 data
        assert len(rows) == 4
    assert rows[0] == list(column_schemas.keys())
    assert rows[1] == rows[2] == rows[3]

    base_data = copy(datetime_data)
    base_data.pop("col9_null")
    base_column_schema = copy(column_schemas)
    base_column_schema.pop("col9_null")

    # schema change will rotate the file
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item(copy(base_data), base_column_schema)
        writer.write_data_item([copy(datetime_data), copy(datetime_data)], column_schemas)

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
    assert rows[0] == list(column_schemas.keys())

    # simulate non announced schema change
    # ignored by reader: we'd need to check this per row which will slow it down
    with get_writer(CsvWriter, disable_compression=True) as writer:
        writer.write_data_item([copy(base_data), copy(datetime_data)], base_column_schema)


@pytest.mark.parametrize("item_type", ["object", "arrow-table"])
def test_non_utf8_binary(item_type: TestDataItemFormat) -> None:
    datetime_data = copy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    column_schemas, data_types = table_update_and_row()
    datetime_data["col7"] += b"\x8e"  # type: ignore[operator]

    if item_type == "arrow-table":
        # write parquet and read it
        with get_writer(ParquetDataWriter) as pq_writer:
            pq_writer.write_data_item([datetime_data], column_schemas)

        with open(pq_writer.closed_files[0].file_path, "rb") as f:
            table = pq.read_table(f)
    else:
        table = datetime_data
    writer_type: Type[DataWriter] = ArrowToCsvWriter if item_type == "arrow-table" else CsvWriter

    with pytest.raises(InvalidDataItem) as inv_ex:
        with get_writer(writer_type, disable_compression=True) as writer:
            writer.write_data_item(table, column_schemas)
    assert "Remove binary columns" in str(inv_ex.value)


def test_arrow_struct() -> None:
    column_schemas, _ = table_update_and_row()
    item, _, _ = arrow_table_all_data_types("arrow-table", include_json=True, include_time=False)
    with pytest.raises(InvalidDataItem):
        with get_writer(ArrowToCsvWriter, disable_compression=True) as writer:
            writer.write_data_item(item, column_schemas)


@pytest.mark.parametrize("item_type", ["object", "arrow-table"])
def test_csv_writer_empty(item_type: TestDataItemFormat) -> None:
    column_schemas, _ = table_update_and_row()

    writer_type: Type[DataWriter] = ArrowToCsvWriter if item_type == "arrow-table" else CsvWriter
    with get_writer(writer_type, disable_compression=True) as writer:
        writer.write_empty_file(column_schemas)

    with open(writer.closed_files[0].file_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f, dialect=csv.unix_dialect))
        # only header
        assert len(rows) == 1

    assert rows[0] == list(column_schemas.keys())


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


@pytest.mark.parametrize("quoting", ["quote_all", "quote_needed", "quote_none"])
def test_arrow_csv_writer_quoting_parameters(quoting: CsvQuoting) -> None:
    import pyarrow as pa

    mock_schema: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "text"},
        "col2": {"name": "col2", "data_type": "bigint"},
    }

    test_data = pa.table({"col1": ["test_value"], "col2": [123]})

    expected_quoting_mapping = {
        "quote_all": "all_valid",
        "quote_needed": "needed",
        "quote_none": "none",
    }

    with patch("pyarrow.csv.CSVWriter") as mock_csv_writer:
        mock_writer_instance = Mock()
        mock_csv_writer.return_value = mock_writer_instance

        mock_file = Mock()
        writer = ArrowToCsvWriter(mock_file, quoting=quoting)
        writer.write_header(mock_schema)
        writer.write_data([test_data])

        mock_csv_writer.assert_called()
        call_args = mock_csv_writer.call_args
        write_options = call_args.kwargs["write_options"]
        assert write_options.quoting_style == expected_quoting_mapping[quoting]

        mock_writer_instance.write.assert_called_with(test_data)


def test_arrow_csv_writer_quote_none_with_special_characters() -> None:
    import tempfile
    import pyarrow as pa
    from pyarrow.lib import ArrowInvalid

    mock_schema: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "text"},
        "col2": {"name": "col2", "data_type": "text"},
    }

    test_data = pa.table({"col1": ["value,with,commas"], "col2": ["value\nwith\nnewlines"]})

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as temp_file:
        writer = ArrowToCsvWriter(temp_file, quoting="quote_none")
        writer.write_header(mock_schema)
        with pytest.raises(ArrowInvalid, match="CSV values may not contain structural characters"):
            writer.write_data([test_data])


@pytest.mark.parametrize("quoting", ["quote_all", "quote_needed", "quote_none", "quote_minimal"])
def test_csv_writer_quoting_parameters(quoting: CsvQuoting) -> None:
    mock_schema: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "text"},
        "col2": {"name": "col2", "data_type": "bigint"},
    }

    test_data = [{"col1": "test_value", "col2": 123}]

    expected_quoting_mapping = {
        "quote_all": csv.QUOTE_ALL,
        "quote_needed": csv.QUOTE_NONNUMERIC,
        "quote_none": csv.QUOTE_NONE,
        "quote_minimal": csv.QUOTE_MINIMAL,
    }

    with patch("csv.DictWriter") as mock_dict_writer:
        mock_writer_instance = Mock()
        mock_dict_writer.return_value = mock_writer_instance

        mock_file = Mock()
        csv_writer = CsvWriter(mock_file, quoting=quoting)

        csv_writer.write_header(mock_schema)
        csv_writer.write_data(test_data)

        mock_dict_writer.assert_called_once()
        call_args = mock_dict_writer.call_args

        assert "quoting" in call_args.kwargs
        assert call_args.kwargs["quoting"] == expected_quoting_mapping[quoting]

        mock_writer_instance.writeheader.assert_called_once()
        mock_writer_instance.writerows.assert_called_once_with(test_data)


@pytest.mark.parametrize(
    "test_case",
    [
        {"lineterminator": "\n", "expected": b'"col1"\n"value1"\n"value2"\n'},
        {"lineterminator": "\r\n", "expected": b'"col1"\r\n"value1"\r\n"value2"\r\n'},
    ],
)
def test_csv_lineterminator(test_case: Dict[str, str]) -> None:
    lineterminator = test_case["lineterminator"]
    expected = test_case["expected"]

    schema: TTableSchemaColumns = {"col1": {"name": "col1", "data_type": "text"}}
    data = [{"col1": "value1"}, {"col1": "value2"}]

    with custom_environ({"DATA_WRITER__LINETERMINATOR": lineterminator}):
        with get_writer(CsvWriter, disable_compression=True) as writer:
            writer.write_data_item(data, schema)

        with open(writer.closed_files[0].file_path, "rb") as f:
            content = f.read()
            assert content == expected


@pytest.mark.parametrize(
    "quoting,delimiter,schema,test_data_dict,expected_header,expected_data_rows",
    [
        pytest.param(
            "quote_none",
            ",",
            {
                "col1": {"name": "col1", "data_type": "text"},
                "col2": {"name": "col2", "data_type": "bigint"},
            },
            {
                "col1": ["test_value", "another_value"],
                "col2": [123, 456],
            },
            "col1,col2",
            ["test_value,123", "another_value,456"],
            id="quote_none_with_data",
        ),
        pytest.param(
            "quote_all",
            ",",
            {
                "col1": {"name": "col1", "data_type": "text"},
                "col2": {"name": "col2", "data_type": "bigint"},
            },
            {"col1": ["value1"], "col2": [123]},
            '"col1","col2"',
            ['"value1","123"'],
            id="quote_all_with_data",
        ),
        pytest.param(
            "quote_needed",
            ",",
            {
                "col1": {"name": "col1", "data_type": "text"},
                "2": {"name": "2", "data_type": "bigint"},
            },
            {"col1": ["value1"], "col2": [123]},
            '"col1","2"',
            ['"value1",123'],
            id="quote_needed_with_data",
        ),
        pytest.param(
            "quote_none",
            ",",
            {
                "col1": {"name": "col1", "data_type": "text"},
                "col2": {"name": "col2", "data_type": "bigint"},
            },
            None,
            "col1,col2",
            [],
            id="quote_none_empty_file",
        ),
        pytest.param(
            "quote_none",
            "|",
            {
                "col1": {"name": "col1", "data_type": "text"},
                "col2": {"name": "col2", "data_type": "bigint"},
            },
            {"col1": ["value1"], "col2": [123]},
            "col1|col2",
            ["value1|123"],
            id="quote_none_custom_delimiter",
        ),
    ],
)
def test_arrow_csv_writer(
    quoting: CsvQuoting,
    delimiter: str,
    schema: TTableSchemaColumns,
    test_data_dict,
    expected_header: str,
    expected_data_rows,
) -> None:
    # Test ArrowToCsvWriter header generation with various quoting styles for both header and data rows
    import tempfile
    import pyarrow as pa

    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv") as f:
        writer = ArrowToCsvWriter(f, quoting=quoting, include_header=True, delimiter=delimiter)
        writer.write_header(schema)

        if test_data_dict is not None:
            test_data = pa.table(test_data_dict)
            writer.write_data([test_data])
        else:
            writer.write_footer()

        f.flush()
        f.seek(0)
        lines = f.read().decode("utf-8").splitlines()

        assert lines[0].strip() == expected_header

        if expected_data_rows is not None and len(expected_data_rows) > 0:
            for i, expected_row in enumerate(expected_data_rows):
                assert lines[i + 1].strip() == expected_row


def test_arrow_csv_writer_special_chars_in_column_names_quote_none() -> None:
    import tempfile
    import pyarrow as pa
    from pyarrow.lib import ArrowInvalid

    # Column names with commas should fail with quote_none
    mock_schema: TTableSchemaColumns = {
        "col,with,comma": {"name": "col,with,comma", "data_type": "text"},
    }

    test_data = pa.table({"col,with,comma": ["value1"]})

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as f:
        writer = ArrowToCsvWriter(f, quoting="quote_none", include_header=True)
        writer.write_header(mock_schema)
        with pytest.raises(ArrowInvalid, match="CSV values may not contain structural characters"):
            writer.write_data([test_data])


def test_arrow_csv_writer_empty_schema() -> None:
    import tempfile

    mock_schema: TTableSchemaColumns = {}

    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv") as f:
        writer = ArrowToCsvWriter(f, quoting="quote_none", include_header=True)
        writer.write_header(mock_schema)
        # this triggers _make_csv_header() with an empty schema
        writer.write_footer()
        f.flush()
        f.seek(0)
        assert f.read() == b""


def test_arrow_csv_writer_invalid_quoting_parameter() -> None:
    import tempfile

    mock_schema: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "text"},
    }

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as f:
        writer = ArrowToCsvWriter(f, quoting="quote_none", include_header=True)
        writer.write_header(mock_schema)
        # Manually set invalid quoting to trigger the error in _make_csv_header
        writer.quoting = "invalid_quoting_style"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="invalid_quoting_style"):
            writer.write_footer()
