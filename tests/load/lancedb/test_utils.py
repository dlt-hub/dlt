import pyarrow as pa
import pytest

from dlt.destinations.impl.lancedb.utils import fill_empty_source_column_values_with_placeholder


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


def test_fill_empty_source_column_values_with_placeholder() -> None:
    data = [pa.array(["", "hello", ""]), pa.array([1, 2, 3]), pa.array(["world", "", "arrow"])]
    table = pa.Table.from_arrays(data, names=["A", "B", "C"])

    source_columns = ["A"]
    placeholder = "placeholder"

    new_table = fill_empty_source_column_values_with_placeholder(table, source_columns, placeholder)

    expected_data = [
        pa.array(["placeholder", "hello", "placeholder"]),
        pa.array([1, 2, 3]),
        pa.array(["world", "", "arrow"]),
    ]
    expected_table = pa.Table.from_arrays(expected_data, names=["A", "B", "C"])
    assert new_table.equals(expected_table)
