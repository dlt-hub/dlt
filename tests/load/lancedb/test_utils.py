import pyarrow as pa
import pytest

from dlt.destinations.impl.lancedb.utils import (
    create_filter_condition,
    fill_empty_source_column_values_with_placeholder,
)


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


def test_fill_empty_source_column_values_with_placeholder() -> None:
    data = [
        pa.array(["", "hello", ""]),
        pa.array(["hello", None, ""]),
        pa.array([1, 2, 3]),
        pa.array(["world", "", "arrow"]),
    ]
    table = pa.Table.from_arrays(data, names=["A", "B", "C", "D"])

    source_columns = ["A", "B"]
    placeholder = "placeholder"

    new_table = fill_empty_source_column_values_with_placeholder(table, source_columns, placeholder)

    expected_data = [
        pa.array(["placeholder", "hello", "placeholder"]),
        pa.array(["hello", "placeholder", "placeholder"]),
        pa.array([1, 2, 3]),
        pa.array(["world", "", "arrow"]),
    ]
    expected_table = pa.Table.from_arrays(expected_data, names=["A", "B", "C", "D"])
    assert new_table.equals(expected_table)


def test_create_filter_condition() -> None:
    assert (
        create_filter_condition("_dlt_load_id", pa.array(["A", "B", "C'c\n"]))
        == "_dlt_load_id IN ('A', 'B', 'C''c\\n')"
    )
    assert (
        create_filter_condition("_dlt_load_id", pa.array([1.2, 3, 5 / 2]))
        == "_dlt_load_id IN (1.2, 3.0, 2.5)"
    )
