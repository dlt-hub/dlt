import pyarrow as pa
import pytest

from dlt.destinations.impl.lance.utils import create_in_filter
from dlt.destinations.impl.lancedb.exceptions import is_lancedb_not_found_error


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


def test_create_filter_condition() -> None:
    # datafusion reads a backslash literally, so the newline stays one and only the quote doubles
    assert (
        create_in_filter("_dlt_load_id", pa.array(["A", "B", "C'c\n", "D\\d"]))
        == "_dlt_load_id IN ('A', 'B', 'C''c\n', 'D\\d')"
    )
    assert (
        create_in_filter("_dlt_load_id", pa.array([1.2, 3, 5 / 2]))
        == "_dlt_load_id IN (1.2, 3.0, 2.5)"
    )
    # a key column repeats its value once per row, the filter only needs the distinct ones
    assert (
        create_in_filter("_dlt_root_id", pa.array(["B", "A", "B"])) == "_dlt_root_id IN ('B', 'A')"
    )
    # a chunked array carries one dictionary per chunk
    assert (
        create_in_filter(
            "_dlt_root_id",
            pa.chunked_array(
                [pa.array(["A", "B"]).dictionary_encode(), pa.array(["C", "A"]).dictionary_encode()]
            ),
        )
        == "_dlt_root_id IN ('A', 'B', 'C')"
    )


def test_lancedb_exception_parsing() -> None:
    assert is_lancedb_not_found_error("Unknown table 'test_table'")
    assert is_lancedb_not_found_error("unknown table 'test_table'")
    assert is_lancedb_not_found_error("Field 'test_field' not found")
    assert is_lancedb_not_found_error("Column 'test_column' not found")
    assert is_lancedb_not_found_error("Missing value for column 'test_column'")
    assert is_lancedb_not_found_error("Missing column 'test_column'")
    assert is_lancedb_not_found_error("Table dlt_ci.my_table does not exist")
    assert not is_lancedb_not_found_error("Internal server error")
