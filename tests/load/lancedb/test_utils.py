import pyarrow as pa
import pytest

from dlt.destinations.impl.lancedb.utils import create_in_filter
from dlt.destinations.impl.lancedb.exceptions import is_lancedb_not_found_error


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


def test_create_filter_condition() -> None:
    assert (
        create_in_filter("_dlt_load_id", pa.array(["A", "B", "C'c\n"]))
        == "_dlt_load_id IN ('A', 'B', 'C''c\\n')"
    )
    assert (
        create_in_filter("_dlt_load_id", pa.array([1.2, 3, 5 / 2]))
        == "_dlt_load_id IN (1.2, 3.0, 2.5)"
    )


def test_lancedb_exception_parsing() -> None:
    assert is_lancedb_not_found_error("Unknown table 'test_table'")
    assert is_lancedb_not_found_error("unknown table 'test_table'")
    assert is_lancedb_not_found_error("Field 'test_field' not found")
    assert is_lancedb_not_found_error("Column 'test_column' not found")
    assert is_lancedb_not_found_error("Missing value for column 'test_column'")
    assert is_lancedb_not_found_error("Missing column 'test_column'")
