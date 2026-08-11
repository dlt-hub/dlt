from typing import List

import pyarrow as pa
import pytest

from dlt.common.schema import TSchemaTables
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.lance.lance_adapter import REMOVE_ORPHANS_HINT
from dlt.destinations.impl.lance.utils import create_in_filter, set_remove_orphans_hint
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


def _doc_tables(merge_keys: List[str]) -> TSchemaTables:
    return {
        "doc": {
            "name": "doc",
            "columns": {
                key: {"name": key, "data_type": "text", "nullable": False, "merge_key": True}
                for key in merge_keys
            },
        },
        "doc__chunk": {"name": "doc__chunk", "parent": "doc", "columns": {}},
    }


@pytest.mark.parametrize("table_name", ["doc", "doc__chunk"])
@pytest.mark.parametrize(
    "merge_keys,expected",
    [([], False), (["doc_id"], True), (["doc_id", "chunk_hash"], False)],
)
def test_remove_orphans_hint_follows_merge_key(
    table_name: str, merge_keys: List[str], expected: bool
) -> None:
    tables = _doc_tables(merge_keys)
    table = set_remove_orphans_hint(tables[table_name], tables)  # type: ignore[arg-type]
    assert table[REMOVE_ORPHANS_HINT] is expected  # type: ignore[literal-required]


@pytest.mark.parametrize("table_name", ["doc", "doc__chunk"])
@pytest.mark.parametrize("hint", [True, False])
def test_remove_orphans_hint_explicit_wins(table_name: str, hint: bool) -> None:
    # a single merge key would resolve to True, the resource opted out of it
    tables = _doc_tables(["doc_id"])
    tables["doc"][REMOVE_ORPHANS_HINT] = hint  # type: ignore[literal-required]
    table = set_remove_orphans_hint(tables[table_name], tables)  # type: ignore[arg-type]
    assert table[REMOVE_ORPHANS_HINT] is hint  # type: ignore[literal-required]


def test_remove_orphans_hint_is_not_overwritten() -> None:
    tables = _doc_tables(["doc_id"])
    table: PreparedTableSchema = {**tables["doc"], REMOVE_ORPHANS_HINT: False}  # type: ignore[misc]
    assert set_remove_orphans_hint(table, tables)[REMOVE_ORPHANS_HINT] is False  # type: ignore[literal-required]


def test_lancedb_exception_parsing() -> None:
    assert is_lancedb_not_found_error("Unknown table 'test_table'")
    assert is_lancedb_not_found_error("unknown table 'test_table'")
    assert is_lancedb_not_found_error("Field 'test_field' not found")
    assert is_lancedb_not_found_error("Column 'test_column' not found")
    assert is_lancedb_not_found_error("Missing value for column 'test_column'")
    assert is_lancedb_not_found_error("Missing column 'test_column'")
    assert is_lancedb_not_found_error("Table dlt_ci.my_table does not exist")
    assert not is_lancedb_not_found_error("Internal server error")
