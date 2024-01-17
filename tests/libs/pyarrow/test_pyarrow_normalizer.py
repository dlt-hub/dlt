from typing import List, Any

import pyarrow as pa

from dlt.common.libs.pyarrow import normalize_py_arrow_schema
from dlt.common.normalizers import explicit_normalizers, import_normalizers
from dlt.common.schema.utils import new_column, TColumnSchema
from dlt.common.destination import DestinationCapabilitiesContext


def _normalize(table: pa.Table, columns: List[TColumnSchema]) -> pa.Table:
    _, naming, _ = import_normalizers(explicit_normalizers())
    caps = DestinationCapabilitiesContext()
    columns_schema = {c["name"]: c for c in columns}
    return normalize_py_arrow_schema(table, columns_schema, naming, caps)


def _row_at_index(table: pa.Table, index: int) -> List[Any]:
    return [table.column(column_name)[0].as_py() for column_name in table.column_names]


def test_quick_return_if_nothing_to_do() -> None:
    table = pa.Table.from_pylist(
        [
            {"a": 1, "b": 2},
        ]
    )
    columns = [new_column("a", "bigint"), new_column("b", "bigint")]
    result = _normalize(table, columns)
    # same object returned
    assert result == table


def test_pyarrow_reorder_columns() -> None:
    table = pa.Table.from_pylist(
        [
            {"col_new": "hello", "col1": 1, "col2": "a"},
        ]
    )
    columns = [new_column("col2", "text"), new_column("col1", "bigint")]
    result = _normalize(table, columns)
    # new columns appear at the end
    assert result.column_names == ["col2", "col1", "col_new"]
    assert _row_at_index(result, 0) == ["a", 1, "hello"]


def test_pyarrow_add_empty_types() -> None:
    table = pa.Table.from_pylist(
        [
            {"col1": 1},
        ]
    )
    columns = [new_column("col1", "bigint"), new_column("col2", "text")]
    result = _normalize(table, columns)
    # new columns appear at the end
    assert result.column_names == ["col1", "col2"]
    assert _row_at_index(result, 0) == [1, None]
    assert result.schema.field(1).type == "string"
