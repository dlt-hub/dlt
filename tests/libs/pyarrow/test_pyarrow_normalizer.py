from typing import List, Any

import pyarrow as pa
import pytest

from dlt.common.libs.pyarrow import normalize_py_arrow_item, NameNormalizationCollision
from dlt.common.schema.utils import new_column, TColumnSchema
from dlt.common.schema.normalizers import explicit_normalizers, import_normalizers
from dlt.common.destination import DestinationCapabilitiesContext


def _normalize(table: pa.Table, columns: List[TColumnSchema]) -> pa.Table:
    _, naming, _ = import_normalizers(explicit_normalizers())
    caps = DestinationCapabilitiesContext()
    columns_schema = {c["name"]: c for c in columns}
    return normalize_py_arrow_item(table, columns_schema, naming, caps)


def _row_at_index(table: pa.Table, index: int) -> List[Any]:
    return [table.column(column_name)[index].as_py() for column_name in table.column_names]


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


def test_field_normalization_clash() -> None:
    table = pa.Table.from_pylist(
        [
            {"col^New": "hello", "col_new": 1},
        ]
    )
    with pytest.raises(NameNormalizationCollision):
        _normalize(table, [])


def test_field_normalization() -> None:
    table = pa.Table.from_pylist(
        [
            {"col^New": "hello", "col2": 1},
        ]
    )
    result = _normalize(table, [])
    assert result.column_names == ["col_new", "col2"]
    assert _row_at_index(result, 0) == ["hello", 1]


def test_default_dlt_columns_not_added() -> None:
    table = pa.Table.from_pylist(
        [
            {"col1": 1},
        ]
    )
    columns = [
        new_column("_dlt_something", "bigint"),
        new_column("_dlt_id", "text"),
        new_column("_dlt_load_id", "text"),
        new_column("col2", "text"),
        new_column("col1", "text"),
    ]
    result = _normalize(table, columns)
    # no dlt_id or dlt_load_id columns
    assert result.column_names == ["_dlt_something", "col2", "col1"]
    assert _row_at_index(result, 0) == [None, None, 1]


def test_non_nullable_columns() -> None:
    """Tests the case where arrow table is created with incomplete schema info,
    such as when converting pandas dataframe to arrow. In this case normalize
    should update not-null constraints in the arrow schema.
    """
    table = pa.Table.from_pylist(
        [
            {
                "col1": 1,
                "col2": "hello",
                # Include column that will be renamed by normalize
                # To ensure nullable flag mapping is correct
                "Col 3": "world",
            },
        ]
    )
    columns = [
        new_column("col1", "bigint", nullable=False),
        new_column("col2", "text"),
        new_column("col_3", "text", nullable=False),
    ]
    result = _normalize(table, columns)

    assert result.column_names == ["col1", "col2", "col_3"]
    # Not-null columns are updated in arrow
    assert result.schema.field("col1").nullable is False
    assert result.schema.field("col_3").nullable is False
    # col2 is still nullable
    assert result.schema.field("col2").nullable is True


@pytest.mark.skip(reason="Somehow this does not fail, should we add an exception??")
def test_fails_if_adding_non_nullable_column() -> None:
    table = pa.Table.from_pylist(
        [
            {"col1": 1},
        ]
    )
    columns = [new_column("col1", "bigint"), new_column("col2", "text", nullable=False)]
    with pytest.raises(Exception):
        _normalize(table, columns)
