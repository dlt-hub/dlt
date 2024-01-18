from copy import deepcopy
from typing import List, Any

import pytest
import pyarrow as pa

from dlt.common.libs.pyarrow import (
    py_arrow_to_table_schema_columns,
    get_py_arrow_datatype,
    remove_null_columns,
    remove_columns,
    append_column,
    rename_columns,
    is_arrow_item,
)
from dlt.common.destination import DestinationCapabilitiesContext
from tests.cases import TABLE_UPDATE_COLUMNS_SCHEMA


def test_py_arrow_to_table_schema_columns():
    dlt_schema = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    caps = DestinationCapabilitiesContext.generic_capabilities()
    # The arrow schema will add precision
    dlt_schema["col4"]["precision"] = caps.timestamp_precision
    dlt_schema["col6"]["precision"], dlt_schema["col6"]["scale"] = caps.decimal_precision
    dlt_schema["col11"]["precision"] = caps.timestamp_precision
    dlt_schema["col4_null"]["precision"] = caps.timestamp_precision
    dlt_schema["col6_null"]["precision"], dlt_schema["col6_null"]["scale"] = caps.decimal_precision
    dlt_schema["col11_null"]["precision"] = caps.timestamp_precision

    # Ignoring wei as we can't distinguish from decimal
    dlt_schema["col8"]["precision"], dlt_schema["col8"]["scale"] = (76, 0)
    dlt_schema["col8"]["data_type"] = "decimal"
    dlt_schema["col8_null"]["precision"], dlt_schema["col8_null"]["scale"] = (76, 0)
    dlt_schema["col8_null"]["data_type"] = "decimal"
    # No json type
    dlt_schema["col9"]["data_type"] = "text"
    del dlt_schema["col9"]["variant"]
    dlt_schema["col9_null"]["data_type"] = "text"
    del dlt_schema["col9_null"]["variant"]

    # arrow string fields don't have precision
    del dlt_schema["col5_precision"]["precision"]

    # Convert to arrow schema
    arrow_schema = pa.schema(
        [
            pa.field(
                column["name"],
                get_py_arrow_datatype(column, caps, "UTC"),
                nullable=column["nullable"],
            )
            for column in dlt_schema.values()
        ]
    )

    result = py_arrow_to_table_schema_columns(arrow_schema)

    # Resulting schema should match the original
    assert result == dlt_schema


def _row_at_index(table: pa.Table, index: int) -> List[Any]:
    return [table.column(column_name)[index].as_py() for column_name in table.column_names]


@pytest.mark.parametrize("pa_type", [pa.Table, pa.RecordBatch])
def test_remove_null_columns(pa_type: Any) -> None:
    table = pa_type.from_pylist(
        [
            {"a": 1, "b": 2, "c": None},
            {"a": 1, "b": None, "c": None},
        ]
    )
    result = remove_null_columns(table)
    assert result.column_names == ["a", "b"]
    assert _row_at_index(result, 0) == [1, 2]
    assert _row_at_index(result, 1) == [1, None]


@pytest.mark.parametrize("pa_type", [pa.Table, pa.RecordBatch])
def test_remove_columns(pa_type: Any) -> None:
    table = pa_type.from_pylist(
        [
            {"a": 1, "b": 2, "c": 5},
            {"a": 1, "b": 3, "c": 4},
        ]
    )
    result = remove_columns(table, ["b"])
    assert result.column_names == ["a", "c"]
    assert _row_at_index(result, 0) == [1, 5]
    assert _row_at_index(result, 1) == [1, 4]


@pytest.mark.parametrize("pa_type", [pa.Table, pa.RecordBatch])
def test_append_column(pa_type: Any) -> None:
    table = pa_type.from_pylist(
        [
            {"a": 1, "b": 2},
            {"a": 1, "b": 3},
        ]
    )
    result = append_column(table, "c", pa.array([5, 6]))
    assert result.column_names == ["a", "b", "c"]
    assert _row_at_index(result, 0) == [1, 2, 5]
    assert _row_at_index(result, 1) == [1, 3, 6]


@pytest.mark.parametrize("pa_type", [pa.Table, pa.RecordBatch])
def test_rename_column(pa_type: Any) -> None:
    table = pa_type.from_pylist(
        [
            {"a": 1, "b": 2, "c": 5},
            {"a": 1, "b": 3, "c": 4},
        ]
    )
    result = rename_columns(table, ["one", "two", "three"])
    assert result.column_names == ["one", "two", "three"]
    assert _row_at_index(result, 0) == [1, 2, 5]
    assert _row_at_index(result, 1) == [1, 3, 4]


@pytest.mark.parametrize("pa_type", [pa.Table, pa.RecordBatch])
def test_is_arrow_item(pa_type: Any) -> None:
    table = pa_type.from_pylist(
        [
            {"a": 1, "b": 2, "c": 5},
            {"a": 1, "b": 3, "c": 4},
        ]
    )
    assert is_arrow_item(table)
    assert not is_arrow_item(table.to_pydict())
    assert not is_arrow_item("hello")
