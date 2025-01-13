from datetime import timezone, datetime, date, timedelta  # noqa: I251
from copy import deepcopy
from typing import List, Any

import pytest
import pyarrow as pa

from dlt.common import pendulum
from dlt.common.libs.pyarrow import (
    py_arrow_to_table_schema_columns,
    from_arrow_scalar,
    get_py_arrow_timestamp,
    to_arrow_scalar,
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


def test_py_arrow_dict_to_column() -> None:
    array_1 = pa.array(["a", "b", "c"], type=pa.dictionary(pa.int8(), pa.string()))
    array_2 = pa.array([1, 2, 3], type=pa.dictionary(pa.int8(), pa.int64()))
    table = pa.table({"strings": array_1, "ints": array_2})
    columns = py_arrow_to_table_schema_columns(table.schema)
    assert columns == {
        "strings": {"name": "strings", "nullable": True, "data_type": "text"},
        "ints": {"name": "ints", "nullable": True, "data_type": "bigint"},
    }
    assert table.to_pydict() == {"strings": ["a", "b", "c"], "ints": [1, 2, 3]}


def test_to_arrow_scalar() -> None:
    naive_dt = get_py_arrow_timestamp(6, tz=None)
    # print(naive_dt)
    # naive datetimes are converted as UTC when time aware python objects are used
    assert to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32), naive_dt).as_py() == datetime(
        2021, 1, 1, 5, 2, 32
    )
    assert to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc), naive_dt
    ).as_py() == datetime(2021, 1, 1, 5, 2, 32)
    assert to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), naive_dt
    ).as_py() == datetime(2021, 1, 1, 5, 2, 32) + timedelta(hours=8)

    # naive datetimes are treated like UTC
    utc_dt = get_py_arrow_timestamp(6, tz="UTC")
    dt_converted = to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), utc_dt
    ).as_py()
    assert dt_converted.utcoffset().seconds == 0
    assert dt_converted == datetime(2021, 1, 1, 13, 2, 32, tzinfo=timezone.utc)

    berlin_dt = get_py_arrow_timestamp(6, tz="Europe/Berlin")
    dt_converted = to_arrow_scalar(
        datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone(timedelta(hours=-8))), berlin_dt
    ).as_py()
    # no dst
    assert dt_converted.utcoffset().seconds == 60 * 60
    assert dt_converted == datetime(2021, 1, 1, 13, 2, 32, tzinfo=timezone.utc)


def test_arrow_type_coercion() -> None:
    # coerce UTC python dt into naive arrow dt
    naive_dt = get_py_arrow_timestamp(6, tz=None)
    sc_dt = to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc), naive_dt)
    # does not convert to pendulum
    py_dt = from_arrow_scalar(sc_dt)
    assert not isinstance(py_dt, pendulum.DateTime)
    assert isinstance(py_dt, datetime)
    assert py_dt.tzname() is None

    # coerce datetime into date
    py_date = pa.date32()
    sc_date = to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc), py_date)
    assert from_arrow_scalar(sc_date) == date(2021, 1, 1)

    py_date = pa.date64()
    sc_date = to_arrow_scalar(datetime(2021, 1, 1, 5, 2, 32, tzinfo=timezone.utc), py_date)
    assert from_arrow_scalar(sc_date) == date(2021, 1, 1)


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
