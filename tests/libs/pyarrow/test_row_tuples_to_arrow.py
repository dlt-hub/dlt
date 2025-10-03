from datetime import date, datetime, timezone, time, timedelta  # noqa: I251
from uuid import uuid4
from typing import List, Tuple, Any

import pyarrow as pa
import pytest
from numpy.testing import assert_equal

from dlt.common import Decimal
from dlt.common.data_types.type_helpers import json_to_str
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.libs.pyarrow import (
    PyToArrowConversionException,
    transpose_rows_to_columns,
    convert_numpy_to_arrow,
    row_tuples_to_arrow,
    serialize_type,
)


def _caps() -> DestinationCapabilitiesContext:
    return DestinationCapabilitiesContext().generic_capabilities()


@pytest.mark.parametrize(
    "all_unknown,leading_nulls", [(True, True), (True, False), (False, True), (False, False)]
)
def test_row_tuples_to_arrow_unknown_types(all_unknown: bool, leading_nulls: bool) -> None:
    """Test inferring data types with pyarrow"""
    rows: List[Tuple[Any, ...]] = [
        (
            1,
            "a",
            1.1,
            True,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [1, 2, 3],
            Decimal("2.00001"),  # precision (38, ...) is decimal128
            Decimal(
                "2.00000000000000000000000000000000000001"
            ),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            '{"foo": "baz"}',
        ),
        (
            2,
            "b",
            2.2,
            False,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [4, 5, 6],
            Decimal("3.00001"),  # precision (38, ...) is decimal128
            Decimal(
                "3.00000000000000000000000000000000000001"
            ),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            '{"foo": "baz"}',
        ),
        (
            3,
            "c",
            3.3,
            True,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [7, 8, 9],
            Decimal("4.00001"),  # within default precision (38, ...) is decimal128
            Decimal(
                "4.00000000000000000000000000000000000001"
            ),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            '{"foo": "baz"}',
        ),
    ]

    # insert two rows full of None; they will be at the top of the column when inferring types
    if leading_nulls is True:
        null_row = tuple(None for _ in rows[0])
        rows.insert(0, null_row)
        rows.insert(0, null_row)

    # Some columns don't specify data type and should be inferred
    columns = {
        "int_col": {"name": "int_col", "data_type": "bigint"},
        "str_col": {"name": "str_col", "data_type": "text"},
        "float_col": {"name": "float_col"},
        "bool_col": {"name": "bool_col", "data_type": "bool"},
        "date_col": {"name": "date_col"},
        "uuid_col": {"name": "uuid_col"},
        "datetime_col": {"name": "datetime_col", "data_type": "timestamp"},
        "array_col": {"name": "array_col"},
        "decimal_col": {"name": "decimal_col"},
        "longer_decimal_col": {"name": "longer_decimal_col"},
        "mapping_col": {"name": "mapping_col", "data_type": "json"},
        "json_col": {"name": "json_col"},
    }

    if all_unknown:
        for col in columns.values():
            col.pop("data_type", None)

    # Call the function
    result = row_tuples_to_arrow(rows, _caps(), columns=columns, tz="UTC")  # type: ignore

    # Result is arrow table containing all columns in original order with correct types
    assert result.num_columns == len(columns)
    result_col_names = [f.name for f in result.schema]
    expected_names = list(columns)
    assert result_col_names == expected_names

    assert pa.types.is_int64(result[0].type)
    assert pa.types.is_string(result[1].type)
    assert pa.types.is_float64(result[2].type)
    assert pa.types.is_boolean(result[3].type)
    assert pa.types.is_date(result[4].type)
    assert pa.types.is_string(result[5].type)
    assert pa.types.is_timestamp(result[6].type)
    assert pa.types.is_list(result[7].type)
    assert pa.types.is_decimal128(result[8].type)
    assert pa.types.is_decimal256(result[9].type)
    if columns["mapping_col"].get("data_type") == "json":
        assert pa.types.is_string(result[10].type)
    else:
        assert pa.types.is_struct(result[10].type)

    assert pa.types.is_string(result[11].type)


def test_convert_to_arrow_json_is_string():
    """json data_type should be serialized to string by row_tuples_to_arrow and convert_numpy_to_arrow."""
    column_name = "json_col"
    columns_schema: TTableSchemaColumns = {column_name: {"name": column_name, "data_type": "json"}}
    value1 = {"foo": "bar"}
    value2 = {"foo": "baz"}
    rows = [(value1,), (value2,)]
    expected_column = [value1, value2]
    # note: json_to_str() is not equivalent to `json.dumps()` because it leaves no whitespace
    # between key and value e.g., `{"key":"value"}`
    expected_arrow_array = pa.array([json_to_str(value1), json_to_str(value2)])

    columns = transpose_rows_to_columns(rows, columns_schema)
    column = columns[column_name]
    assert_equal(columns[column_name], (expected_column))

    arrow_array = convert_numpy_to_arrow(
        column,
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert arrow_array.equals(expected_arrow_array)

    arrow_table = row_tuples_to_arrow(rows, _caps(), columns_schema, "UTC")
    assert pa.types.is_string(arrow_table[column_name].type)


def test_convert_to_arrow_null_column_is_not_removed():
    column_name = "null_col"
    columns_schema: TTableSchemaColumns = {
        "int_col": {"name": "int_col"},
        column_name: {"name": column_name},
    }
    rows = [(1, None), (2, None)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert pa.types.is_null(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, _caps(), columns_schema, "UTC")
    assert len(arrow_table.schema) == 2
    assert column_name in arrow_table.schema.names


def test_convert_to_arrow_null_column_with_data_type_is_not_removed():
    column_name = "null_col"
    columns_schema: TTableSchemaColumns = {
        "int_col": {"name": "int_col"},
        column_name: {"name": column_name, "data_type": "text"},
    }
    rows = [(1, None), (2, None)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert not pa.types.is_null(arrow_array.type)
    assert pa.types.is_string(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, _caps(), columns_schema, "UTC")
    assert len(arrow_table.schema) == 2
    assert column_name in arrow_table.schema.names


def test_convert_to_arrow_cast_string_to_decimal():
    column_name = "decimal_col"
    columns_schema: TTableSchemaColumns = {
        column_name: {"name": column_name, "data_type": "decimal"}
    }
    rows = [("2.00001",), (None,), ("3.00001",)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert pa.types.is_decimal128(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, _caps(), columns_schema, "UTC")
    assert pa.types.is_decimal128(arrow_table[column_name].type)


def test_convert_to_arrow_cast_float_to_decimal():
    column_name = "decimal_col"
    columns_schema: TTableSchemaColumns = {
        column_name: {"name": column_name, "data_type": "decimal"}
    }
    rows = [(2.00001,), (None,), (3.00001,)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert pa.types.is_decimal128(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, _caps(), columns_schema, "UTC")
    assert pa.types.is_decimal128(arrow_table[column_name].type)


def test_row_tuples_to_arrow_error_for_decimals() -> None:
    """Decimals are represented by precision and scale. In pyarrow, decimals are represented as strings.

    precision: the number of significant digits; default=38, which is a 128 bytes decimal
    scale: the number of decimal digitsl; default=9

    Currently, dlt has 3 behaviors when converting data to pyarrow:
    - pyarrow: pyarrow sets the precision, scale, and type (decimal128 vs. decimal256) based on the Python object
    - destination: dlt applies the destination's settings, default is (38, 9)
    - user: dlt applies the user specified decimal settings
    """
    # the test assumes the following default values
    DEFAULT_PRECISION, DEFAULT_SCALE = (
        DestinationCapabilitiesContext().generic_capabilities().decimal_precision
    )
    assert DEFAULT_PRECISION == 38
    assert DEFAULT_SCALE == 9

    col_name = "decimal_col"
    base_column: TTableSchemaColumns = {col_name: {"name": col_name}}
    column_with_data_type: TTableSchemaColumns = {
        col_name: {"name": col_name, "data_type": "decimal"}
    }
    column_with_precision: TTableSchemaColumns = {
        col_name: {"name": col_name, "data_type": "decimal", "precision": 20}
    }
    column_with_scale: TTableSchemaColumns = {
        col_name: {"name": col_name, "data_type": "decimal", "scale": 10}
    }
    column_with_precision_and_scale: TTableSchemaColumns = {
        col_name: {"name": col_name, "data_type": "decimal", "precision": 22, "scale": 11}
    }

    # decimal scale 7, within default (38, 9)
    decimal_scale_7 = Decimal("2.0000001")

    # if data_type is None, pyarrow infers precision and scale
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_7]], _caps(), columns=base_column, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 8
    assert arrow_field_type.scale == 7

    # data_type="decimal" and destination defaults
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_7]], _caps(), columns=column_with_data_type, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 38
    assert arrow_field_type.scale == DEFAULT_SCALE

    # data_type="decimal" and precision specified; scale defaults to 0
    with pytest.raises(PyToArrowConversionException):
        row_tuples_to_arrow(
            [[None], [decimal_scale_7]], _caps(), columns=column_with_precision, tz="UTC"
        )

    # data_type="decimal" and scale specified; if precision is None, default to destination capabilities
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_7]], _caps(), columns=column_with_scale, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == DEFAULT_PRECISION
    assert arrow_field_type.scale == DEFAULT_SCALE

    # if data_type="decimal" and precision and scale are set, use specified settings
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_7]], _caps(), columns=column_with_precision_and_scale, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision_and_scale[col_name]["precision"]
    assert arrow_field_type.scale == column_with_precision_and_scale[col_name]["scale"]

    # decimal scale 10, outside default (38, 9)
    decimal_scale_10 = Decimal("2.0000000001")

    # if data_type is None, pyarrow accommodates precision/scale outside destination values
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_10]], _caps(), columns=base_column, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 11
    assert arrow_field_type.scale == 10

    # if `data_type="decimal"`, but data is outside of destination (38, 9) raise error
    with pytest.raises(PyToArrowConversionException):
        arrow_table = row_tuples_to_arrow(
            [[None], [decimal_scale_10]], _caps(), columns=column_with_data_type, tz="UTC"
        )

    # setting sufficient precision and scale explicitly prevents the error
    arrow_table = row_tuples_to_arrow(
        [[None], [decimal_scale_10]], _caps(), columns=column_with_precision_and_scale, tz="UTC"
    )
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision_and_scale[col_name]["precision"]
    assert arrow_field_type.scale == column_with_precision_and_scale[col_name]["scale"]


@pytest.mark.parametrize(
    "value_kind,aware_input",
    [
        ("string", "naive"),
        ("string", "aware"),
        ("datetime", "naive"),
        ("datetime", "aware"),
    ],
    ids=[
        "string-naive",
        "string-aware",
        "datetime-naive",
        "datetime-aware",
    ],
)
@pytest.mark.parametrize(
    "tz_hint",
    [False, True, None],
    ids=["tz_false", "tz_true", "tz_none"],
)
def test_row_tuples_to_arrow_various_timestamps(
    value_kind: str, aware_input: str, tz_hint: Any
) -> None:
    """verify timestamp parsing from strings and datetime with different timezone hints.

    Args:
        value_kind: whether to use string or datetime values in rows.
        aware_input: whether input values include timezone info.
        tz_hint: column schema timezone flag (False, True, or None).
    """
    # prepare two sample timestamps
    v1: Any
    v2: Any
    if value_kind == "string":
        if aware_input == "aware":
            # include explicit timezone in the string
            v1 = "2024-01-01T12:34:56.123456Z"
            v2 = "2024-12-31T23:59:59.654321+00:00"
        else:
            # naive iso8601 strings without timezone
            v1 = "2024-01-01T12:34:56.123456"
            v2 = "2024-12-31T23:59:59.654321"
    else:
        if aware_input == "aware":
            v1 = datetime(2024, 1, 1, 12, 34, 56, 123456, tzinfo=timezone.utc)
            v2 = datetime(2024, 12, 31, 23, 59, 59, 654321, tzinfo=timezone.utc)
        else:
            v1 = datetime(2024, 1, 1, 12, 34, 56, 123456)
            v2 = datetime(2024, 12, 31, 23, 59, 59, 654321)

    rows = [(v1,), (None,), (v2,)]

    columns: TTableSchemaColumns = {
        "ts": {
            "name": "ts",
            "data_type": "timestamp",
        }
    }
    if tz_hint is not None:
        # explicitly test False, True and None for timezone behavior
        columns["ts"]["timezone"] = tz_hint

    # convert and validate
    tbl = row_tuples_to_arrow(rows, _caps(), columns=columns, tz="UTC")
    assert tbl.num_columns == 1
    assert tbl.num_rows == 3

    col_type = tbl["ts"].type
    assert pa.types.is_timestamp(col_type)
    # default precision is 6 -> microseconds
    assert col_type.unit == "us"
    # timezone hint controls tz metadata
    expected_tz = None if tz_hint is False else "UTC"
    assert col_type.tz == expected_tz


def test_row_tuples_to_arrow_pandas_ns_downcasts_to_us() -> None:
    """pandas nanosecond precision timestamps are cast to microseconds (precision=6).

    this ensures that high precision inputs do not exceed the destination default precision.
    """
    pd = pytest.importorskip("pandas")

    # create pandas timestamps with nanosecond precision
    ts1 = pd.Timestamp("2024-01-01T12:34:56.123456789Z")
    ts2 = pd.Timestamp("2024-01-01T12:34:57.987654321Z")

    rows = [(ts1,), (None,), (ts2,)]
    columns: TTableSchemaColumns = {
        "ts": {"name": "ts", "data_type": "timestamp", "timezone": True}
    }

    tbl = row_tuples_to_arrow(rows, _caps(), columns=columns, tz="UTC")
    col_type = tbl["ts"].type

    # verify type respects default precision (6 -> microseconds) and utc tz
    assert pa.types.is_timestamp(col_type)
    assert col_type.unit == "us"
    assert col_type.tz == "UTC"

    # verify values are truncated to microseconds (drop the last 3 ns digits)
    v_py = tbl["ts"][0].as_py()
    assert isinstance(v_py, datetime)
    assert v_py.microsecond == 123456


@pytest.mark.parametrize(
    "value_kind",
    [
        "string-naive",
        "string-aware",
        "time",
        "timedelta",
    ],
    ids=[
        "string-naive",
        "string-aware",
        "time",
        "timedelta",
    ],
)
def test_row_tuples_to_arrow_various_times(value_kind: str) -> None:
    """verify time parsing for time data_type.

    tests python time objects, naive time strings, timezone-suffixed strings, and timedelta.
    timezone is not supported for time columns, and timedelta should not be implicitly cast to time.
    """
    # prepare sample values per kind
    v1: Any
    v2: Any
    if value_kind == "string-naive":
        v1 = "12:34:56.123456"
        v2 = "23:59:59.654321"
    elif value_kind == "string-aware":
        # time strings with timezone/offset are not supported for dlt time columns
        v1 = "12:34:56.123456+02:00"
        v2 = "23:59:59.654321+00:00"
    elif value_kind == "time":
        v1 = time(12, 34, 56, 123456)
        v2 = time(23, 59, 59, 654321)
    elif value_kind == "timedelta":
        v1 = timedelta(hours=12, minutes=34, seconds=56, microseconds=123456)
        v2 = timedelta(seconds=48405, microseconds=654321)
    else:
        raise AssertionError("unexpected value_kind")

    rows = [(v1,), (None,), (v2,)]
    columns: TTableSchemaColumns = {"t": {"name": "t", "data_type": "time"}}

    # valid inputs should convert to time with default precision (microseconds)
    tbl = row_tuples_to_arrow(rows, _caps(), columns=columns, tz="UTC")
    assert tbl.num_columns == 1
    assert tbl.num_rows == 3

    col_type = tbl["t"].type
    assert pa.types.is_time(col_type)
    assert col_type.unit == "us"

    # verify first value is parsed with expected microseconds
    first = tbl["t"][0].as_py()
    assert isinstance(first, time)
    assert first.microsecond == 123456

    assert tbl["t"][1].as_py() is None

    second = tbl["t"][2].as_py()
    assert isinstance(second, time)
    assert second.microsecond == 654321


def test_row_tuples_to_arrow_detects_range_type() -> None:
    pytest.importorskip("sqlalchemy", minversion="2.0")
    from sqlalchemy.dialects.postgresql import Range  # type: ignore[attr-defined]

    # Applies to NUMRANGE, DATERANGE, etc sql types. Sqlalchemy returns a Range dataclass
    IntRange = Range

    rows = [
        (IntRange(1, 10),),
        (IntRange(2, 20),),
        (IntRange(3, 30),),
    ]
    result = row_tuples_to_arrow(
        rows,
        _caps(),
        columns={"range_col": {"name": "range_col", "nullable": False}},
        tz="UTC",
    )
    assert result.num_columns == 1
    assert pa.types.is_struct(result[0].type)

    # Check range has all fields
    range_type = result[0].type
    range_fields = {f.name: f for f in range_type}
    assert pa.types.is_int64(range_fields["lower"].type)
    assert pa.types.is_int64(range_fields["upper"].type)
    assert pa.types.is_boolean(range_fields["empty"].type)
    assert pa.types.is_string(range_fields["bounds"].type)


def test_json_fallback_to_string_for_non_coercible_nested_objects() -> None:
    """ensure json columns fall back to string when nested values cannot be coerced to a uniform nested type.

    this exercises convert_numpy_to_arrow fallback case 2: values are encoded to json strings.
    """
    # mix incompatible python container types across rows: list in one row, dict in another
    rows = [([1, 2, 3],), (None,), ({"a": 1},)]
    columns: TTableSchemaColumns = {"mixed": {"name": "mixed", "data_type": "json"}}

    tbl = row_tuples_to_arrow(rows, _caps(), columns=columns, tz="UTC")

    # result must be a string column with json-serialized values
    assert pa.types.is_string(tbl["mixed"].type)
    values = tbl["mixed"].to_pylist()
    assert values[0] == json_to_str([1, 2, 3])
    assert values[1] is None
    assert values[2] == json_to_str({"a": 1})


def test_json_with_nested_type_hint_produces_real_nested_type() -> None:
    """json column with x-nested-type hint should materialize as a real nested arrow type when supported.

    sets DestinationCapabilitiesContext.supports_nested_types=True and provides a serialized nested type.
    """
    caps = _caps()
    caps.supports_nested_types = True

    # define desired nested type: struct<a: int64, b: list<string>>
    hinted_type = pa.struct(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.list_(pa.string())),
        ]
    )
    hinted_type_serialized = serialize_type(hinted_type)

    columns: TTableSchemaColumns = {
        "obj": {
            "name": "obj",
            "data_type": "json",
            "x-nested-type": hinted_type_serialized,  # type: ignore[typeddict-unknown-key]
        }
    }

    rows = [({"a": 1, "b": ["x", "y"]},), (None,), ({"a": 2, "b": ["z"]},)]

    tbl = row_tuples_to_arrow(rows, caps, columns=columns, tz="UTC")

    col_type = tbl["obj"].type
    assert pa.types.is_struct(col_type)
    # ensure it is exactly the hinted type
    assert col_type == hinted_type
