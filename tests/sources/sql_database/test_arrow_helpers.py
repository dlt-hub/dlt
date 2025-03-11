from datetime import date, datetime, timezone  # noqa: I251
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
)
from dlt.sources.sql_database.arrow_helpers import row_tuples_to_arrow


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
    result = row_tuples_to_arrow(rows, columns=columns, tz="UTC")  # type: ignore

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


def test_convert_to_arrow_json_is_not_a_string():
    column_name = "json_col"
    columns_schema: TTableSchemaColumns = {column_name: {"name": column_name, "data_type": "json"}}
    value1 = {"foo": "bar"}
    value2 = {"foo": "baz"}
    rows = [(value1,), (value2,)]
    expected_column = [value1, value2]
    # NOTE json_to_str() is not equivalent to `json.dumps()` because it leaves no whitespace
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

    arrow_table = row_tuples_to_arrow(rows, columns_schema, "UTC")
    assert pa.types.is_string(arrow_table[column_name].type)


def test_convert_to_arrow_null_column_is_removed():
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

    arrow_table = row_tuples_to_arrow(rows, columns_schema, "UTC")
    assert len(arrow_table.schema) == 1
    assert column_name not in arrow_table.schema.names


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

    arrow_table = row_tuples_to_arrow(rows, columns_schema, "UTC")
    assert len(arrow_table.schema) == 2
    assert column_name in arrow_table.schema.names


def test_convert_to_arrow_cast_string_to_decimal():
    column_name = "decimal_col"
    columns_schema: TTableSchemaColumns = {
        column_name: {"name": column_name, "data_type": "decimal"}
    }
    rows = [("2.00001",), ("3.00001",)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert pa.types.is_decimal128(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, columns_schema, "UTC")
    assert pa.types.is_decimal128(arrow_table[column_name].type)


def test_convert_to_arrow_cast_float_to_decimal():
    column_name = "decimal_col"
    columns_schema: TTableSchemaColumns = {
        column_name: {"name": column_name, "data_type": "decimal"}
    }
    rows = [(2.00001,), (3.00001,)]

    columns = transpose_rows_to_columns(rows, columns_schema)
    arrow_array = convert_numpy_to_arrow(
        columns[column_name],
        DestinationCapabilitiesContext().generic_capabilities(),
        columns_schema[column_name],
        "UTC",
        True,
    )
    assert pa.types.is_decimal128(arrow_array.type)

    arrow_table = row_tuples_to_arrow(rows, columns_schema, "UTC")
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
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=base_column)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 8
    assert arrow_field_type.scale == 7

    # data_type="decimal" and destination defaults
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_data_type)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 38
    assert arrow_field_type.scale == DEFAULT_SCALE

    # data_type="decimal" and precision specified; scale defaults to 0
    with pytest.raises(PyToArrowConversionException):
        row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_precision)

    # data_type="decimal" and scale specified; if precision is None, default to destination capabilities
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_scale)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == DEFAULT_PRECISION
    assert arrow_field_type.scale == DEFAULT_SCALE

    # if data_type="decimal" and precision and scale are set, use specified settings
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_precision_and_scale)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision_and_scale[col_name]["precision"]
    assert arrow_field_type.scale == column_with_precision_and_scale[col_name]["scale"]

    # decimal scale 10, outside default (38, 9)
    decimal_scale_10 = Decimal("2.0000000001")

    # if data_type is None, pyarrow accommodates precision/scale outside destination values
    arrow_table = row_tuples_to_arrow([[decimal_scale_10]], columns=base_column)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == 11
    assert arrow_field_type.scale == 10

    # if `data_type="decimal"`, but data is outside of destination (38, 9) raise error
    with pytest.raises(PyToArrowConversionException):
        arrow_table = row_tuples_to_arrow([[decimal_scale_10]], columns=column_with_data_type)

    # setting sufficient precision and scale explicitly prevents the error
    arrow_table = row_tuples_to_arrow([[decimal_scale_10]], columns=column_with_precision_and_scale)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision_and_scale[col_name]["precision"]
    assert arrow_field_type.scale == column_with_precision_and_scale[col_name]["scale"]


pytest.importorskip("sqlalchemy", minversion="2.0")


def test_row_tuples_to_arrow_detects_range_type() -> None:
    from sqlalchemy.dialects.postgresql import Range  # type: ignore[attr-defined]

    # Applies to NUMRANGE, DATERANGE, etc sql types. Sqlalchemy returns a Range dataclass
    IntRange = Range

    rows = [
        (IntRange(1, 10),),
        (IntRange(2, 20),),
        (IntRange(3, 30),),
    ]
    result = row_tuples_to_arrow(
        rows=rows,
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
