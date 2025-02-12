from datetime import date, datetime, timezone  # noqa: I251
from decimal import Decimal
from uuid import uuid4

import pyarrow as pa
import pytest

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.sources.sql_database.arrow_helpers import row_tuples_to_arrow


@pytest.mark.parametrize("all_unknown", [True, False])
def test_row_tuples_to_arrow_unknown_types(all_unknown: bool) -> None:
    """Test inferring data types with pyarrow"""

    rows = [
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
            Decimal("2.00000000000000000000000000000000000001"),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            {"foo": "baz"},
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
            Decimal("3.00001"), # precision (38, ...) is decimal128
            Decimal("3.00000000000000000000000000000000000001"),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            {"foo": "baz"},
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
            Decimal("4.00000000000000000000000000000000000001"),  # precision (>38, ...) is decimal256
            {"foo": "bar"},
            {"foo": "baz"},
        ),
    ]

    # Some columns don't specify data type and should be inferred
    columns = {
        "int_col": {"name": "int_col", "data_type": "bigint", "nullable": False},
        "str_col": {"name": "str_col", "data_type": "text", "nullable": False},
        "float_col": {"name": "float_col", "nullable": False},
        "bool_col": {"name": "bool_col", "data_type": "bool", "nullable": False},
        "date_col": {"name": "date_col", "nullable": False},
        "uuid_col": {"name": "uuid_col", "nullable": False},
        "datetime_col": {
            "name": "datetime_col",
            "data_type": "timestamp",
            "nullable": False,
        },
        "array_col": {"name": "array_col", "nullable": False},
        "decimal_col": {"name": "decimal_col", "nullable": False},
        "longer_decimal_col": {"name": "longer_decimal_col", "nullable": False},
        "mapping_col": {"name": "mapping_col", "nullable": False},
        "json_col": {"name": "json_col", "data_type": "json", "nullable": False},
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
    assert pa.types.is_struct(result[10].type)
    assert pa.types.is_string(result[11].type) is (columns["json_col"].get("data_type") == "json")


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
    DEFAULT_PRECISION, DEFAULT_SCALE = DestinationCapabilitiesContext().generic_capabilities().decimal_precision
    assert DEFAULT_PRECISION == 38
    assert DEFAULT_SCALE == 9
    
    col_name = "decimal_col"
    base_column = {col_name: {"name": col_name}}
    column_with_data_type = {col_name: {"name": col_name, "data_type": "decimal"}}
    column_with_precision = {col_name: {"name": col_name, "data_type": "decimal", "precision": 20}}
    column_with_scale = {col_name: {"name": col_name, "data_type": "decimal", "scale": 10}}
    column_with_precision_and_scale = {col_name: {"name":col_name, "data_type": "decimal", "precision": 22, "scale": 11}}

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

    # data_type="decimal" and precision specified
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_precision)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision[col_name]["precision"]
    assert arrow_field_type.scale == DEFAULT_SCALE

    # data_type="decimal" and scale specified
    arrow_table = row_tuples_to_arrow([[decimal_scale_7]], columns=column_with_scale)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == DEFAULT_PRECISION
    assert arrow_field_type.scale == column_with_scale[col_name]["scale"]

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
    with pytest.raises(RuntimeError):
        arrow_table = row_tuples_to_arrow([[decimal_scale_10]], columns=column_with_data_type)

    # setting sufficient precision and scale explicitly prevents the error
    arrow_table = row_tuples_to_arrow([[decimal_scale_10]], columns=column_with_precision_and_scale)
    arrow_field_type = arrow_table[col_name].type
    assert pa.types.is_decimal128(arrow_field_type)
    assert arrow_field_type.precision == column_with_precision_and_scale[col_name]["precision"]
    assert arrow_field_type.scale == column_with_precision_and_scale[col_name]["scale"]


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
