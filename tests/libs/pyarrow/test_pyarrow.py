from datetime import timezone, datetime, date, timedelta  # noqa: I251
from copy import deepcopy
from typing import List, Any

import pytest
import pyarrow as pa

from dlt.common import pendulum
from dlt.common.destination.capabilities import adjust_schema_to_capabilities
from dlt.common.libs.pyarrow import (
    columns_to_arrow,
    deserialize_type,
    fill_empty_source_column_values_with_placeholder,
    get_column_type_from_py_arrow,
    py_arrow_to_table_schema_columns,
    from_arrow_scalar,
    get_py_arrow_timestamp,
    serialize_type,
    to_arrow_scalar,
    get_py_arrow_datatype,
    remove_null_columns,
    remove_columns,
    append_column,
    rename_columns,
    is_arrow_item,
    remove_null_columns_from_schema,
    UnsupportedArrowTypeException,
    cast_date64_columns_to_timestamp,
)
from dlt.common.destination import DestinationCapabilitiesContext
from tests.cases import table_update_and_row


def test_py_arrow_to_table_schema_columns():
    dlt_schema, _ = table_update_and_row()

    caps = DestinationCapabilitiesContext.generic_capabilities()
    # The arrow schema will add precision
    dlt_schema["col4"]["precision"] = caps.timestamp_precision
    dlt_schema["col6"]["precision"], dlt_schema["col6"]["scale"] = caps.decimal_precision
    dlt_schema["col11"]["precision"] = caps.timestamp_precision
    dlt_schema["col4_null"]["precision"] = caps.timestamp_precision
    dlt_schema["col6_null"]["precision"], dlt_schema["col6_null"]["scale"] = caps.decimal_precision
    dlt_schema["col11_null"]["precision"] = caps.timestamp_precision
    dlt_schema["col12"]["precision"] = caps.timestamp_precision

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


@pytest.mark.parametrize("supports_nested_types", (True, False))
def test_py_arrow_to_table_schema_columns_nested_types(supports_nested_types: bool):
    """Test py_arrow_to_table_schema_columns with various nested types, including dictionary and deeply nested structures."""
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.supports_nested_types = supports_nested_types

    # Simple nested types
    struct_type = pa.struct(
        [pa.field("f1", pa.int32()), pa.field("f2", pa.string()), pa.field("f3", pa.bool_())]
    )

    list_type = pa.list_(pa.int64())
    list_struct_type = pa.list_(struct_type)
    map_type = pa.map_(pa.string(), pa.float64())

    # Deep nesting
    deep_nested_type = pa.list_(
        pa.struct(
            [
                pa.field("nested_list", pa.list_(pa.int32())),
                pa.field(
                    "nested_map",
                    pa.map_(
                        pa.string(),
                        pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.string())]),
                    ),
                ),
                pa.field("nested_dict", pa.dictionary(pa.int16(), pa.list_(pa.string()))),
            ]
        )
    )

    # Create schema with all these types
    schema = pa.schema(
        [
            pa.field("struct_col", struct_type),
            pa.field("list_col", list_type),
            pa.field("list_struct_col", list_struct_type),
            pa.field("map_col", map_type),
            pa.field("deep_nested_col", deep_nested_type),
        ]
    )

    # Convert to table schema columns
    columns = py_arrow_to_table_schema_columns(schema)
    adjust_schema_to_capabilities(columns, caps)

    # Verify all columns are correctly identified as JSON data type
    for _, column in columns.items():
        assert column["data_type"] == "json"
        if supports_nested_types:
            assert "x-nested-type" in column
            assert column["x-nested-type"].startswith("arrow-ipc:")  # type: ignore[typeddict-item]
        else:
            assert "x-nested-type" not in column

    # Convert back to Arrow schema for roundtrip test
    roundtrip_schema = columns_to_arrow(columns, caps)

    # Compare original types with roundtrip types
    for field_name in schema.names:
        roundtrip_type = roundtrip_schema.field(field_name).type
        if supports_nested_types:
            original_type = schema.field(field_name).type
            assert str(original_type) == str(
                roundtrip_type
            ), f"Types don't match for {field_name}: {original_type} vs {roundtrip_type}"
        else:
            assert roundtrip_type == pa.string()


def test_nested_type_serialization_deserialization():
    """Test that nested types can be correctly serialized and deserialized."""
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.supports_nested_types = True

    # Create a complex nested type
    nested_type = pa.struct(
        [
            pa.field("list_field", pa.list_(pa.int32())),
            pa.field(
                "struct_field",
                pa.struct(
                    [pa.field("nested_int", pa.int64()), pa.field("nested_string", pa.string())]
                ),
            ),
            pa.field("map_field", pa.map_(pa.string(), pa.float64())),
            pa.field("dict_field", pa.dictionary(pa.int8(), pa.string())),
        ]
    )

    # Serialize the type
    serialized = serialize_type(nested_type)

    # Verify the serialized string format
    assert serialized.startswith("arrow-ipc:")

    # Deserialize and compare
    deserialized = deserialize_type(serialized)

    # Compare the string representations to verify equality
    assert str(nested_type) == str(deserialized)

    # Test with table schema conversion
    schema = pa.schema([pa.field("nested_column", nested_type)])
    columns = py_arrow_to_table_schema_columns(schema)

    # Verify the column is marked as JSON and has the serialized type
    assert columns["nested_column"]["data_type"] == "json"
    assert "x-nested-type" in columns["nested_column"]

    # Deserialize the stored type and compare
    stored_type = deserialize_type(columns["nested_column"]["x-nested-type"])  # type: ignore[typeddict-item]
    assert str(nested_type) == str(stored_type)

    # Complete the roundtrip by converting back to Arrow
    roundtrip_schema = columns_to_arrow(columns, caps)

    # Verify the roundtrip
    assert str(schema.field("nested_column").type) == str(
        roundtrip_schema.field("nested_column").type
    )


def test_py_arrow_to_table_schema_columns_dict_in_struct():
    """Test dictionary types nested inside struct types."""
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.supports_nested_types = True
    # Create schema with dictionary inside struct
    arrow_schema = pa.schema(
        [
            pa.field(
                "struct_with_dict",
                pa.struct(
                    [
                        pa.field("dict_field", pa.dictionary(pa.int32(), pa.string())),
                        pa.field("normal_field", pa.int64()),
                    ]
                ),
            ),
        ]
    )

    # Convert to table schema columns
    columns = py_arrow_to_table_schema_columns(arrow_schema)

    # Struct with dict should be converted to json type with nested-type info
    assert columns["struct_with_dict"]["data_type"] == "json"
    assert "x-nested-type" in columns["struct_with_dict"]

    # Roundtrip the schema
    reconstructed_schema = columns_to_arrow(columns, caps, "UTC")

    # The nested structure should be preserved
    struct_type = reconstructed_schema.field("struct_with_dict").type
    assert pa.types.is_struct(struct_type)

    # Dictionary encoding may not be preserved, but the value type should be
    dict_field_type = struct_type.field("dict_field").type
    assert pa.types.is_string(dict_field_type.value_type)


def test_py_arrow_to_table_schema_columns_nested_dict_types():
    """Test dictionary types with nested value types."""
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.supports_nested_types = True

    # Create schema with dictionary of nested types
    nested_list_type = pa.list_(pa.list_(pa.int64()))
    nested_struct_type = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])

    arrow_schema = pa.schema(
        [
            pa.field("dict_of_lists", pa.dictionary(pa.int32(), nested_list_type)),
            pa.field("dict_of_structs", pa.dictionary(pa.int32(), nested_struct_type)),
            pa.field("list_of_dicts", pa.list_(pa.dictionary(pa.int8(), pa.string()))),
        ]
    )

    # Convert to table schema columns
    columns = py_arrow_to_table_schema_columns(arrow_schema)

    # Dict of lists and dict of structs should be converted to the value types
    assert columns["dict_of_lists"]["data_type"] == "json"
    assert columns["dict_of_structs"]["data_type"] == "json"
    assert columns["list_of_dicts"]["data_type"] == "json"

    # Each should have nested type information
    for col in ["dict_of_lists", "dict_of_structs", "list_of_dicts"]:
        assert "x-nested-type" in columns[col]

    # Roundtrip and verify structure is preserved
    reconstructed_schema = columns_to_arrow(columns, caps, "UTC")

    # The structures should be preserved even if dictionary encoding is not
    for i, field in enumerate(arrow_schema):
        original_type = field.type
        reconstructed_type = reconstructed_schema.field(i).type

        # For dictionary types, compare the value types, when converting
        # they are converted into their value type and then because type is nested
        # it gets converted with arrow ipc
        if pa.types.is_dictionary(original_type):
            original_value_type = original_type.value_type
            reconstructed_value_type = reconstructed_type

            # Compare the representation of value types
            assert str(original_value_type) == str(reconstructed_value_type)
        else:
            # For non-dictionary types, compare the full type
            assert str(original_type) == str(reconstructed_type)


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


def test_exception_for_unsupported_arrow_type() -> None:
    # arrow type `duration` is currently unsupported by dlt
    obj = pa.duration("s")
    # error on type conversion
    with pytest.raises(UnsupportedArrowTypeException):
        get_column_type_from_py_arrow(obj)


def test_exception_for_schema_with_unsupported_arrow_type() -> None:
    table = pa.table(
        {
            "col1": pa.array([1, 2], type=pa.int32()),
            "col2": pa.array([timedelta(days=1), timedelta(days=2)], type=pa.duration("s")),
        }
    )

    # assert the exception is raised
    with pytest.raises(UnsupportedArrowTypeException) as excinfo:
        py_arrow_to_table_schema_columns(table.schema)

    (msg,) = excinfo.value.args
    assert "duration" in msg
    assert "col2" in msg


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


def test_null_arrow_type() -> None:
    obj = pa.null()
    column_type = get_column_type_from_py_arrow(obj)
    assert {"seen-null-first": True} == column_type["x-normalizer"]  # type: ignore[typeddict-item]


def test_remove_null_columns_from_schema() -> None:
    schema = pa.schema(
        [
            pa.field("col1", pa.int32()),
            pa.field("col2", pa.null()),
            pa.field("col3", pa.string()),
            pa.field("col4", pa.null()),
        ]
    )

    new_schema, contains_null = remove_null_columns_from_schema(schema)
    assert contains_null is True
    assert new_schema.names == ["col1", "col3"]
    assert all(not pa.types.is_null(f.type) for f in new_schema)


def test_fill_empty_source_column_values_with_placeholder() -> None:
    data = [
        pa.array(["", "hello", ""]),
        pa.array(["hello", None, ""]),
        pa.array([1, 2, 3]),
        pa.array(["world", "", "arrow"]),
    ]
    table = pa.Table.from_arrays(data, names=["A", "B", "C", "D"])

    source_columns = ["A", "B"]
    placeholder = "placeholder"

    new_table = fill_empty_source_column_values_with_placeholder(table, source_columns, placeholder)

    expected_data = [
        pa.array(["placeholder", "hello", "placeholder"]),
        pa.array(["hello", "placeholder", "placeholder"]),
        pa.array([1, 2, 3]),
        pa.array(["world", "", "arrow"]),
    ]
    expected_table = pa.Table.from_arrays(expected_data, names=["A", "B", "C", "D"])
    assert new_table.equals(expected_table)


def test_cast_date64_columns_to_timestamp_preserves_ms_bits() -> None:
    # Prepare timestamp[us] values with non-ms microseconds to detect precision loss
    us_values = [0, 1001, 1609459200123123, 1609459200456789, None]
    ts_us_arr = pa.array(us_values, type=pa.timestamp("us"))
    # Mimic connectorx mis-typed output by reinterpreting as date64[ms]
    date64_arr = ts_us_arr.view(pa.date64())
    tbl = pa.table({"ts_like": date64_arr})

    # Reinterpret date64 -> timestamp[us] (naive)
    out = cast_date64_columns_to_timestamp(tbl)

    # Type is timestamp[us] and values are preserved exactly (no precision loss)
    assert pa.types.is_timestamp(out["ts_like"].type)
    assert out["ts_like"].type == pa.timestamp("us")
    expected_us = ts_us_arr
    # Table columns are ChunkedArray; compare against a chunked view of the expected array
    assert out["ts_like"].equals(pa.chunked_array([expected_us]))
    # Additionally ensure ms integer 1609459200456 is present when converting back to ms
    micros = pa.compute.cast(out["ts_like"], pa.int64()).combine_chunks()
    assert micros[3].as_py() == 1609459200456789


def test_cast_date64_is_noop_when_absent_and_returns_same_object() -> None:
    # Table without date64 columns should be returned unchanged (same object)
    tbl = pa.table({"a": pa.array([1, 2, None]), "b": pa.array(["x", "y", "z"])})
    out = cast_date64_columns_to_timestamp(tbl)
    assert out is tbl


def test_cast_date64_chunked_array_support() -> None:
    # Build a chunked date64 column from two timestamp[us] chunks (simulate DB microseconds)
    vals1 = pa.array([0, 1001, 2002], type=pa.timestamp("us"))
    vals2 = pa.array([1609459200123123, None], type=pa.timestamp("us"))
    date64_chunked = pa.chunked_array([vals1.view(pa.date64()), vals2.view(pa.date64())])
    tbl = pa.table({"ts_like": date64_chunked})

    out = cast_date64_columns_to_timestamp(tbl)

    # Should be timestamp[us] chunked array equal to original timestamp chunks (no rescale)
    assert pa.types.is_timestamp(out["ts_like"].type)
    assert out["ts_like"].type == pa.timestamp("us")
    expected = pa.chunked_array([vals1, vals2])
    assert out["ts_like"].equals(expected)
