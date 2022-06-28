import pytest

from dlt.common import Decimal, json
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


def test_get_preferred_type(schema: Schema) -> None:
    _add_preferred_types(schema)

    assert "timestamp" in map(lambda m: m[1], schema._compiled_preferred_types)
    assert "double" in map(lambda m: m[1], schema._compiled_preferred_types)

    assert schema.get_preferred_type("timestamp") == "timestamp"
    assert schema.get_preferred_type("value") == "wei"
    assert schema.get_preferred_type("timestamp_confidence_entity") == "double"
    assert schema.get_preferred_type("_timestamp") is None


def test_map_column_preferred_type(schema: Schema) -> None:
    _add_preferred_types(schema)
    # preferred type match
    assert schema._map_value_to_column_type(1278712.0, "confidence") == "double"
    # preferred type can be coerced
    assert schema._map_value_to_column_type(1278712, "confidence") == "double"
    assert schema._map_value_to_column_type("18271", "confidence") == "double"
    # timestamp from coercable type
    assert schema._map_value_to_column_type(18271, "timestamp") == "timestamp"
    assert schema._map_value_to_column_type("18271.11", "timestamp") == "timestamp"
    assert schema._map_value_to_column_type("2022-05-10T00:54:38.237000+00:00", "timestamp") == "timestamp"

    # value should be wei
    assert schema._map_value_to_column_type(" 0xfe ", "value") == "wei"
    # number should be decimal
    assert schema._map_value_to_column_type(" -0.821 ", "number") == "decimal"


def test_map_column_type(schema: Schema) -> None:
    # default mappings
    assert schema._map_value_to_column_type("18271.11", "_column_name") == "text"
    assert schema._map_value_to_column_type(["city"], "_column_name") == "complex"
    assert schema._map_value_to_column_type(0x72, "_column_name") == "bigint"
    assert schema._map_value_to_column_type(0x72, "_column_name") == "bigint"
    assert schema._map_value_to_column_type(b"bytes str", "_column_name") == "binary"
    assert schema._map_value_to_column_type(b"bytes str", "_column_name") == "binary"


def test_map_column_type_complex(schema: Schema) -> None:
    # complex type mappings
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    # complex types must be cast to text
    assert schema._map_value_to_column_type(v_list, "cx_value") == "complex"
    assert schema._map_value_to_column_type(v_dict, "cx_value") == "complex"


def test_coerce_row(schema: Schema) -> None:
    _add_preferred_types(schema)
    timestamp_float = 78172.128
    timestamp_str = "1970-01-01T21:42:52.128000+00:00"
    # add new column with preferred
    row_1 = {"timestamp": timestamp_float, "confidence": "0.1", "value": "0xFF", "number": Decimal("128.67")}
    new_row_1, new_table = schema.coerce_row("event_user", None, row_1)
    # convert columns to list, they must correspond to the order of fields in row_1
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    assert new_columns[1]["data_type"] == "double"
    assert new_columns[2]["data_type"] == "wei"
    assert new_columns[3]["data_type"] == "decimal"
    # also rows values should be coerced (confidence)
    assert new_row_1 == {"timestamp": timestamp_str, "confidence": 0.1, "value": 255, "number": Decimal("128.67")}

    # update schema
    schema.update_schema(new_table)

    # no coercion on confidence
    row_2 = {"timestamp": timestamp_float, "confidence": 0.18721}
    new_row_2, new_table = schema.coerce_row("event_user", None, row_2)
    assert new_table is None
    assert new_row_2 == {"timestamp": timestamp_str, "confidence": 0.18721}

    # all coerced
    row_3 = {"timestamp": "78172.128", "confidence": 1}
    new_row_3, new_table = schema.coerce_row("event_user", None, row_3)
    assert new_table is None
    assert new_row_3 == {"timestamp": timestamp_str, "confidence": 1.0}

    # create variant column where variant column will have preferred
    # variant column should not be checked against preferred
    row_4 = {"timestamp": "78172.128", "confidence": "STR"}
    new_row_4, new_table = schema.coerce_row("event_user", None, row_4)
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "text"
    assert new_columns[0]["name"] == "confidence_v_text"
    assert new_row_4 == {"timestamp": timestamp_str, "confidence_v_text": "STR"}
    schema.update_schema(new_table)

    # add against variant
    new_row_4, new_table = schema.coerce_row("event_user", None, row_4)
    assert new_table is None
    assert new_row_4 == {"timestamp": timestamp_str, "confidence_v_text": "STR"}

    # another variant
    new_row_5, new_table = schema.coerce_row("event_user", None, {"confidence": False})
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "bool"
    assert new_columns[0]["name"] == "confidence_v_bool"
    assert new_row_5 == {"confidence_v_bool": False}
    schema.update_schema(new_table)

    # variant column clashes with existing column
    _, new_table = schema.coerce_row("event_user", None, {"new_colbool": False, "new_colbool_v_bigint": "not bigint"})
    schema.update_schema(new_table)
    with pytest.raises(CannotCoerceColumnException):
        schema.coerce_row("event_user", None, {"new_colbool": 123})


def test_coerce_row_iso_timestamp(schema: Schema) -> None:
    _add_preferred_types(schema)
    timestamp_str = "2022-05-10T00:17:15.300000+00:00"
    # will generate timestamp type
    row_1 = {"timestamp": timestamp_str}
    _, new_table = schema.coerce_row("event_user", None, row_1)
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    schema.update_schema(new_table)

    # will coerce float
    row_2 = {"timestamp": 78172.128}
    _, new_table = schema.coerce_row("event_user", None, row_2)
    # no new columns
    assert new_table is None

    # will generate variant
    row_3 = {"timestamp": "übermorgen"}
    _, new_table = schema.coerce_row("event_user", None, row_3)
    new_columns = list(new_table["columns"].values())
    assert new_columns[0]["name"] == "timestamp_v_text"


def test_coerce_complex_variant(schema: Schema) -> None:
    # create two columns to which complex type cannot be coerced
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    new_row, new_table = schema.coerce_row("event_user", None, row)
    assert new_row == row
    schema.update_schema(new_table)

    # add two more complex columns that should be coerced to text
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    c_row = {"c_list": v_list, "c_dict": v_dict}
    c_new_row, c_new_table = schema.coerce_row("event_user", None, c_row)
    c_new_columns = list(c_new_table["columns"].values())
    assert c_new_columns[0]["name"] == "c_list"
    assert c_new_columns[0]["data_type"] == "complex"
    assert c_new_columns[1]["name"] == "c_dict"
    assert c_new_columns[1]["data_type"] == "complex"
    assert c_new_row["c_list"] == json.dumps(v_list)
    schema.update_schema(c_new_table)

    # add same row again
    c_new_row, c_new_table = schema.coerce_row("event_user", None, c_row)
    assert c_new_table is None
    assert c_new_row["c_dict"] == json.dumps(v_dict)

    # add complex types on the same columns
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    # expect two new variant columns to be created
    c_new_row_v, c_new_table_v = schema.coerce_row("event_user", None, c_row_v)
    c_new_columns_v = list(c_new_table_v["columns"].values())
    # two new variant columns added
    assert len(c_new_columns_v) == 2
    assert c_new_columns_v[0]["name"] == "floatX_v_complex"
    assert c_new_columns_v[1]["name"] == "confidenceX_v_complex"
    assert c_new_row_v["floatX_v_complex"] == json.dumps(v_list)
    assert c_new_row_v["confidenceX_v_complex"] == json.dumps(v_dict)
    assert c_new_row_v["strX"] == json.dumps(v_dict)
    schema.update_schema(c_new_table_v)

    # add that row again
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    c_new_row_v, c_new_table_v = schema.coerce_row("event_user", None, c_row_v)
    assert c_new_table_v is None
    assert c_new_row_v["floatX_v_complex"] == json.dumps(v_list)
    assert c_new_row_v["confidenceX_v_complex"] == json.dumps(v_dict)
    assert c_new_row_v["strX"] == json.dumps(v_dict)


def test_corece_new_null_value(schema: Schema) -> None:
    row = {"timestamp": None}
    new_row, new_table = schema.coerce_row("event_user", None, row)
    assert "timestamp" not in new_row
    # columns were not created
    assert new_table is None


def test_corece_null_value_over_existing(schema: Schema) -> None:
    row = {"timestamp": 82178.1298812}
    new_row, new_table = schema.coerce_row("event_user", None, row)
    schema.update_schema(new_table)
    row = {"timestamp": None}
    new_row, _ = schema.coerce_row("event_user", None, row)
    assert "timestamp" not in new_row


def test_corece_null_value_over_not_null(schema: Schema) -> None:
    row = {"timestamp": 82178.1298812}
    _, new_table = schema.coerce_row("event_user", None, row)
    schema.update_schema(new_table)
    schema.get_table_columns("event_user")["timestamp"]["nullable"] = False
    row = {"timestamp": None}
    with pytest.raises(CannotCoerceNullException):
        schema.coerce_row("event_user", None, row)


def _add_preferred_types(schema: Schema) -> None:
    schema._settings["preferred_types"] = {}
    schema._settings["preferred_types"]["timestamp"] = "timestamp"
    # any column with confidence should be float
    schema._settings["preferred_types"]["re:confidence"] = "double"
    # value should be wei
    schema._settings["preferred_types"]["value"] = "wei"
    # number should be decimal
    schema._settings["preferred_types"]["re:^number$"] = "decimal"

    schema._compile_regexes()
