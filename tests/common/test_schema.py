from copy import deepcopy
from typing import List, Sequence
import pytest
import os

from dlt.common import json, pendulum, Decimal
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id
from dlt.common.schema import Column, Schema, StoredSchema, normalize_schema_name, utils
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException, InvalidSchemaName, SchemaEngineNoUpgradePathException
from dlt.common.storages import SchemaStorage

from tests.common.utils import load_json_case

from tests.utils import TEST_STORAGE

schema_storage = SchemaStorage(TEST_STORAGE, makedirs=True)

SCHEMA_NAME = "event"
EXPECTED_FILE_NAME = f"{SCHEMA_NAME}_schema.json"


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_storage()


def test_normalize_schema_name() -> None:
    assert normalize_schema_name("BAN_ANA") == "banana"
    assert normalize_schema_name("event-.!:value") == "eventvalue"


def test_new_schema(schema: Schema) -> None:
    assert schema.schema_name == "event"
    assert schema.schema_version == 1
    assert schema.ENGINE_VERSION == 2
    assert schema._preferred_types == {}
    assert schema._excludes == []
    assert schema._includes == []
    tables = schema.schema_tables
    assert "_version" in tables
    assert "version" in tables["_version"]
    assert "_loads" in tables
    assert "load_id" in tables["_loads"]


def test_invalid_schema_name() -> None:
    with pytest.raises(InvalidSchemaName) as exc:
        Schema("a_b")
    assert exc.value.name == "a_b"


@pytest.mark.parametrize("columns,hint,value", [
    (["_record_hash", "_root_hash", "_load_id", "_parent_hash", "_pos"], "nullable", False),
    (["_record_hash"], "unique", True),
    (["_parent_hash"], "foreign_key", True),
])
def test_new_schema_hints(columns: Sequence[str], hint: str, value: bool) -> None:
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value


def test_new_schema_alt_name() -> None:
    schema = Schema("model")
    assert schema.schema_name == "model"


def test_save_store_schema(schema: Schema) -> None:
    assert not schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    saved_file_name = schema_storage.save_store_schema(schema)
    # return absolute path
    assert saved_file_name == schema_storage.storage._make_path(EXPECTED_FILE_NAME)
    assert schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    schema_copy = schema_storage.load_store_schema("event")
    assert schema.schema_name == schema_copy.schema_name
    assert schema.schema_version == schema_copy.schema_version


def test_save_folder_schema(schema: Schema) -> None:
    # mock schema version to some random number so we know we load what we save
    schema._version = 762171

    schema_storage.storage.create_folder("copy")
    saved_file_name = schema_storage.save_folder_schema(schema, "copy")
    assert saved_file_name.endswith(os.path.join(TEST_STORAGE, "copy", SchemaStorage.FOLDER_SCHEMA_FILE))
    assert schema_storage.storage.has_file(f"copy/{SchemaStorage.FOLDER_SCHEMA_FILE}")
    schema_copy = schema_storage.load_folder_schema("copy")
    assert schema.schema_version == schema_copy.schema_version


def test_load_engine_v1_schema() -> None:
    schema_dict: StoredSchema = load_json_case("schemas/ev1/event_schema")
    # ensure engine v1
    assert schema_dict["engine_version"] == 1
    # schema_dict will be updated to new engine version
    schema = Schema.from_dict(schema_dict)
    assert schema_dict["engine_version"] == 2
    # we have 27 tables
    assert len(schema._schema_tables) == 27
    # serialized schema must be same
    assert schema_dict == schema.to_dict()


def test_unknown_engine_upgrade() -> None:
    schema_dict: StoredSchema = load_json_case("schemas/ev1/event_schema")
    # there's no path to migrate 3 -> 2
    schema_dict["engine_version"] = 3
    with pytest.raises(SchemaEngineNoUpgradePathException):
        utils.upgrade_engine_version(schema_dict, 3, 2)


def test_preserve_column_order(schema: Schema) -> None:
    # python dicts are ordered from v3.6, add 50 column with random names
    update: List[Column] = [schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)]
    schema.update_schema("event_test_order", update)

    def verify_items(table, update) -> None:
        assert [i[0] for i in table.items()] == list(table.keys()) == [u["name"] for u in update]
        assert [i[1] for i in table.items()] == list(table.values()) == update

    table = schema.get_table("event_test_order")
    verify_items(table, update)
    # save and load
    schema_storage.save_store_schema(schema)
    loaded_schema = schema_storage.load_store_schema("event")
    table = loaded_schema.get_table("event_test_order")
    verify_items(table, update)
    # add more columns
    update2: List[Column] = [schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)]
    loaded_schema.update_schema("event_test_order", update2)
    table = loaded_schema.get_table("event_test_order")
    verify_items(table, update + update2)
    # save and load
    schema_storage.save_store_schema(loaded_schema)
    loaded_schema = schema_storage.load_store_schema("event")
    table = loaded_schema.get_table("event_test_order")
    verify_items(table, update  + update2)


def test_get_schema_new_exist() -> None:
    with pytest.raises(FileNotFoundError):
        schema_storage.load_store_schema("wrongschema")

    with pytest.raises(FileNotFoundError):
        schema_storage.load_folder_schema(".")


def test_coerce_type() -> None:
    # same type coercion
    assert utils.coerce_type("double", "double", 8721.1) == 8721.1
    # anything into text
    assert utils.coerce_type("text", "bool", False) == str(False)


def test_coerce_type_float() -> None:
    # bigint into float
    assert utils.coerce_type("double", "bigint", 762162) == 762162.0
    # text into float if parsable
    assert utils.coerce_type("double", "text", " -1726.1288 ") == -1726.1288


def test_coerce_type_integer() -> None:
    # bigint/wei type
    assert utils.coerce_type("bigint", "text", " -1726 ") == -1726
    assert utils.coerce_type("wei", "text", " -1726 ") == -1726
    assert utils.coerce_type("bigint", "double", 1276.0) == 1276
    assert utils.coerce_type("wei", "double", 1276.0) == 1276
    assert utils.coerce_type("wei", "decimal", Decimal(1276.0)) == 1276
    assert utils.coerce_type("bigint", "decimal", 1276.0) == 1276
    # float into bigint raises
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "double", 912.12)
    with pytest.raises(ValueError):
        utils.coerce_type("wei", "double", 912.12)
    # decimal (non integer) also raises
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "decimal", Decimal(912.12))
    with pytest.raises(ValueError):
        utils.coerce_type("wei", "decimal", Decimal(912.12))
    # non parsable floats and ints
    with pytest.raises(ValueError):
        utils.coerce_type("bigint", "text", "f912.12")
    with pytest.raises(ValueError):
        utils.coerce_type("double", "text", "a912.12")


def test_coerce_type_decimal() -> None:
    # decimal type
    assert utils.coerce_type("decimal", "text", " -1726 ") == Decimal("-1726")
    # we keep integer if value is integer
    assert utils.coerce_type("decimal", "bigint", -1726) == -1726
    assert utils.coerce_type("decimal", "double", 1276.0) == Decimal("1276")


def test_coerce_type_from_hex_text() -> None:
    # hex text into various types
    assert utils.coerce_type("wei", "text", " 0xff") == 255
    assert utils.coerce_type("bigint", "text", " 0xff") == 255
    assert utils.coerce_type("decimal", "text", " 0xff") == Decimal(255)
    assert utils.coerce_type("double", "text", " 0xff") == 255.0


def test_coerce_type_timestamp() -> None:
    # timestamp cases
    assert utils.coerce_type("timestamp", "text", " 1580405246 ") == "2020-01-30T17:27:26+00:00"
    # the tenths of microseconds will be ignored
    assert utils.coerce_type("timestamp", "double", 1633344898.7415245) == "2021-10-04T10:54:58.741524+00:00"
    # if text is ISO string it will be coerced
    assert utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466000+00:00") == "2022-05-10T03:41:31.466000+00:00"
    # non parsable datetime
    with pytest.raises(ValueError):
        utils.coerce_type("timestamp", "text", "2022-05-10T03:41:31.466000X+00:00")


def test_coerce_type_binary() -> None:
    # from hex string
    assert utils.coerce_type("binary", "text", "0x30") == b'0'
    # from base64
    assert utils.coerce_type("binary", "text", "YmluYXJ5IHN0cmluZw==") == b'binary string'
    # int into bytes
    assert utils.coerce_type("binary", "bigint", 15) == b"\x0f"
    # can't into double
    with pytest.raises(ValueError):
        utils.coerce_type("binary", "double", 912.12)
    # can't broken base64
    with pytest.raises(ValueError):
        assert utils.coerce_type("binary", "text", "!YmluYXJ5IHN0cmluZw==")


def test_coerce_type_complex() -> None:
    # dicts and lists should be coerced into strings automatically
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    assert utils.py_type_to_sc_type(type(v_list)) == "complex"
    assert utils.py_type_to_sc_type(type(v_dict)) == "complex"
    assert utils.coerce_type("complex", "complex", v_dict) is v_dict
    assert utils.coerce_type("complex", "complex", v_list) is v_list
    assert utils.coerce_type("text", "complex", v_dict) == json.dumps(v_dict)
    assert utils.coerce_type("text", "complex", v_list) == json.dumps(v_list)


def test_get_preferred_type(schema: Schema) -> None:
    _add_preferred_types(schema)

    assert "timestamp" in map(lambda m: m[1], schema._compiled_preferred_types)
    assert "double" in map(lambda m: m[1], schema._compiled_preferred_types)

    assert schema._get_preferred_type("timestamp") == "timestamp"
    assert schema._get_preferred_type("value") == "wei"
    assert schema._get_preferred_type("timestamp_confidence_entity") == "double"
    assert schema._get_preferred_type("_timestamp") is None


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
    assert schema._map_value_to_column_type(["city"], "_column_name") == "text"
    assert schema._map_value_to_column_type(0x72, "_column_name") == "bigint"
    assert schema._map_value_to_column_type(0x72, "_column_name") == "bigint"
    assert schema._map_value_to_column_type(b"bytes str", "_column_name") == "binary"
    assert schema._map_value_to_column_type(b"bytes str", "_column_name") == "binary"


def test_map_column_type_complex(schema: Schema) -> None:
    # complex type mappings
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    # complex types must be cast to text
    assert schema._map_value_to_column_type(v_list, "cx_value") == "text"
    assert schema._map_value_to_column_type(v_dict, "cx_value") == "text"


def test_coerce_row(schema: Schema) -> None:
    _add_preferred_types(schema)
    timestamp_float = 78172.128
    timestamp_str = "1970-01-01T21:42:52.128000+00:00"
    # add new column with preferred
    row_1 = {"timestamp": timestamp_float, "confidence": "0.1", "value": "0xFF", "number": Decimal("128.67")}
    new_row_1, new_columns = schema.coerce_row("event_user", row_1)
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    assert new_columns[1]["data_type"] == "double"
    assert new_columns[2]["data_type"] == "wei"
    assert new_columns[3]["data_type"] == "decimal"
    # also rows values should be coerced (confidence)
    assert new_row_1 == {"timestamp": timestamp_str, "confidence": 0.1, "value": 255, "number": Decimal("128.67")}

    # update schema
    schema.update_schema("event_user", new_columns)

    # no coercion on confidence
    row_2 = {"timestamp": timestamp_float, "confidence": 0.18721}
    new_row_2, new_columns = schema.coerce_row("event_user", row_2)
    assert new_columns == []
    assert new_row_2 == {"timestamp": timestamp_str, "confidence": 0.18721}

    # all coerced
    row_3 = {"timestamp": "78172.128", "confidence": 1}
    new_row_3, new_columns = schema.coerce_row("event_user", row_3)
    assert new_columns == []
    assert new_row_3 == {"timestamp": timestamp_str, "confidence": 1.0}

    # create variant column where variant column will have preferred
    # variant column should not be checked against preferred
    row_4 = {"timestamp": "78172.128", "confidence": "STR"}
    new_row_4, new_columns = schema.coerce_row("event_user", row_4)
    assert new_columns[0]["data_type"] == "text"
    assert new_columns[0]["name"] == "confidence_v_text"
    assert new_row_4 == {"timestamp": timestamp_str, "confidence_v_text": "STR"}
    schema.update_schema("event_user", new_columns)

    # add against variant
    new_row_4, new_columns = schema.coerce_row("event_user", row_4)
    assert new_columns == []
    assert new_row_4 == {"timestamp": timestamp_str, "confidence_v_text": "STR"}

    # another variant
    new_row_5, new_columns = schema.coerce_row("event_user", {"confidence": False})
    assert new_columns[0]["data_type"] == "bool"
    assert new_columns[0]["name"] == "confidence_v_bool"
    assert new_row_5 == {"confidence_v_bool": False}
    schema.update_schema("event_user", new_columns)

    # variant column clashes with existing column
    _, new_columns = schema.coerce_row("event_user", {"new_colbool": False, "new_colbool_v_bigint": "not bigint"})
    schema.update_schema("event_user", new_columns)
    with pytest.raises(CannotCoerceColumnException):
        schema.coerce_row("event_user", {"new_colbool": 123})


def test_coerce_row_iso_timestamp(schema: Schema) -> None:
    _add_preferred_types(schema)
    timestamp_str = "2022-05-10T00:17:15.300000+00:00"
    # will generate timestamp type
    row_1 = {"timestamp": timestamp_str}
    _, new_columns = schema.coerce_row("event_user", row_1)
    assert new_columns[0]["data_type"] == "timestamp"
    assert new_columns[0]["name"] == "timestamp"
    schema.update_schema("event_user", new_columns)

    # will coerce float
    row_2 = {"timestamp": 78172.128}
    _, new_columns = schema.coerce_row("event_user", row_2)
    # no new columns
    assert new_columns == []

    # will generate variant
    row_3 = {"timestamp": "übermorgen"}
    _, new_columns = schema.coerce_row("event_user", row_3)
    assert new_columns[0]["name"] == "timestamp_v_text"


def test_coerce_complex_variant(schema: Schema) -> None:
    # create two columns to which complex type cannot be coerced
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    new_row, new_columns = schema.coerce_row("event_user", row)
    assert new_row == row
    schema.update_schema("event_user", new_columns)

    # add two more complex columns that should be coerced to text
    v_list = [1, 2, "3", {"complex": True}]
    v_dict = {"list": [1, 2], "str": "complex"}
    c_row = {"c_list": v_list, "c_dict": v_dict}
    c_new_row, c_new_columns = schema.coerce_row("event_user", c_row)
    assert c_new_columns[0]["name"] == "c_list"
    assert c_new_columns[0]["data_type"] == "text"
    assert c_new_columns[1]["name"] == "c_dict"
    assert c_new_columns[1]["data_type"] == "text"
    assert c_new_row["c_list"] == json.dumps(v_list)
    schema.update_schema("event_user", c_new_columns)

    # add same row again
    c_new_row, c_new_columns = schema.coerce_row("event_user", c_row)
    assert not c_new_columns
    assert c_new_row["c_dict"] == json.dumps(v_dict)

    # add complex types on the same columns
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    # expect two new variant columns to be created
    c_new_row_v, c_new_columns_v = schema.coerce_row("event_user", c_row_v)
    # two new variant columns added
    assert len(c_new_columns_v) == 2
    assert c_new_columns_v[0]["name"] == "floatX_v_text"
    assert c_new_columns_v[1]["name"] == "confidenceX_v_text"
    assert c_new_row_v["floatX_v_text"] == json.dumps(v_list)
    assert c_new_row_v["confidenceX_v_text"] == json.dumps(v_dict)
    assert c_new_row_v["strX"] == json.dumps(v_dict)
    schema.update_schema("event_user", c_new_columns_v)

    # add that row again
    c_row_v = {"floatX": v_list, "confidenceX": v_dict, "strX": v_dict}
    c_new_row_v, c_new_columns_v = schema.coerce_row("event_user", c_row_v)
    assert not c_new_columns_v
    assert c_new_row_v["floatX_v_text"] == json.dumps(v_list)
    assert c_new_row_v["confidenceX_v_text"] == json.dumps(v_dict)
    assert c_new_row_v["strX"] == json.dumps(v_dict)


def test_corece_new_null_value(schema: Schema) -> None:
    row = {"timestamp": None}
    new_row, new_columns = schema.coerce_row("event_user", row)
    assert "timestamp" not in new_row
    # columns were not created
    assert len(new_columns) == 0


def test_corece_null_value_over_existing(schema: Schema) -> None:
    row = {"timestamp": 82178.1298812}
    new_row, new_columns = schema.coerce_row("event_user", row)
    schema.update_schema("event_user", new_columns)
    row = {"timestamp": None}
    new_row, _ = schema.coerce_row("event_user", row)
    assert "timestamp" not in new_row


def test_corece_null_value_over_not_null(schema: Schema) -> None:
    row = {"timestamp": 82178.1298812}
    new_row, new_columns = schema.coerce_row("event_user", row)
    schema.update_schema("event_user", new_columns)
    schema.get_table("event_user")["timestamp"]["nullable"] = False
    row = {"timestamp": None}
    with pytest.raises(CannotCoerceNullException):
        schema.coerce_row("event_user", row)


@pytest.mark.parametrize("columns,hint,value", [
    (["timestamp", "_timestamp", "_dist_key", "_record_hash", "_root_hash", "_load_id", "_parent_hash", "_pos", "sender_id"], "nullable", False),
    (["confidence", "_sender_id"], "nullable", True),
    (["timestamp", "_timestamp"], "partition", True),
    (["_dist_key", "sender_id"], "cluster", True),
    (["_record_hash"], "unique", True),
    (["_parent_hash"], "foreign_key", True),
    (["timestamp", "_timestamp"], "sort", True),
])
def test_rasa_event_hints(columns: Sequence[str], hint: str, value: bool) -> None:
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value


def test_row_field_filter(schema: Schema) -> None:
    schema = _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    filtered_case = schema.filter_row("event_bot", deepcopy(bot_case), "__")
    # metadata, is_flagged and data should be eliminated
    ref_case = deepcopy(bot_case)
    del ref_case["metadata"]
    del ref_case["is_flagged"]
    del ref_case["data"]
    del ref_case["data__custom__goes"]
    del ref_case["custom_data"]
    assert ref_case == filtered_case
    # one of the props was included form the excluded (due to ^event_bot__data__custom$)
    assert ref_case["data__custom"] == "remains"


def test_whole_row_filter(schema: Schema) -> None:
    schema = _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    # the whole row should be eliminated if the exclude matches all the rows
    filtered_case = schema.filter_row("event_bot__metadata", deepcopy(bot_case)["metadata"], "__")
    assert filtered_case == {}
    # also child rows will be excluded
    filtered_case = schema.filter_row("event_bot__metadata__user", deepcopy(bot_case)["metadata"], "__")
    assert filtered_case == {}


def test_whole_row_filter_with_exception(schema: Schema) -> None:
    schema = _add_excludes(schema)
    bot_case: StrAny = load_json_case("mod_bot_case")
    # whole row will be eliminated
    filtered_case = schema.filter_row("event_bot__custom_data", deepcopy(bot_case)["custom_data"], "__")
    # mind that path event_bot__custom_data__included_object was also eliminated
    assert filtered_case == {}
    # this child of the row has exception (^event_bot__custom_data__included_object__ - the __ at the end select all childern but not the parent)
    filtered_case = schema.filter_row("event_bot__custom_data__included_object", deepcopy(bot_case)["custom_data"]["included_object"], "__")
    assert filtered_case == bot_case["custom_data"]["included_object"]
    filtered_case = schema.filter_row("event_bot__custom_data__excluded_path", deepcopy(bot_case)["custom_data"]["excluded_path"], "__")
    assert filtered_case == {}


def test_filter_hints_table() -> None:
    schema_dict: StoredSchema = load_json_case("schemas/ev1/event_schema")
    schema = Schema.from_dict(schema_dict)
    # get all not_null columns on event
    bot_case: StrAny = load_json_case("mod_bot_case")
    rows = schema.filter_hints_in_row("event_bot", "not_null", bot_case)
    # timestamp must be first because it is first on the column list
    assert list(rows.keys()) == ["timestamp", "sender_id"]

    # add _root_hash
    bot_case["_root_hash"] = uniq_id()
    rows = schema.filter_hints_in_row("event_bot", "not_null", bot_case)
    assert list(rows.keys()) == ["timestamp", "sender_id", "_root_hash"]

    # other hints
    rows = schema.filter_hints_in_row("event_bot", "partition", bot_case)
    assert list(rows.keys()) == ["timestamp"]
    rows = schema.filter_hints_in_row("event_bot", "cluster", bot_case)
    assert list(rows.keys()) == ["sender_id"]
    rows = schema.filter_hints_in_row("event_bot", "sort", bot_case)
    assert list(rows.keys()) == ["timestamp"]
    rows = schema.filter_hints_in_row("event_bot", "primary_key", bot_case)
    assert list(rows.keys()) == []
    bot_case["_record_hash"] = uniq_id()
    rows = schema.filter_hints_in_row("event_bot", "primary_key", bot_case)
    assert list(rows.keys()) == ["_record_hash"]


def test_filter_hints_no_table() -> None:
    schema_storage = SchemaStorage("tests/common/cases/schemas/rasa")
    schema = schema_storage.load_store_schema("event")
    bot_case: StrAny = load_json_case("mod_bot_case")
    rows = schema.filter_hints_in_row("event_bot", "not_null", bot_case)
    # must be exactly in order of fields in row: timestamp is first
    assert list(rows.keys()) == ["timestamp", "sender_id"]

    rows = schema.filter_hints_in_row("event_bot", "primary_key", bot_case)
    assert list(rows.keys()) == []

    # infer table, update schema
    coerced_row, update = schema.coerce_row("event_bot", bot_case)
    schema.update_schema("event_bot", update)
    assert schema.get_table("event_bot") is not None

    # make sure the column order is the same when inferring from newly created table
    rows = schema.filter_hints_in_row("event_bot", "not_null", coerced_row)
    assert list(rows.keys()) == ["timestamp", "sender_id"]


def _add_excludes(schema: Schema) -> None:
    schema._excludes = ["^event_bot__metadata", "^event_bot__is_flagged$", "^event_bot__data", "^event_bot__custom_data"]
    schema._includes = ["^event_bot__data__custom$", "^event_bot__custom_data__included_object__"]
    schema_storage.save_store_schema(schema)
    return schema_storage.load_store_schema("event")


def _add_preferred_types(schema: Schema) -> None:
    schema._preferred_types["^timestamp$"] = "timestamp"
    # any column with confidence should be float
    schema._preferred_types["confidence"] = "double"
    # value should be wei
    schema._preferred_types["^value$"] = "wei"
    # number should be decimal
    schema._preferred_types["^number$"] = "decimal"

    schema._compile_regexes()


def delete_storage() -> None:
    files = schema_storage.storage.list_folder_files(".")
    for file in files:
        schema_storage.storage.delete(file)
    if schema_storage.storage.has_folder("copy"):
        schema_storage.storage.delete_folder("copy", recursively=True)