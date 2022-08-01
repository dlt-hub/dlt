from typing import List, Sequence
import pytest

from dlt.common import pendulum
from dlt.common.configuration import SchemaVolumeConfiguration, make_configuration
from dlt.common.exceptions import DictValidationException
from dlt.common.schema.typing import TColumnName, TSimpleRegex
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id
from dlt.common.schema import TColumnSchema, Schema, TStoredSchema, utils
from dlt.common.schema.exceptions import InvalidSchemaName, ParentTableNotFoundException, SchemaEngineNoUpgradePathException
from dlt.common.storages import SchemaStorage

from tests.utils import autouse_root_storage
from tests.common.utils import load_json_case, load_yml_case

SCHEMA_NAME = "event"
EXPECTED_FILE_NAME = f"{SCHEMA_NAME}_schema.json"


@pytest.fixture
def schema_storage() -> SchemaStorage:
    C = make_configuration(
        SchemaVolumeConfiguration,
        SchemaVolumeConfiguration,
        initial_values={
            "import_schema_path": "tests/common/cases/schemas/rasa",
            "external_schema_format": "json"
        })
    return SchemaStorage(C, makedirs=True)


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def cn_schema() -> Schema:
    return Schema("default", {
        "names": "tests.common.schema.custom_normalizers",
        "json": {
            "module": "tests.common.schema.custom_normalizers",
            "config": {
                "not_null": ["fake_id"]
            }
        }
    })


# @pytest.fixture(autouse=True)
# def auto_delete_storage() -> None:
#     delete_storage()


def test_normalize_schema_name(schema: Schema) -> None:
    assert schema.normalize_schema_name("BAN_ANA") == "banana"
    assert schema.normalize_schema_name("event-.!:value") == "eventvalue"
    assert schema.normalize_schema_name("123event-.!:value") == "s123eventvalue"
    assert schema.normalize_schema_name("") == ""
    with pytest.raises(ValueError):
        schema.normalize_schema_name(None)


def test_new_schema(schema: Schema) -> None:
    assert schema.name == "event"
    stored_schema = schema.to_dict()
    # version hash is present
    assert len(stored_schema["version_hash"]) > 0
    utils.validate_stored_schema(stored_schema)
    assert_new_schema_values(schema)


def test_new_schema_custom_normalizers(cn_schema: Schema) -> None:
    assert_new_schema_values_custom_normalizers(cn_schema)


def test_simple_regex_validator() -> None:
    # can validate only simple regexes
    assert utils.simple_regex_validator(".", "k", "v", str) is False
    assert utils.simple_regex_validator(".", "k", "v", TSimpleRegex) is True

    # validate regex
    assert utils.simple_regex_validator(".", "k", TSimpleRegex("re:^_record$"), TSimpleRegex) is True
    # invalid regex
    with pytest.raises(DictValidationException) as e:
        utils.simple_regex_validator(".", "k", "re:[[^_record$", TSimpleRegex)
    assert "[[^_record$" in str(e.value)
    # regex not marked as re:
    with pytest.raises(DictValidationException):
        utils.simple_regex_validator(".", "k", "^_record$", TSimpleRegex)
    # expected str as base type
    with pytest.raises(DictValidationException):
        utils.simple_regex_validator(".", "k", 1, TSimpleRegex)


def test_load_corrupted_schema() -> None:
    eth_v4: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v4")
    del eth_v4["tables"]["blocks"]
    with pytest.raises(ParentTableNotFoundException):
        utils.validate_stored_schema(eth_v4)


def test_column_name_validator(schema: Schema) -> None:
    assert utils.column_name_validator(schema.normalize_column_name)(".", "k", "v", str) is False
    assert utils.column_name_validator(schema.normalize_column_name)(".", "k", "v", TColumnName) is True

    assert utils.column_name_validator(schema.normalize_column_name)(".", "k", "snake_case", TColumnName) is True
    with pytest.raises(DictValidationException) as e:
        utils.column_name_validator(schema.normalize_column_name)(".", "k", "1snake_case", TColumnName)
    assert "not a valid column name" in str(e.value)
    # expected str as base type
    with pytest.raises(DictValidationException):
        utils.column_name_validator(schema.normalize_column_name)(".", "k", 1, TColumnName)


def test_invalid_schema_name() -> None:
    with pytest.raises(InvalidSchemaName) as exc:
        Schema("a_b")
    assert exc.value.name == "a_b"


@pytest.mark.parametrize("columns,hint,value", [
    (["_dlt_id", "_dlt_root_id", "_dlt_load_id", "_dlt_parent_id", "_dlt_list_idx"], "nullable", False),
    (["_dlt_id"], "unique", True),
    (["_dlt_parent_id"], "foreign_key", True),
])
def test_relational_normalizer_schema_hints(columns: Sequence[str], hint: str, value: bool, schema_storage: SchemaStorage) -> None:
    schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value


def test_new_schema_alt_name() -> None:
    schema = Schema("model")
    assert schema.name == "model"


def test_save_store_schema(schema: Schema, schema_storage: SchemaStorage) -> None:
    assert not schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    saved_file_name = schema_storage.save_schema(schema)
    # return absolute path
    assert saved_file_name == schema_storage.storage._make_path(EXPECTED_FILE_NAME)
    assert schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    schema_copy = schema_storage.load_schema("event")
    assert schema.name == schema_copy.name
    assert schema.version == schema_copy.version
    assert_new_schema_values(schema_copy)


def test_save_store_schema_custom_normalizers(cn_schema: Schema, schema_storage: SchemaStorage) -> None:
    schema_storage.save_schema(cn_schema)
    schema_copy = schema_storage.load_schema("default")
    assert_new_schema_values_custom_normalizers(schema_copy)


def test_upgrade_engine_v1_schema() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/ev1/event_schema")
    # ensure engine v1
    assert schema_dict["engine_version"] == 1
    # schema_dict will be updated to new engine version
    utils.upgrade_engine_version(schema_dict, from_engine=1, to_engine=2)
    assert schema_dict["engine_version"] == 2
    # we have 27 tables
    assert len(schema_dict["tables"]) == 27

    # upgrade schema eng 2 -> 4
    schema_dict: DictStrAny = load_json_case("schemas/ev2/event_schema")
    assert schema_dict["engine_version"] == 2
    upgraded = utils.upgrade_engine_version(schema_dict, from_engine=2, to_engine=4)
    assert upgraded["engine_version"] == 4
    utils.validate_stored_schema(upgraded)

    # upgrade 1 -> 4
    schema_dict: DictStrAny = load_json_case("schemas/ev1/event_schema")
    assert schema_dict["engine_version"] == 1
    upgraded = utils.upgrade_engine_version(schema_dict, from_engine=1, to_engine=4)
    assert upgraded["engine_version"] == 4
    utils.validate_stored_schema(upgraded)


def test_unknown_engine_upgrade() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event_schema")
    # there's no path to migrate 3 -> 2
    schema_dict["engine_version"] = 3
    with pytest.raises(SchemaEngineNoUpgradePathException):
        utils.upgrade_engine_version(schema_dict, 3, 2)


def test_preserve_column_order(schema: Schema, schema_storage: SchemaStorage) -> None:
    # python dicts are ordered from v3.6, add 50 column with random names
    update: List[TColumnSchema] = [schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)]
    schema.update_schema(utils.new_table("event_test_order", columns=update))

    def verify_items(table, update) -> None:
        assert [i[0] for i in table.items()] == list(table.keys()) == [u["name"] for u in update]
        assert [i[1] for i in table.items()] == list(table.values()) == update

    table = schema.get_table_columns("event_test_order")
    verify_items(table, update)
    # save and load
    schema_storage.save_schema(schema)
    loaded_schema = schema_storage.load_schema("event")
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update)
    # add more columns
    update2: List[TColumnSchema] = [schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)]
    loaded_schema.update_schema(utils.new_table("event_test_order", columns=update2))
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update + update2)
    # save and load
    schema_storage.save_schema(loaded_schema)
    loaded_schema = schema_storage.load_schema("event")
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update  + update2)


def test_get_schema_new_exist(schema_storage: SchemaStorage) -> None:
    with pytest.raises(FileNotFoundError):
        schema_storage.load_schema("wrongschema")


@pytest.mark.parametrize("columns,hint,value", [
    (["timestamp", "_timestamp", "_dist_key", "_dlt_id", "_dlt_root_id", "_dlt_load_id", "_dlt_parent_id", "_dlt_list_idx", "sender_id"], "nullable", False),
    (["confidence", "_sender_id"], "nullable", True),
    (["timestamp", "_timestamp"], "partition", True),
    (["_dist_key", "sender_id"], "cluster", True),
    (["_dlt_id"], "unique", True),
    (["_dlt_parent_id"], "foreign_key", True),
    (["timestamp", "_timestamp"], "sort", True),
])
def test_rasa_event_hints(columns: Sequence[str], hint: str, value: bool, schema_storage: SchemaStorage) -> None:
    schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value


def test_filter_hints_table() -> None:
    # this schema contains event_bot table with expected hints
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event_schema")
    schema = Schema.from_dict(schema_dict)
    # get all not_null columns on event
    bot_case: StrAny = load_json_case("mod_bot_case")
    rows = schema.filter_row_with_hint("event_bot", "not_null", bot_case)
    # timestamp must be first because it is first on the column list
    assert list(rows.keys()) == ["timestamp", "sender_id"]

    # add _dlt_root_id
    bot_case["_dlt_root_id"] = uniq_id()
    rows = schema.filter_row_with_hint("event_bot", "not_null", bot_case)
    assert list(rows.keys()) == ["timestamp", "sender_id", "_dlt_root_id"]

    # other hints
    rows = schema.filter_row_with_hint("event_bot", "partition", bot_case)
    assert list(rows.keys()) == ["timestamp"]
    rows = schema.filter_row_with_hint("event_bot", "cluster", bot_case)
    assert list(rows.keys()) == ["sender_id"]
    rows = schema.filter_row_with_hint("event_bot", "sort", bot_case)
    assert list(rows.keys()) == ["timestamp"]
    rows = schema.filter_row_with_hint("event_bot", "primary_key", bot_case)
    assert list(rows.keys()) == []
    bot_case["_dlt_id"] = uniq_id()
    rows = schema.filter_row_with_hint("event_bot", "primary_key", bot_case)
    assert list(rows.keys()) == ["_dlt_id"]


def test_filter_hints_no_table(schema_storage: SchemaStorage) -> None:
    # this is empty schema without any tables
    schema = schema_storage.load_schema("event")
    bot_case: StrAny = load_json_case("mod_bot_case")
    # actually the empty `event_bot` table exists (holds exclusion filters)
    rows = schema.filter_row_with_hint("event_bot", "not_null", bot_case)
    assert list(rows.keys()) == []

    # must be exactly in order of fields in row: timestamp is first
    rows = schema.filter_row_with_hint("event_action", "not_null", bot_case)
    assert list(rows.keys()) == ["timestamp", "sender_id"]

    rows = schema.filter_row_with_hint("event_action", "primary_key", bot_case)
    assert list(rows.keys()) == []

    # infer table, update schema for the empty bot table
    coerced_row, update = schema.coerce_row("event_bot", None, bot_case)
    schema.update_schema(update)
    # not empty anymore
    assert schema.get_table_columns("event_bot") is not None

    # make sure the column order is the same when inferring from newly created table
    rows = schema.filter_row_with_hint("event_bot", "not_null", coerced_row)
    assert list(rows.keys()) == ["timestamp", "sender_id"]


def test_merge_hints(schema: Schema) -> None:
    # erase hints
    schema._settings["default_hints"] = {}
    schema._compiled_hints = {}
    new_hints = {
            "not_null": ["_dlt_id", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "re:^_dlt_load_id$"],
            "foreign_key": ["re:^_dlt_parent_id$"],
            "unique": ["re:^_dlt_id$"]
        }
    schema.merge_hints(new_hints)
    assert schema._settings["default_hints"] == new_hints

    # again does not change anything (just the order may be different)
    schema.merge_hints(new_hints)
    assert len(new_hints) == len(schema._settings["default_hints"])
    for k in new_hints:
        assert set(new_hints[k]) == set(schema._settings["default_hints"][k])

    # add new stuff
    new_new_hints = {
        "not_null": ["timestamp"],
        "primary_key": ["id"]
    }
    schema.merge_hints(new_new_hints)
    expected_hints = {
            "not_null": ["_dlt_id", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "re:^_dlt_load_id$", "timestamp"],
            "foreign_key": ["re:^_dlt_parent_id$"],
            "unique": ["re:^_dlt_id$"],
            "primary_key": ["id"]
        }
    assert len(expected_hints) == len(schema._settings["default_hints"])
    for k in expected_hints:
        assert set(expected_hints[k]) == set(schema._settings["default_hints"][k])


def test_all_tables(schema: Schema, schema_storage: SchemaStorage) -> None:
    assert schema.all_tables() == []
    dlt_tables = schema.all_tables(with_dlt_tables=True)
    assert set([t["name"] for t in dlt_tables]) == set([Schema.LOADS_TABLE_NAME, Schema.VERSION_TABLE_NAME])
    # with tables
    schema = schema_storage.load_schema("event")
    assert [t["name"] for t in schema.all_tables()] == ['event_slot', 'event_user', 'event_bot']


def test_write_disposition(schema_storage: SchemaStorage) -> None:
    schema = schema_storage.load_schema("event")
    assert schema.get_write_disposition("event_slot") == "append"
    assert schema.get_write_disposition(Schema.LOADS_TABLE_NAME) == "skip"

    # child tables
    schema.get_table("event_user")["write_disposition"] = "replace"
    schema.update_schema(utils.new_table("event_user__intents", "event_user"))
    assert schema.get_table("event_user__intents").get("write_disposition") is None
    assert schema.get_write_disposition("event_user__intents") == "replace"
    schema.get_table("event_user__intents")["write_disposition"] = "append"
    assert schema.get_write_disposition("event_user__intents") == "append"

    # same but with merge
    schema.get_table("event_bot")["write_disposition"] = "merge"
    schema.update_schema(utils.new_table("event_bot__message", "event_bot"))
    assert schema.get_write_disposition("event_bot__message") == "merge"
    schema.get_table("event_bot")["write_disposition"] = "skip"
    assert schema.get_write_disposition("event_bot__message") == "skip"



# def delete_storage() -> None:
#     if not schema_storage.storage.has_folder(""):
#         SchemaStorage(SchemaVolumeConfiguration, makedirs=True)
#     else:
#         files = schema_storage.storage.list_folder_files(".")
#         for file in files:
#             schema_storage.storage.delete(file)
#         if schema_storage.storage.has_folder("copy"):
#             schema_storage.storage.delete_folder("copy", recursively=True)


def assert_new_schema_values_custom_normalizers(schema: Schema) -> None:
    # check normalizers config
    assert schema._normalizers_config["names"] == "tests.common.schema.custom_normalizers"
    assert schema._normalizers_config["json"]["module"] == "tests.common.schema.custom_normalizers"
    # check if schema was extended by json normalizer
    assert ["fake_id"] == schema.settings["default_hints"]["not_null"]
    # call normalizers
    assert schema.normalize_column_name("a") == "column_a"
    assert schema.normalize_table_name("a__b") == "A__b"
    assert schema.normalize_schema_name("1A_b") == "s1ab"
    # assumes elements are normalized
    assert schema.normalize_make_path("A", "B", "!C") == "A__B__!C"
    assert schema.normalize_break_path("A__B__!C") == ["A", "B", "!C"]
    row = list(schema.normalize_data_item(schema, {"bool": True}, "load_id"))
    assert row[0] == (("table", None), {"bool": True})


def assert_new_schema_values(schema: Schema) -> None:
    assert schema.version == 1
    assert schema.stored_version == 1
    assert schema.stored_version_hash is not None
    assert schema.version_hash is not None
    assert schema.ENGINE_VERSION == 4
    assert len(schema.settings["default_hints"]) > 0
    # check normalizers config
    assert schema._normalizers_config["names"] == "dlt.common.normalizers.names.snake_case"
    assert schema._normalizers_config["json"]["module"] == "dlt.common.normalizers.json.relational"
    # check if schema was extended by json normalizer
    assert set(["_dlt_id", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "_dlt_load_id"]).issubset(schema.settings["default_hints"]["not_null"])
    # call normalizers
    assert schema.normalize_column_name("A") == "a"
    assert schema.normalize_table_name("A__B") == "a__b"
    assert schema.normalize_schema_name("1A_b") == "s1ab"
    # assumes elements are normalized
    assert schema.normalize_make_path("A", "B", "!C") == "A__B__!C"
    assert schema.normalize_break_path("A__B__!C") == ["A", "B", "!C"]
    schema.normalize_data_item(schema, {}, "load_id")
    # check default tables
    tables = schema.tables
    assert "_dlt_version" in tables
    assert "version" in tables["_dlt_version"]["columns"]
    assert "_dlt_loads" in tables
    assert "load_id" in tables["_dlt_loads"]["columns"]
