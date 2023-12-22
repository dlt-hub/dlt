from copy import deepcopy
import os
from typing import List, Sequence, cast
import pytest

from dlt.common import pendulum
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.storages import SchemaStorageConfiguration
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.exceptions import DictValidationException
from dlt.common.normalizers.naming import snake_case, direct
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id
from dlt.common.schema import TColumnSchema, Schema, TStoredSchema, utils, TColumnHint
from dlt.common.schema.exceptions import (
    InvalidSchemaName,
    ParentTableNotFoundException,
    SchemaEngineNoUpgradePathException,
)
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    VERSION_TABLE_NAME,
    TColumnName,
    TSimpleRegex,
    COLUMN_HINTS,
)
from dlt.common.storages import SchemaStorage

from tests.utils import autouse_test_storage, preserve_environ
from tests.common.utils import load_json_case, load_yml_case, COMMON_TEST_CASES_PATH

SCHEMA_NAME = "event"
EXPECTED_FILE_NAME = f"{SCHEMA_NAME}.schema.json"


@pytest.fixture
def schema_storage() -> SchemaStorage:
    C = resolve_configuration(
        SchemaStorageConfiguration(),
        explicit_value={
            "import_schema_path": "tests/common/cases/schemas/rasa",
            "external_schema_format": "json",
        },
    )
    return SchemaStorage(C, makedirs=True)


@pytest.fixture
def schema_storage_no_import() -> SchemaStorage:
    C = resolve_configuration(SchemaStorageConfiguration())
    return SchemaStorage(C, makedirs=True)


@pytest.fixture
def schema() -> Schema:
    return Schema("event")


@pytest.fixture
def cn_schema() -> Schema:
    return Schema(
        "column_default",
        {
            "names": "tests.common.normalizers.custom_normalizers",
            "json": {
                "module": "tests.common.normalizers.custom_normalizers",
                "config": {"not_null": ["fake_id"]},
            },
        },
    )


def test_normalize_schema_name(schema: Schema) -> None:
    assert schema.naming.normalize_table_identifier("BAN_ANA") == "ban_ana"
    assert schema.naming.normalize_table_identifier("event-.!:value") == "event_value"
    assert schema.naming.normalize_table_identifier("123event-.!:value") == "_123event_value"
    with pytest.raises(ValueError):
        assert schema.naming.normalize_table_identifier("")
    with pytest.raises(ValueError):
        schema.naming.normalize_table_identifier(None)


def test_new_schema(schema: Schema) -> None:
    assert schema.name == "event"
    stored_schema = schema.to_dict()
    # version hash is present
    assert len(stored_schema["version_hash"]) > 0
    utils.validate_stored_schema(stored_schema)
    assert_new_schema_values(schema)


def test_new_schema_custom_normalizers(cn_schema: Schema) -> None:
    assert_new_schema_values_custom_normalizers(cn_schema)


def test_schema_config_normalizers(schema: Schema, schema_storage_no_import: SchemaStorage) -> None:
    # save snake case schema
    schema_storage_no_import.save_schema(schema)
    # config direct naming convention
    os.environ["SCHEMA__NAMING"] = "direct"
    # new schema has direct naming convention
    schema_direct_nc = Schema("direct_naming")
    assert schema_direct_nc._normalizers_config["names"] == "direct"
    # still after loading the config is "snake"
    schema = schema_storage_no_import.load_schema(schema.name)
    assert schema._normalizers_config["names"] == "snake_case"
    # provide capabilities context
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "snake_case"
    destination_caps.max_identifier_length = 127
    with Container().injectable_context(destination_caps):
        # caps are ignored if schema is configured
        schema_direct_nc = Schema("direct_naming")
        assert schema_direct_nc._normalizers_config["names"] == "direct"
        # but length is there
        assert schema_direct_nc.naming.max_length == 127
        # also for loaded schema
        schema = schema_storage_no_import.load_schema(schema.name)
        assert schema._normalizers_config["names"] == "snake_case"
        assert schema.naming.max_length == 127


def test_simple_regex_validator() -> None:
    # can validate only simple regexes
    assert utils.simple_regex_validator(".", "k", "v", str) is False
    assert utils.simple_regex_validator(".", "k", "v", TSimpleRegex) is True

    # validate regex
    assert (
        utils.simple_regex_validator(".", "k", TSimpleRegex("re:^_record$"), TSimpleRegex) is True
    )
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
    eth_v8: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v8")
    del eth_v8["tables"]["blocks"]
    with pytest.raises(ParentTableNotFoundException):
        utils.validate_stored_schema(eth_v8)


def test_column_name_validator(schema: Schema) -> None:
    assert utils.column_name_validator(schema.naming)(".", "k", "v", str) is False
    assert utils.column_name_validator(schema.naming)(".", "k", "v", TColumnName) is True

    assert utils.column_name_validator(schema.naming)(".", "k", "snake_case", TColumnName) is True
    # double underscores are accepted
    assert utils.column_name_validator(schema.naming)(".", "k", "snake__case", TColumnName) is True
    # triple underscores are accepted
    assert utils.column_name_validator(schema.naming)(".", "k", "snake___case", TColumnName) is True
    # quadruple underscores generate empty identifier
    with pytest.raises(DictValidationException) as e:
        utils.column_name_validator(schema.naming)(".", "k", "snake____case", TColumnName)
    assert "not a valid column name" in str(e.value)
    # this name is invalid
    with pytest.raises(DictValidationException) as e:
        utils.column_name_validator(schema.naming)(".", "k", "1snake_case", TColumnName)
    assert "not a valid column name" in str(e.value)
    # expected str as base type
    with pytest.raises(DictValidationException):
        utils.column_name_validator(schema.naming)(".", "k", 1, TColumnName)


def test_schema_name() -> None:
    # invalid char
    with pytest.raises(InvalidSchemaName) as exc:
        Schema("a!b")
    assert exc.value.name == "a!b"
    with pytest.raises(InvalidSchemaName) as exc:
        Schema("1_a")
    # too long
    with pytest.raises(InvalidSchemaName) as exc:
        Schema("a" * 65)


def test_create_schema_with_normalize_name() -> None:
    assert utils.normalize_schema_name("a!b") == "a_b"
    assert len(utils.normalize_schema_name("a" * 65)) == 64


def test_schema_descriptions_and_annotations(schema_storage: SchemaStorage):
    schema = SchemaStorage.load_schema_file(
        os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "event", extensions=("yaml",)
    )
    assert schema.tables["blocks"]["description"] == "Ethereum blocks"
    assert schema.tables["blocks"]["x-annotation"] == "this will be preserved on save"  # type: ignore[typeddict-item]
    assert (
        schema.tables["blocks"]["columns"]["_dlt_load_id"]["description"]
        == "load id coming from the extractor"
    )
    assert schema.tables["blocks"]["columns"]["_dlt_load_id"]["x-column-annotation"] == "column annotation preserved on save"  # type: ignore[typeddict-item]

    # mod and save
    schema.tables["blocks"]["description"] += "Saved"
    schema.tables["blocks"]["x-annotation"] += "Saved"  # type: ignore[typeddict-item]
    schema.tables["blocks"]["columns"]["_dlt_load_id"]["description"] += "Saved"
    schema.tables["blocks"]["columns"]["_dlt_load_id"]["x-column-annotation"] += "Saved"  # type: ignore[typeddict-item]
    schema_storage.save_schema(schema)

    loaded_schema = schema_storage.load_schema("event")
    assert loaded_schema.tables["blocks"]["description"].endswith("Saved")
    assert loaded_schema.tables["blocks"]["x-annotation"].endswith("Saved")  # type: ignore[typeddict-item]
    assert loaded_schema.tables["blocks"]["columns"]["_dlt_load_id"]["description"].endswith(
        "Saved"
    )
    assert loaded_schema.tables["blocks"]["columns"]["_dlt_load_id"]["x-column-annotation"].endswith("Saved")  # type: ignore[typeddict-item]


def test_replace_schema_content() -> None:
    schema = Schema("simple")
    eth_v5: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v5")
    eth_v5["imported_version_hash"] = "IMP_HASH"
    schema_eth = Schema.from_dict(eth_v5)  # type: ignore[arg-type]
    schema.replace_schema_content(schema_eth)
    assert schema_eth.stored_version_hash == schema.stored_version_hash
    assert schema_eth.version == schema.version
    assert schema_eth.version_hash == schema.version_hash
    assert schema_eth._imported_version_hash == schema._imported_version_hash

    # replace content of modified schema
    eth_v5 = load_yml_case("schemas/eth/ethereum_schema_v5")
    schema_eth = Schema.from_dict(eth_v5, bump_version=False)  # type: ignore[arg-type]
    assert schema_eth.version_hash != schema_eth.stored_version_hash
    # replace content does not bump version
    schema = Schema("simple")
    schema.replace_schema_content(schema_eth)
    assert schema.version_hash != schema.stored_version_hash


@pytest.mark.parametrize(
    "columns,hint,value",
    [
        (
            ["_dlt_id", "_dlt_root_id", "_dlt_load_id", "_dlt_parent_id", "_dlt_list_idx"],
            "nullable",
            False,
        ),
        (["_dlt_id"], "unique", True),
        (["_dlt_parent_id"], "foreign_key", True),
    ],
)
def test_relational_normalizer_schema_hints(
    columns: Sequence[str], hint: str, value: bool, schema_storage: SchemaStorage
) -> None:
    schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value  # type: ignore[literal-required]


def test_new_schema_alt_name() -> None:
    schema = Schema("model")
    assert schema.name == "model"


def test_save_store_schema(schema: Schema, schema_storage: SchemaStorage) -> None:
    assert not schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    saved_file_name = schema_storage.save_schema(schema)
    # return absolute path
    assert saved_file_name == schema_storage.storage.make_full_path(EXPECTED_FILE_NAME)
    assert schema_storage.storage.has_file(EXPECTED_FILE_NAME)
    schema_copy = schema_storage.load_schema("event")
    assert schema.name == schema_copy.name
    assert schema.version == schema_copy.version
    assert_new_schema_values(schema_copy)


def test_save_store_schema_custom_normalizers(
    cn_schema: Schema, schema_storage: SchemaStorage
) -> None:
    schema_storage.save_schema(cn_schema)
    schema_copy = schema_storage.load_schema(cn_schema.name)
    assert_new_schema_values_custom_normalizers(schema_copy)


def test_save_load_incomplete_column(
    schema: Schema, schema_storage_no_import: SchemaStorage
) -> None:
    # make sure that incomplete column is saved and restored without default hints
    incomplete_col = utils.new_column("I", nullable=False)
    incomplete_col["primary_key"] = True
    incomplete_col["x-special"] = "spec"  # type: ignore[typeddict-unknown-key]
    table = utils.new_table("table", columns=[incomplete_col])
    schema.update_table(table)
    schema_storage_no_import.save_schema(schema)
    schema_copy = schema_storage_no_import.load_schema("event")
    assert schema_copy.get_table("table")["columns"]["I"] == {
        "name": "I",
        "nullable": False,
        "primary_key": True,
        "x-special": "spec",
    }


def test_upgrade_engine_v1_schema() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/ev1/event.schema")
    # ensure engine v1
    assert schema_dict["engine_version"] == 1
    # schema_dict will be updated to new engine version
    utils.migrate_schema(schema_dict, from_engine=1, to_engine=2)
    assert schema_dict["engine_version"] == 2
    # we have 27 tables
    assert len(schema_dict["tables"]) == 27

    # upgrade schema eng 2 -> 4
    schema_dict = load_json_case("schemas/ev2/event.schema")
    assert schema_dict["engine_version"] == 2
    upgraded = utils.migrate_schema(schema_dict, from_engine=2, to_engine=4)
    assert upgraded["engine_version"] == 4

    # upgrade 1 -> 4
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = utils.migrate_schema(schema_dict, from_engine=1, to_engine=4)
    assert upgraded["engine_version"] == 4

    # upgrade 1 -> 6
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = utils.migrate_schema(schema_dict, from_engine=1, to_engine=6)
    assert upgraded["engine_version"] == 6

    # upgrade 1 -> 7
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = utils.migrate_schema(schema_dict, from_engine=1, to_engine=7)
    assert upgraded["engine_version"] == 7

    # upgrade 1 -> 8
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = utils.migrate_schema(schema_dict, from_engine=1, to_engine=8)
    assert upgraded["engine_version"] == 8


def test_unknown_engine_upgrade() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event.schema")
    # there's no path to migrate 3 -> 2
    schema_dict["engine_version"] = 3
    with pytest.raises(SchemaEngineNoUpgradePathException):
        utils.migrate_schema(schema_dict, 3, 2)  # type: ignore[arg-type]


def test_preserve_column_order(schema: Schema, schema_storage: SchemaStorage) -> None:
    # python dicts are ordered from v3.6, add 50 column with random names
    update: List[TColumnSchema] = [
        schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)
    ]
    schema.update_table(utils.new_table("event_test_order", columns=update))

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
    update2: List[TColumnSchema] = [
        schema._infer_column(uniq_id(), pendulum.now().timestamp()) for _ in range(50)
    ]
    loaded_schema.update_table(utils.new_table("event_test_order", columns=update2))
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update + update2)
    # save and load
    schema_storage.save_schema(loaded_schema)
    loaded_schema = schema_storage.load_schema("event")
    table = loaded_schema.get_table_columns("event_test_order")
    verify_items(table, update + update2)


def test_get_schema_new_exist(schema_storage: SchemaStorage) -> None:
    with pytest.raises(FileNotFoundError):
        schema_storage.load_schema("wrongschema")


@pytest.mark.parametrize(
    "columns,hint,value",
    [
        (
            [
                "timestamp",
                "_timestamp",
                "_dist_key",
                "_dlt_id",
                "_dlt_root_id",
                "_dlt_load_id",
                "_dlt_parent_id",
                "_dlt_list_idx",
                "sender_id",
            ],
            "nullable",
            False,
        ),
        (["confidence", "_sender_id"], "nullable", True),
        (["timestamp", "_timestamp"], "partition", True),
        (["_dist_key", "sender_id"], "cluster", True),
        (["_dlt_id"], "unique", True),
        (["_dlt_parent_id"], "foreign_key", True),
        (["timestamp", "_timestamp"], "sort", True),
    ],
)
def test_rasa_event_hints(
    columns: Sequence[str], hint: str, value: bool, schema_storage: SchemaStorage
) -> None:
    schema = schema_storage.load_schema("event")
    for name in columns:
        # infer column hints
        c = schema._infer_column(name, "x")
        assert c[hint] is value  # type: ignore[literal-required]


def test_filter_hints_table() -> None:
    # this schema contains event_bot table with expected hints
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    # get all not_null columns on event
    bot_case: DictStrAny = load_json_case("mod_bot_case")
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
    schema.update_table(update)
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
        "not_null": [
            "_dlt_id",
            "_dlt_root_id",
            "_dlt_parent_id",
            "_dlt_list_idx",
            "re:^_dlt_load_id$",
        ],
        "foreign_key": ["re:^_dlt_parent_id$"],
        "unique": ["re:^_dlt_id$"],
    }
    schema.merge_hints(new_hints)  # type: ignore[arg-type]
    assert schema._settings["default_hints"] == new_hints

    # again does not change anything (just the order may be different)
    schema.merge_hints(new_hints)  # type: ignore[arg-type]
    assert len(new_hints) == len(schema._settings["default_hints"])
    for k in new_hints:
        assert set(new_hints[k]) == set(schema._settings["default_hints"][k])  # type: ignore[index]

    # add new stuff
    new_new_hints = {"not_null": ["timestamp"], "primary_key": ["id"]}
    schema.merge_hints(new_new_hints)  # type: ignore[arg-type]
    expected_hints = {
        "not_null": [
            "_dlt_id",
            "_dlt_root_id",
            "_dlt_parent_id",
            "_dlt_list_idx",
            "re:^_dlt_load_id$",
            "timestamp",
        ],
        "foreign_key": ["re:^_dlt_parent_id$"],
        "unique": ["re:^_dlt_id$"],
        "primary_key": ["id"],
    }
    assert len(expected_hints) == len(schema._settings["default_hints"])
    for k in expected_hints:
        assert set(expected_hints[k]) == set(schema._settings["default_hints"][k])  # type: ignore[index]


def test_default_table_resource() -> None:
    """Parent tables without `resource` set default to table name"""
    eth_v5 = load_yml_case("schemas/eth/ethereum_schema_v5")
    tables = Schema.from_dict(eth_v5).tables

    assert tables["blocks"]["resource"] == "blocks"
    assert all([t.get("resource") is None for t in tables.values() if t.get("parent")])


def test_data_tables(schema: Schema, schema_storage: SchemaStorage) -> None:
    assert schema.data_tables() == []
    dlt_tables = schema.dlt_tables()
    assert set([t["name"] for t in dlt_tables]) == set([LOADS_TABLE_NAME, VERSION_TABLE_NAME])
    # with tables
    schema = schema_storage.load_schema("event")
    # some of them are incomplete
    assert set(schema.tables.keys()) == set(
        [LOADS_TABLE_NAME, VERSION_TABLE_NAME, "event_slot", "event_user", "event_bot"]
    )
    assert [t["name"] for t in schema.data_tables()] == ["event_slot"]
    assert schema.is_new_table("event_slot") is False
    assert schema.is_new_table("new_table") is True
    assert schema.is_new_table("event_user") is True
    assert len(schema.get_table_columns("event_user")) == 0
    assert len(schema.get_table_columns("event_user", include_incomplete=True)) == 0

    # add incomplete column
    schema.update_table(
        {
            "name": "event_user",
            "columns": {"name": {"name": "name", "primary_key": True, "nullable": False}},
        }
    )
    assert [t["name"] for t in schema.data_tables()] == ["event_slot"]
    assert schema.is_new_table("event_user") is True
    assert len(schema.get_table_columns("event_user")) == 0
    assert len(schema.get_table_columns("event_user", include_incomplete=True)) == 1

    # make it complete
    schema.update_table(
        {"name": "event_user", "columns": {"name": {"name": "name", "data_type": "text"}}}
    )
    assert [t["name"] for t in schema.data_tables()] == ["event_slot", "event_user"]
    assert [t["name"] for t in schema.data_tables(include_incomplete=True)] == [
        "event_slot",
        "event_user",
        "event_bot",
    ]
    assert schema.is_new_table("event_user") is False
    assert len(schema.get_table_columns("event_user")) == 1
    assert len(schema.get_table_columns("event_user", include_incomplete=True)) == 1


def test_write_disposition(schema_storage: SchemaStorage) -> None:
    schema = schema_storage.load_schema("event")
    assert utils.get_write_disposition(schema.tables, "event_slot") == "append"
    assert utils.get_write_disposition(schema.tables, LOADS_TABLE_NAME) == "skip"

    # child tables
    schema.get_table("event_user")["write_disposition"] = "replace"
    schema.update_table(utils.new_table("event_user__intents", "event_user"))
    assert schema.get_table("event_user__intents").get("write_disposition") is None
    assert utils.get_write_disposition(schema.tables, "event_user__intents") == "replace"
    schema.get_table("event_user__intents")["write_disposition"] = "append"
    assert utils.get_write_disposition(schema.tables, "event_user__intents") == "append"

    # same but with merge
    schema.get_table("event_bot")["write_disposition"] = "merge"
    schema.update_table(utils.new_table("event_bot__message", "event_bot"))
    assert utils.get_write_disposition(schema.tables, "event_bot__message") == "merge"
    schema.get_table("event_bot")["write_disposition"] = "skip"
    assert utils.get_write_disposition(schema.tables, "event_bot__message") == "skip"


def test_compare_columns() -> None:
    table = utils.new_table(
        "test_table",
        columns=[
            {"name": "col1", "data_type": "text", "nullable": True},
            {"name": "col2", "data_type": "text", "nullable": False},
            {"name": "col3", "data_type": "timestamp", "nullable": True},
            {"name": "col4", "data_type": "timestamp", "nullable": True},
        ],
    )
    table2 = utils.new_table(
        "test_table", columns=[{"name": "col1", "data_type": "text", "nullable": False}]
    )
    # columns identical with self
    for c in table["columns"].values():
        assert utils.compare_complete_columns(c, c) is True
    assert (
        utils.compare_complete_columns(table["columns"]["col3"], table["columns"]["col4"]) is False
    )
    # data type may not differ
    assert (
        utils.compare_complete_columns(table["columns"]["col1"], table["columns"]["col3"]) is False
    )
    # nullability may differ
    assert (
        utils.compare_complete_columns(table["columns"]["col1"], table2["columns"]["col1"]) is True
    )
    # any of the hints may differ
    for hint in COLUMN_HINTS:
        table["columns"]["col3"][hint] = True  # type: ignore[typeddict-unknown-key]
    # name may not differ
    assert (
        utils.compare_complete_columns(table["columns"]["col3"], table["columns"]["col4"]) is False
    )


def test_normalize_table_identifiers() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/github/issues.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    # assert column generated from "reactions/+1" and "-1", it is a valid identifier even with three underscores
    assert "reactions___1" in schema.tables["issues"]["columns"]
    issues_table = deepcopy(schema.tables["issues"])
    # this schema is already normalized so normalization is idempotent
    assert schema.tables["issues"] == schema.normalize_table_identifiers(issues_table)
    assert schema.tables["issues"] == schema.normalize_table_identifiers(
        schema.normalize_table_identifiers(issues_table)
    )


def test_normalize_table_identifiers_merge_columns() -> None:
    # create conflicting columns
    table_create = [
        {"name": "case", "data_type": "bigint", "nullable": False, "x-description": "desc"},
        {"name": "Case", "data_type": "double", "nullable": True, "primary_key": True},
    ]
    # schema normalizing to snake case will conflict on case and Case
    table = utils.new_table("blend", columns=table_create)  # type: ignore[arg-type]
    norm_table = Schema("norm").normalize_table_identifiers(table)
    # only one column
    assert len(norm_table["columns"]) == 1
    assert norm_table["columns"]["case"] == {
        "nullable": False,  # remove default, preserve non default
        "primary_key": True,
        "name": "case",
        "data_type": "double",
        "x-description": "desc",
    }


def assert_new_schema_values_custom_normalizers(schema: Schema) -> None:
    # check normalizers config
    assert schema._normalizers_config["names"] == "tests.common.normalizers.custom_normalizers"
    assert (
        schema._normalizers_config["json"]["module"]
        == "tests.common.normalizers.custom_normalizers"
    )
    # check if schema was extended by json normalizer
    assert ["fake_id"] == schema.settings["default_hints"]["not_null"]
    # call normalizers
    assert schema.naming.normalize_identifier("a") == "column_a"
    assert schema.naming.normalize_path("a__b") == "column_a__column_b"
    assert schema.naming.normalize_identifier("1A_b") == "column_1a_b"
    # assumes elements are normalized
    assert schema.naming.make_path("A", "B", "!C") == "A__B__!C"
    assert schema.naming.break_path("A__B__!C") == ["A", "B", "!C"]
    row = list(schema.normalize_data_item({"bool": True}, "load_id", "a_table"))
    assert row[0] == (("a_table", None), {"bool": True})


def assert_new_schema_values(schema: Schema) -> None:
    assert schema.version == 1
    assert schema.stored_version == 1
    assert schema.stored_version_hash is not None
    assert schema.version_hash is not None
    assert schema.ENGINE_VERSION == 8
    assert schema._stored_previous_hashes == []
    assert len(schema.settings["default_hints"]) > 0
    # check settings
    assert (
        utils.standard_type_detections() == schema.settings["detections"] == schema._type_detections
    )
    # check normalizers config
    assert schema._normalizers_config["names"] == "snake_case"
    assert schema._normalizers_config["json"]["module"] == "dlt.common.normalizers.json.relational"
    assert isinstance(schema.naming, snake_case.NamingConvention)
    # check if schema was extended by json normalizer
    assert set(
        ["_dlt_id", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "_dlt_load_id"]
    ).issubset(schema.settings["default_hints"]["not_null"])
    # call normalizers
    assert schema.naming.normalize_identifier("A") == "a"
    assert schema.naming.normalize_path("A__B") == "a__b"
    assert schema.naming.normalize_identifier("1A_b") == "_1_a_b"
    # assumes elements are normalized
    assert schema.naming.make_path("A", "B", "!C") == "A__B__!C"
    assert schema.naming.break_path("A__B__!C") == ["A", "B", "!C"]
    assert schema.naming.break_path("reactions___1") == ["reactions", "_1"]
    schema.normalize_data_item({}, "load_id", schema.name)
    # check default tables
    tables = schema.tables
    assert "_dlt_version" in tables
    assert "version" in tables["_dlt_version"]["columns"]
    assert "_dlt_loads" in tables
    assert "load_id" in tables["_dlt_loads"]["columns"]


def test_group_tables_by_resource(schema: Schema) -> None:
    schema.update_table(utils.new_table("a_events", columns=[]))
    schema.update_table(utils.new_table("b_events", columns=[]))
    schema.update_table(utils.new_table("c_products", columns=[], resource="products"))
    schema.update_table(utils.new_table("a_events__1", columns=[], parent_table_name="a_events"))
    schema.update_table(
        utils.new_table("a_events__1__2", columns=[], parent_table_name="a_events__1")
    )
    schema.update_table(utils.new_table("b_events__1", columns=[], parent_table_name="b_events"))

    # All resources without filter
    expected_tables = {
        "a_events": [
            schema.tables["a_events"],
            schema.tables["a_events__1"],
            schema.tables["a_events__1__2"],
        ],
        "b_events": [schema.tables["b_events"], schema.tables["b_events__1"]],
        "products": [schema.tables["c_products"]],
        "_dlt_version": [schema.tables["_dlt_version"]],
        "_dlt_loads": [schema.tables["_dlt_loads"]],
    }
    result = utils.group_tables_by_resource(schema.tables)
    assert result == expected_tables

    # With resource filter
    result = utils.group_tables_by_resource(
        schema.tables, pattern=utils.compile_simple_regex(TSimpleRegex("re:[a-z]_events"))
    )
    assert result == {
        "a_events": [
            schema.tables["a_events"],
            schema.tables["a_events__1"],
            schema.tables["a_events__1__2"],
        ],
        "b_events": [schema.tables["b_events"], schema.tables["b_events__1"]],
    }

    # With resources that has many top level tables
    schema.update_table(utils.new_table("mc_products", columns=[], resource="products"))
    schema.update_table(
        utils.new_table("mc_products__sub", columns=[], parent_table_name="mc_products")
    )
    result = utils.group_tables_by_resource(
        schema.tables, pattern=utils.compile_simple_regex(TSimpleRegex("products"))
    )
    # both tables with resource "products" must be here
    assert result == {
        "products": [
            {
                "columns": {},
                "name": "c_products",
                "resource": "products",
                "write_disposition": "append",
            },
            {
                "columns": {},
                "name": "mc_products",
                "resource": "products",
                "write_disposition": "append",
            },
            {"columns": {}, "name": "mc_products__sub", "parent": "mc_products"},
        ]
    }
