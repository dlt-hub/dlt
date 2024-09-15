import os
import pytest

from dlt.common.schema.exceptions import SchemaEngineNoUpgradePathException
from dlt.common.schema.migrations import migrate_schema
from dlt.common.schema.normalizers import DEFAULT_NAMING_MODULE
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TStoredSchema
from dlt.common.schema.utils import new_table
from dlt.common.typing import DictStrAny

from tests.common.utils import load_json_case


def test_upgrade_engine_v1_schema() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/ev1/event.schema")
    # ensure engine v1
    assert schema_dict["engine_version"] == 1
    # schema_dict will be updated to new engine version
    migrate_schema(schema_dict, from_engine=1, to_engine=2)
    assert schema_dict["engine_version"] == 2
    # we have 27 tables
    assert len(schema_dict["tables"]) == 27

    # upgrade schema eng 2 -> 4
    schema_dict = load_json_case("schemas/ev2/event.schema")
    assert schema_dict["engine_version"] == 2
    upgraded = migrate_schema(schema_dict, from_engine=2, to_engine=4)
    assert upgraded["engine_version"] == 4

    # upgrade 1 -> 4
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=4)
    assert upgraded["engine_version"] == 4

    # upgrade 1 -> 6
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=6)
    assert upgraded["engine_version"] == 6

    # upgrade 1 -> 7
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=7)
    assert upgraded["engine_version"] == 7

    # upgrade 1 -> 8
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=8)
    assert upgraded["engine_version"] == 8

    # upgrade 1 -> 9
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=9)
    assert upgraded["engine_version"] == 9

    # upgrade 1 -> 10
    schema_dict = load_json_case("schemas/ev1/event.schema")
    assert schema_dict["engine_version"] == 1
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=10)
    assert upgraded["engine_version"] == 10


def test_complex_type_migration() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/rasa/event.schema")
    upgraded = migrate_schema(schema_dict, from_engine=schema_dict["engine_version"], to_engine=10)
    assert upgraded["settings"]["preferred_types"]["re:^_test_slot$"] == "json"  # type: ignore
    assert upgraded["tables"]["event_slot"]["columns"]["value"]["data_type"] == "json"


def test_complex_type_new_table_migration() -> None:
    # table without columns is passing through
    table = new_table("new_table")
    assert table["columns"] == {}
    table = new_table("new_table", columns=[])
    assert table["columns"] == {}

    # converts complex, keeps json
    table = new_table(
        "new_table",
        columns=[
            {"name": "old", "data_type": "complex"},  # type: ignore
            {"name": "new", "data_type": "json"},
            {"name": "incomplete", "primary_key": True},
        ],
    )
    assert table["columns"]["old"]["data_type"] == "json"
    assert table["columns"]["new"]["data_type"] == "json"


def test_keeps_old_name_in_variant_column() -> None:
    schema = Schema("dx")
    # for this test use case sensitive naming convention
    os.environ["SCHEMA__NAMING"] = "direct"
    schema.update_normalizers()
    # create two columns to which json type cannot be coerced
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    _, event_user = schema.coerce_row("event_user", None, row)
    schema.update_table(event_user)

    # mock a variant column
    event_user_partial = new_table(
        "event_user",
        columns=[
            {"name": "floatX▶v_complex", "data_type": "json", "variant": True},
            {"name": "confidenceX▶v_complex", "data_type": "json", "variant": False},
        ],
    )
    schema.update_table(event_user_partial, normalize_identifiers=False)

    # add json types on the same columns
    v_list = [1, 2, "3", {"json": True}]
    v_dict = {"list": [1, 2], "str": "json"}
    c_row_v = {"floatX": v_list, "confidenceX": v_dict}
    # expect two new variant columns to be created
    c_new_row_v, c_new_table_v = schema.coerce_row("event_user", None, c_row_v)
    c_new_columns_v = list(c_new_table_v["columns"].values())
    print(c_new_row_v)
    print(c_new_table_v)
    # floatX▶v_complex is kept (was marked with variant)
    # confidenceX▶v_json is added (confidenceX▶v_complex not marked as variant)
    assert len(c_new_columns_v) == 1
    assert c_new_columns_v[0]["name"] == "confidenceX▶v_json"
    assert c_new_columns_v[0]["variant"] is True
    # c_row_v coerced to variants
    assert c_new_row_v["floatX▶v_complex"] == v_list
    assert c_new_row_v["confidenceX▶v_json"] == v_dict


def test_row_and_parent_key_migration() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/ev1/event.schema")
    event = schema_dict["tables"]["event"]
    # set unique in _dlt_id to true
    event["_dlt_id"]["unique"] = True

    upgraded = migrate_schema(schema_dict, from_engine=schema_dict["engine_version"], to_engine=10)
    event_user__parse_data__intent_ranking = upgraded["tables"][
        "event_user__parse_data__intent_ranking"
    ]
    _dlt_id = event_user__parse_data__intent_ranking["columns"]["_dlt_id"]
    # unique id was false so row_key is the same
    assert _dlt_id["row_key"] is False
    assert "foreign_key" not in _dlt_id
    assert "parent_key" not in _dlt_id

    # parent_id modified
    _dlt_parent_id = event_user__parse_data__intent_ranking["columns"]["_dlt_parent_id"]
    assert _dlt_parent_id["parent_key"] is True
    assert "foreign_key" not in _dlt_parent_id

    # we set unique to True above so we expect row_key to be set
    event = upgraded["tables"]["event"]
    _dlt_id = event["columns"]["_dlt_id"]
    # unique id was false so row_key is the same
    assert _dlt_id["row_key"] is True


def test_preferred_hints_migration() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/rasa/event.schema")
    upgraded = migrate_schema(schema_dict, from_engine=schema_dict["engine_version"], to_engine=10)
    # foreign key hints must be dropped
    default_hints = upgraded["settings"]["default_hints"]
    assert "foreign_key" not in default_hints
    # unique still there
    assert default_hints["unique"] == ["re:^_dlt_id$"]
    # row && parent key
    assert default_hints["row_key"] == ["_dlt_id"]
    assert default_hints["parent_key"] == ["_dlt_parent_id"]


def test_row_and_parent_key_migration_upper_case() -> None:
    os.environ["SCHEMA__NAMING"] = "tests.common.cases.normalizers.sql_upper"
    os.environ["SCHEMA__ALLOW_IDENTIFIER_CHANGE_ON_TABLE_WITH_DATA"] = "TRUE"

    schema_dict: DictStrAny = load_json_case("schemas/ev1/event.schema")
    upgraded_v9 = migrate_schema(
        schema_dict, from_engine=schema_dict["engine_version"], to_engine=9
    )
    assert upgraded_v9["normalizers"]["names"] == DEFAULT_NAMING_MODULE
    # use in schema, normalize and get dict back
    # NOTE: this may stop working at some point. we use v9 schema without validation
    schema = Schema.from_stored_schema(upgraded_v9)
    schema.update_normalizers()
    upgraded_v9 = schema.to_dict(bump_version=False)

    # set unique in _dlt_id to true
    event = upgraded_v9["tables"]["EVENT"]
    event["columns"]["_DLT_ID"]["unique"] = True

    upgraded = migrate_schema(upgraded_v9, from_engine=9, to_engine=10)  # type: ignore

    event_user__parse_data__intent_ranking = upgraded["tables"][
        "EVENT_USER__PARSE_DATA__INTENT_RANKING"
    ]
    _dlt_id = event_user__parse_data__intent_ranking["columns"]["_DLT_ID"]
    # unique id was false so row_key is the same
    assert _dlt_id["row_key"] is False
    assert "foreign_key" not in _dlt_id
    assert "parent_key" not in _dlt_id

    # parent_id modified
    _dlt_parent_id = event_user__parse_data__intent_ranking["columns"]["_DLT_PARENT_ID"]
    assert _dlt_parent_id["parent_key"] is True
    assert "foreign_key" not in _dlt_parent_id

    # we set unique to True above so we expect row_key to be set
    event = upgraded["tables"]["EVENT"]
    _dlt_id = event["columns"]["_DLT_ID"]
    # unique id was false so row_key is the same
    assert _dlt_id["row_key"] is True

    # verify hints migration
    default_hints = upgraded["settings"]["default_hints"]
    assert "foreign_key" not in default_hints
    # unique still there
    assert default_hints["unique"] == ["_DLT_ID"]
    # row && parent key
    assert default_hints["row_key"] == ["_DLT_ID"]
    assert default_hints["parent_key"] == ["_DLT_PARENT_ID"]


def test_unknown_engine_upgrade() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event.schema")
    # there's no path to migrate 3 -> 2
    schema_dict["engine_version"] = 3
    with pytest.raises(SchemaEngineNoUpgradePathException):
        migrate_schema(schema_dict, 3, 2)  # type: ignore[arg-type]
