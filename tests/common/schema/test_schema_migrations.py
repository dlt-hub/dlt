import pytest

from dlt.common.schema.exceptions import SchemaEngineNoUpgradePathException
from dlt.common.schema.migrations import migrate_schema
from dlt.common.schema.typing import TStoredSchema
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
    upgraded = migrate_schema(schema_dict, from_engine=1, to_engine=9)
    assert upgraded["engine_version"] == 9


def test_complex_type_migration() -> None:
    schema_dict: DictStrAny = load_json_case("schemas/rasa/event.schema")
    print(schema_dict)
    upgraded = migrate_schema(schema_dict, from_engine=schema_dict["engine_version"], to_engine=10)
    assert upgraded["settings"]["preferred_types"]["re:^_test_slot$"] == "json"
    assert upgraded["tables"]["event_slot"]["columns"]["value"]["data_type"] == "json"


# def test_complex_type_new_table_migration() -> None:


def test_unknown_engine_upgrade() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/ev1/event.schema")
    # there's no path to migrate 3 -> 2
    schema_dict["engine_version"] = 3
    with pytest.raises(SchemaEngineNoUpgradePathException):
        migrate_schema(schema_dict, 3, 2)  # type: ignore[arg-type]
