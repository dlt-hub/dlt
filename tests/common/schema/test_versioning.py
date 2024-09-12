import pytest
import yaml

from dlt.common import json
from dlt.common.schema import utils
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TStoredSchema

from tests.common.utils import load_json_case, load_yml_case


def test_content_hash() -> None:
    eth_v4: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v4")
    hash_ = utils.generate_version_hash(eth_v4)
    # change content
    eth_v4["tables"]["_dlt_loads"]["write_disposition"] = "append"
    hash2 = utils.generate_version_hash(eth_v4)
    assert hash_ != hash2
    # version and version_hash are excluded from hash computation
    eth_v4["version"] += 1
    assert utils.generate_version_hash(eth_v4) == hash2
    eth_v4["version_hash"] = "xxxx"
    assert utils.generate_version_hash(eth_v4) == hash2
    # import schema hash is also excluded
    eth_v4["imported_version_hash"] = "xxxx"
    assert utils.generate_version_hash(eth_v4) == hash2
    # changing table order does not impact the hash
    loads_table = eth_v4["tables"].pop("_dlt_loads")
    # insert at the end: _dlt_loads was first originally
    eth_v4["tables"]["_dlt_loads"] = loads_table
    assert utils.generate_version_hash(eth_v4) == hash2
    # changing column order impacts the hash
    col = loads_table["columns"].pop("inserted_at")
    # inserted_at was first, now insert at the end
    loads_table["columns"]["inserted_at"] = col
    assert utils.generate_version_hash(eth_v4) != hash2
    # can compute hash on table with no columns
    del loads_table["columns"]
    assert utils.generate_version_hash(eth_v4) != hash2
    loads_table["columns"] = {}
    assert utils.generate_version_hash(eth_v4) != hash2
    # can compute hash on schema without tables
    del eth_v4["tables"]
    assert utils.generate_version_hash(eth_v4) != hash2


def test_bump_version_no_stored_hash() -> None:
    eth_v3: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v3")
    assert "version_hash" not in eth_v3
    stored_version = eth_v3["version"]
    schema = Schema.from_dict(eth_v3)  # type: ignore[arg-type]
    assert schema.stored_version == schema.version == stored_version


def test_bump_version_changed_schema() -> None:
    eth_v4: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v4")
    stored_version = eth_v4["version"]
    eth_v4["tables"]["_dlt_loads"]["write_disposition"] = "append"
    schema = Schema.from_dict(eth_v4)  # type: ignore[arg-type]
    assert schema.stored_version == schema.version == stored_version + 1


def test_infer_column_bumps_version() -> None:
    # when schema is updated with a series of updates, version must change
    schema = Schema("event")
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    _, new_table = schema.coerce_row("event_user", None, row)
    schema.update_table(new_table)
    # schema version will be recomputed
    assert schema.version == 1
    assert schema.version_hash is not None
    version_hash = schema.version_hash

    # another table
    _, new_table = schema.coerce_row("event_bot", None, row)
    schema.update_table(new_table)
    # version is still 1 (increment of 1)
    assert schema.version == 1
    # but the hash changed
    assert schema.version_hash != version_hash

    # save
    saved_schema = schema.to_dict()
    assert saved_schema["version_hash"] == schema.version_hash
    assert saved_schema["version"] == 1


def test_preserve_version_on_load() -> None:
    eth_v10: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v10")
    version = eth_v10["version"]
    version_hash = eth_v10["version_hash"]
    schema = Schema.from_dict(eth_v10)  # type: ignore[arg-type]
    # version should not be bumped
    assert version_hash == schema._stored_version_hash
    assert version_hash == schema.version_hash
    assert version == schema.version


@pytest.mark.parametrize("remove_defaults", [True, False])
def test_version_preserve_on_reload(remove_defaults: bool) -> None:
    eth_v8: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v8")
    schema = Schema.from_dict(eth_v8)  # type: ignore[arg-type]

    to_save_dict = schema.to_dict(remove_defaults=remove_defaults)
    assert schema.stored_version == to_save_dict["version"]
    assert schema.stored_version_hash == to_save_dict["version_hash"]
    saved_schema = Schema.from_dict(to_save_dict)  # type: ignore[arg-type]
    # stored hashes must match
    assert saved_schema.stored_version == schema.stored_version
    assert saved_schema.stored_version_hash == schema.stored_version_hash
    # recomputed hashes must match
    assert saved_schema.version == schema.stored_version
    assert saved_schema.version_hash == schema.stored_version_hash

    # serialize as json
    eth_json = schema.to_pretty_json(remove_defaults=remove_defaults)
    saved_schema = Schema.from_dict(json.loads(eth_json))
    assert saved_schema.stored_version == schema.stored_version
    assert saved_schema.stored_version_hash == schema.stored_version_hash

    # serialize as yaml, for that use a schema that was stored in json
    rasa_v4: TStoredSchema = load_json_case("schemas/rasa/event.schema")
    rasa_schema = Schema.from_dict(rasa_v4)  # type: ignore[arg-type]
    rasa_yml = rasa_schema.to_pretty_yaml(remove_defaults=remove_defaults)
    saved_rasa_schema = Schema.from_dict(yaml.safe_load(rasa_yml))
    assert saved_rasa_schema.stored_version == rasa_schema.stored_version
    assert saved_rasa_schema.stored_version_hash == rasa_schema.stored_version_hash


def test_create_ancestry() -> None:
    eth_v9: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v9")
    schema = Schema.from_dict(eth_v9)  # type: ignore[arg-type]

    expected_previous_hashes = [
        "oHfYGTI2GHOxuzwVz6+yvMilXUvHYhxrxkanC2T6MAI=",
        "C5An8WClbavalXDdNSqXbdI7Swqh/mTWMcwWKCF//EE=",
        "yjMtV4Zv0IJlfR5DPMwuXxGg8BRhy7E79L26XAHWEGE=",
    ]
    hash_count = len(expected_previous_hashes)
    assert schema._stored_previous_hashes == expected_previous_hashes
    version = schema._stored_version

    # modify save and load schema 15 times and check ancestry
    for i in range(1, 15):
        # keep expected previous_hashes
        expected_previous_hashes.insert(0, schema._stored_version_hash)

        # update schema
        row = {f"float{i}": 78172.128}
        _, new_table = schema.coerce_row("event_user", None, row)
        schema.update_table(new_table)
        schema_dict = schema.to_dict()
        schema = Schema.from_stored_schema(schema_dict)

        assert schema._stored_previous_hashes == expected_previous_hashes[:10]
        assert schema._stored_version == version + i

        # we never have more than 10 previous_hashes
        assert len(schema._stored_previous_hashes) == i + hash_count if i + hash_count <= 10 else 10

    assert len(schema._stored_previous_hashes) == 10
