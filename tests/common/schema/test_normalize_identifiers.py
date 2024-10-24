from copy import deepcopy
import os
from typing import Callable
import pytest

from dlt.common import json
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.common.storages import SchemaStorageConfiguration
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.normalizers.naming import snake_case, direct
from dlt.common.schema import TColumnSchema, Schema, TStoredSchema, utils, TTableSchema
from dlt.common.schema.exceptions import TableIdentifiersFrozen
from dlt.common.schema.typing import SIMPLE_REGEX_PREFIX
from dlt.common.storages import SchemaStorage

from tests.common.cases.normalizers import sql_upper
from tests.common.utils import load_json_case, load_yml_case


@pytest.fixture
def schema_storage_no_import() -> SchemaStorage:
    C = resolve_configuration(SchemaStorageConfiguration())
    return SchemaStorage(C, makedirs=True)


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


def test_save_store_schema_custom_normalizers(
    cn_schema: Schema, schema_storage: SchemaStorage
) -> None:
    schema_storage.save_schema(cn_schema)
    schema_copy = schema_storage.load_schema(cn_schema.name)
    assert_new_schema_values_custom_normalizers(schema_copy)


def test_new_schema_custom_normalizers(cn_schema: Schema) -> None:
    assert_new_schema_values_custom_normalizers(cn_schema)


def test_save_load_incomplete_column(
    schema: Schema, schema_storage_no_import: SchemaStorage
) -> None:
    # make sure that incomplete column is saved and restored without default hints
    incomplete_col = utils.new_column("I", nullable=False)
    incomplete_col["primary_key"] = True
    incomplete_col["x-special"] = "spec"  # type: ignore[typeddict-unknown-key]
    table = utils.new_table("table", columns=[incomplete_col])
    schema.update_table(table, normalize_identifiers=False)
    schema_storage_no_import.save_schema(schema)
    schema_copy = schema_storage_no_import.load_schema("event")
    assert schema_copy.get_table("table")["columns"]["I"] == {
        "name": "I",
        "nullable": False,
        "primary_key": True,
        "x-special": "spec",
    }


def test_schema_config_normalizers(schema: Schema, schema_storage_no_import: SchemaStorage) -> None:
    # save snake case schema
    assert schema._normalizers_config["names"] == "snake_case"
    schema_storage_no_import.save_schema(schema)
    # config direct naming convention
    os.environ["SCHEMA__NAMING"] = "direct"
    # new schema has direct naming convention
    schema_direct_nc = Schema("direct_naming")
    schema_storage_no_import.save_schema(schema_direct_nc)
    assert schema_direct_nc._normalizers_config["names"] == "direct"
    # still after loading the config is "snake"
    schema = schema_storage_no_import.load_schema(schema.name)
    assert schema._normalizers_config["names"] == "snake_case"
    # provide capabilities context
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "sql_cs_v1"
    destination_caps.max_identifier_length = 127
    with Container().injectable_context(destination_caps):
        # caps are ignored if schema is configured
        schema_direct_nc = Schema("direct_naming")
        assert schema_direct_nc._normalizers_config["names"] == "direct"
        # but length is there
        assert schema_direct_nc.naming.max_length == 127
        # when loading schema configuration is ignored
        schema = schema_storage_no_import.load_schema(schema.name)
        assert schema._normalizers_config["names"] == "snake_case"
        assert schema.naming.max_length == 127
        # but if we ask to update normalizers config schema is applied
        schema.update_normalizers()
        assert schema._normalizers_config["names"] == "direct"

        # load schema_direct_nc (direct)
        schema_direct_nc = schema_storage_no_import.load_schema(schema_direct_nc.name)
        assert schema_direct_nc._normalizers_config["names"] == "direct"

        # drop config
        del os.environ["SCHEMA__NAMING"]
        schema_direct_nc = schema_storage_no_import.load_schema(schema_direct_nc.name)
        assert schema_direct_nc._normalizers_config["names"] == "direct"


def test_schema_normalizers_no_config(
    schema: Schema, schema_storage_no_import: SchemaStorage
) -> None:
    # convert schema to direct and save
    os.environ["SCHEMA__NAMING"] = "direct"
    schema.update_normalizers()
    assert schema._normalizers_config["names"] == "direct"
    schema_storage_no_import.save_schema(schema)
    # make sure we drop the config correctly
    del os.environ["SCHEMA__NAMING"]
    schema_test = Schema("test")
    assert schema_test.naming.name() == "snake_case"
    # use capabilities without default naming convention
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    assert destination_caps.naming_convention is None
    destination_caps.max_identifier_length = 66
    with Container().injectable_context(destination_caps):
        schema_in_caps = Schema("schema_in_caps")
        assert schema_in_caps._normalizers_config["names"] == "snake_case"
        assert schema_in_caps.naming.name() == "snake_case"
        assert schema_in_caps.naming.max_length == 66
        schema_in_caps.update_normalizers()
        assert schema_in_caps.naming.name() == "snake_case"
        # old schema preserves convention when loaded
        schema = schema_storage_no_import.load_schema(schema.name)
        assert schema._normalizers_config["names"] == "direct"
        # update normalizer no effect
        schema.update_normalizers()
        assert schema._normalizers_config["names"] == "direct"
        assert schema.naming.max_length == 66

    # use caps with default naming convention
    destination_caps = DestinationCapabilitiesContext.generic_capabilities()
    destination_caps.naming_convention = "sql_cs_v1"
    destination_caps.max_identifier_length = 127
    with Container().injectable_context(destination_caps):
        schema_in_caps = Schema("schema_in_caps")
        # new schema gets convention from caps
        assert schema_in_caps._normalizers_config["names"] == "sql_cs_v1"
        # old schema preserves convention when loaded
        schema = schema_storage_no_import.load_schema(schema.name)
        assert schema._normalizers_config["names"] == "direct"
        # update changes to caps schema
        schema.update_normalizers()
        assert schema._normalizers_config["names"] == "sql_cs_v1"
        assert schema.naming.max_length == 127


@pytest.mark.parametrize("section", ("SOURCES__SCHEMA__NAMING", "SOURCES__THIS__SCHEMA__NAMING"))
def test_config_with_section(section: str) -> None:
    os.environ["SOURCES__OTHER__SCHEMA__NAMING"] = "direct"
    os.environ[section] = "sql_cs_v1"
    this_schema = Schema("this")
    that_schema = Schema("that")
    assert this_schema.naming.name() == "sql_cs_v1"
    expected_that_schema = (
        "snake_case" if section == "SOURCES__THIS__SCHEMA__NAMING" else "sql_cs_v1"
    )
    assert that_schema.naming.name() == expected_that_schema

    # test update normalizers
    os.environ[section] = "direct"
    expected_that_schema = "snake_case" if section == "SOURCES__THIS__SCHEMA__NAMING" else "direct"
    this_schema.update_normalizers()
    assert this_schema.naming.name() == "direct"
    that_schema.update_normalizers()
    assert that_schema.naming.name() == expected_that_schema


def test_normalize_table_identifiers() -> None:
    # load with snake case
    schema_dict: TStoredSchema = load_json_case("schemas/github/issues.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    issues_table = schema.tables["issues"]
    issues_table_str = json.dumps(issues_table)
    # normalize table to upper
    issues_table_norm = utils.normalize_table_identifiers(
        issues_table, sql_upper.NamingConvention()
    )
    # nothing got changes in issues table
    assert issues_table_str == json.dumps(issues_table)
    # check normalization
    assert issues_table_norm["name"] == "ISSUES"
    assert "REACTIONS___1" in issues_table_norm["columns"]
    # subsequent normalization does not change dict
    assert issues_table_norm == utils.normalize_table_identifiers(
        issues_table_norm, sql_upper.NamingConvention()
    )


def test_normalize_table_identifiers_idempotent() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/github/issues.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    # assert column generated from "reactions/+1" and "-1", it is a valid identifier even with three underscores
    assert "reactions___1" in schema.tables["issues"]["columns"]
    issues_table = schema.tables["issues"]
    # this schema is already normalized so normalization is idempotent
    assert schema.tables["issues"] == utils.normalize_table_identifiers(issues_table, schema.naming)
    assert schema.tables["issues"] == utils.normalize_table_identifiers(
        utils.normalize_table_identifiers(issues_table, schema.naming), schema.naming
    )


def test_normalize_table_identifiers_merge_columns() -> None:
    # create conflicting columns
    table_create = [
        {"name": "case", "data_type": "bigint", "nullable": False, "x-description": "desc"},
        {"name": "Case", "data_type": "double", "nullable": True, "primary_key": True},
    ]
    # schema normalizing to snake case will conflict on case and Case
    table = utils.new_table("blend", columns=table_create)  # type: ignore[arg-type]
    table_str = json.dumps(table)
    norm_table = utils.normalize_table_identifiers(table, Schema("norm").naming)
    # nothing got changed in original table
    assert table_str == json.dumps(table)
    # only one column
    assert len(norm_table["columns"]) == 1
    assert norm_table["columns"]["case"] == {
        "nullable": False,  # remove default, preserve non default
        "primary_key": True,
        "name": "case",
        "data_type": "double",
        "x-description": "desc",
    }


def test_normalize_table_identifiers_table_reference() -> None:
    table: TTableSchema = {
        "name": "playlist_track",
        "columns": {
            "TRACK ID": {"name": "id", "data_type": "bigint", "nullable": False},
            "Playlist ID": {"name": "table_id", "data_type": "bigint", "nullable": False},
            "Position": {"name": "position", "data_type": "bigint", "nullable": False},
        },
        "references": [
            {
                "referenced_table": "PLAYLIST",
                "columns": ["Playlist ID", "Position"],
                "referenced_columns": ["ID", "Position"],
            },
            {
                "referenced_table": "Track",
                "columns": ["TRACK ID"],
                "referenced_columns": ["id"],
            },
        ],
    }

    norm_table = utils.normalize_table_identifiers(table, Schema("norm").naming)

    assert norm_table["references"][0]["referenced_table"] == "playlist"
    assert norm_table["references"][0]["columns"] == ["playlist_id", "position"]
    assert norm_table["references"][0]["referenced_columns"] == ["id", "position"]

    assert norm_table["references"][1]["referenced_table"] == "track"
    assert norm_table["references"][1]["columns"] == ["track_id"]
    assert norm_table["references"][1]["referenced_columns"] == ["id"]


def test_update_normalizers() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/github/issues.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    # drop seen data
    del schema.tables["issues"]["x-normalizer"]
    del schema.tables["issues__labels"]["x-normalizer"]
    del schema.tables["issues__assignees"]["x-normalizer"]
    # save default hints in original form
    default_hints = schema._settings["default_hints"]

    os.environ["SCHEMA__NAMING"] = "tests.common.cases.normalizers.sql_upper"
    schema.update_normalizers()
    assert isinstance(schema.naming, sql_upper.NamingConvention)
    # print(schema.to_pretty_yaml())
    assert_schema_identifiers_case(schema, str.upper)

    # resource must be old name
    assert schema.tables["ISSUES"]["resource"] == "issues"

    # make sure normalizer config is replaced
    assert schema._normalizers_config["names"] == "tests.common.cases.normalizers.sql_upper"
    assert "allow_identifier_change_on_table_with_data" not in schema._normalizers_config

    # regexes are uppercased
    new_default_hints = schema._settings["default_hints"]
    for hint, regexes in default_hints.items():
        # same number of hints
        assert len(regexes) == len(new_default_hints[hint])
        # but all upper cased
        assert set(n.upper() for n in regexes) == set(new_default_hints[hint])


def test_normalize_default_hints(schema_storage_no_import: SchemaStorage) -> None:
    # use destination caps to force naming convention
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.configuration.container import Container

    eth_V9 = load_yml_case("schemas/eth/ethereum_schema_v9")
    orig_schema = Schema.from_dict(eth_V9)
    # save schema
    schema_storage_no_import.save_schema(orig_schema)

    with Container().injectable_context(
        DestinationCapabilitiesContext.generic_capabilities(naming_convention=sql_upper)
    ) as caps:
        assert caps.naming_convention is sql_upper
        # creating a schema from dict keeps original normalizers
        schema = Schema.from_dict(eth_V9)
        assert_schema_identifiers_case(schema, str.lower)
        assert schema._normalizers_config["names"].endswith("snake_case")

        # loading from storage keeps storage normalizers
        storage_schema = schema_storage_no_import.load_schema("ethereum")
        assert_schema_identifiers_case(storage_schema, str.lower)
        assert storage_schema._normalizers_config["names"].endswith("snake_case")

        # new schema instance is created using caps/config
        new_schema = Schema("new")
        assert_schema_identifiers_case(new_schema, str.upper)
        assert (
            new_schema._normalizers_config["names"]
            == "tests.common.cases.normalizers.sql_upper.NamingConvention"
        )

        # attempt to update normalizers blocked by tables with data
        with pytest.raises(TableIdentifiersFrozen):
            schema.update_normalizers()
        # also cloning with update normalizers
        with pytest.raises(TableIdentifiersFrozen):
            schema.clone(update_normalizers=True)

        # remove processing hints and normalize
        norm_cloned = schema.clone(update_normalizers=True, remove_processing_hints=True)
        assert_schema_identifiers_case(norm_cloned, str.upper)
        assert (
            norm_cloned._normalizers_config["names"]
            == "tests.common.cases.normalizers.sql_upper.NamingConvention"
        )

        norm_schema = Schema.from_dict(
            deepcopy(eth_V9), remove_processing_hints=True, bump_version=False
        )
        norm_schema.update_normalizers()
        assert_schema_identifiers_case(norm_schema, str.upper)
        assert (
            norm_schema._normalizers_config["names"]
            == "tests.common.cases.normalizers.sql_upper.NamingConvention"
        )

        # both ways of obtaining schemas (cloning, cleaning dict) must generate identical schemas
        assert norm_cloned.to_pretty_json() == norm_schema.to_pretty_json()

        # save to storage
        schema_storage_no_import.save_schema(norm_cloned)

    # load schema out of caps
    storage_schema = schema_storage_no_import.load_schema("ethereum")
    assert_schema_identifiers_case(storage_schema, str.upper)
    # the instance got converted into
    assert storage_schema._normalizers_config["names"].endswith("sql_upper.NamingConvention")
    assert storage_schema.stored_version_hash == storage_schema.version_hash
    # cloned when bumped must have same version hash
    norm_cloned._bump_version()
    assert storage_schema.stored_version_hash == norm_cloned.stored_version_hash


def test_raise_on_change_identifier_table_with_data() -> None:
    schema_dict: TStoredSchema = load_json_case("schemas/github/issues.schema")
    schema = Schema.from_dict(schema_dict)  # type: ignore[arg-type]
    # mark issues table to seen data and change naming to sql upper
    issues_table = schema.tables["issues"]
    issues_table["x-normalizer"] = {"seen-data": True}
    os.environ["SCHEMA__NAMING"] = "tests.common.cases.normalizers.sql_upper"
    with pytest.raises(TableIdentifiersFrozen) as fr_ex:
        schema.update_normalizers()
    # _dlt_version is the first table to be normalized, and since there are tables
    # that have seen data, we consider _dlt_version also be materialized
    assert fr_ex.value.table_name == "_dlt_version"
    assert isinstance(fr_ex.value.from_naming, snake_case.NamingConvention)
    assert isinstance(fr_ex.value.to_naming, sql_upper.NamingConvention)
    # try again, get exception (schema was not partially modified)
    with pytest.raises(TableIdentifiersFrozen) as fr_ex:
        schema.update_normalizers()

    # use special naming convention that only changes column names ending with x to _
    issues_table["columns"]["columnx"] = {"name": "columnx", "data_type": "bigint"}
    assert schema.tables["issues"] is issues_table
    os.environ["SCHEMA__NAMING"] = "tests.common.cases.normalizers.snake_no_x"
    with pytest.raises(TableIdentifiersFrozen) as fr_ex:
        schema.update_normalizers()
    assert fr_ex.value.table_name == "issues"
    # allow to change tables with data
    os.environ["SCHEMA__ALLOW_IDENTIFIER_CHANGE_ON_TABLE_WITH_DATA"] = "True"
    schema.update_normalizers()
    assert schema._normalizers_config["allow_identifier_change_on_table_with_data"] is True


def assert_schema_identifiers_case(schema: Schema, casing: Callable[[str], str]) -> None:
    for table_name, table in schema.tables.items():
        assert table_name == casing(table_name) == table["name"]
        if "parent" in table:
            assert table["parent"] == casing(table["parent"])
        for col_name, column in table["columns"].items():
            assert col_name == casing(col_name) == column["name"]

    # make sure table prefixes are set
    assert schema._dlt_tables_prefix == casing("_dlt")
    assert schema.loads_table_name == casing("_dlt_loads")
    assert schema.version_table_name == casing("_dlt_version")
    assert schema.state_table_name == casing("_dlt_pipeline_state")

    def _case_regex(regex: str) -> str:
        if regex.startswith(SIMPLE_REGEX_PREFIX):
            return SIMPLE_REGEX_PREFIX + casing(regex[3:])
        else:
            return casing(regex)

    # regexes are uppercased
    new_default_hints = schema._settings["default_hints"]
    for hint, regexes in new_default_hints.items():
        # but all upper cased
        assert set(_case_regex(n) for n in regexes) == set(new_default_hints[hint])


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
