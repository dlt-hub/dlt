from copy import deepcopy
from typing import List

import pytest

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TTableSchema, TTableSchemaColumns
from dlt.common.schema.utils import (
    exclude_dlt_entities,
    version_table,
    loads_table,
    pipeline_state_table,
)
from tests.common.utils import load_yml_case

NAME_NORMALIZER_REFS = (
    "tests.common.cases.normalizers.title_case",
    "tests.common.cases.normalizers.sql_upper",
    "tests.common.cases.normalizers.snake_no_x",
)


@pytest.fixture
def ethereum_schema() -> Schema:
    return Schema.from_dict(load_yml_case("schemas/eth/ethereum_schema_v11"))


@pytest.mark.parametrize("name_normalizer_ref", NAME_NORMALIZER_REFS)
def test_exclude_dlt_entities_remove_none(
    ethereum_schema: Schema, name_normalizer_ref: str
) -> None:
    _apply_name_normalizer(ethereum_schema, name_normalizer_ref)

    tables = list(ethereum_schema.tables.values())
    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=False,
        exclude_dlt_columns=False,
    )

    # verify all tables are still present
    filtered_names = {table["name"] for table in filtered}
    assert ethereum_schema.version_table_name in filtered_names
    assert ethereum_schema.loads_table_name in filtered_names
    blocks_table_name = _normalized_table_name(ethereum_schema, "blocks")
    blocks = _get_table(filtered, blocks_table_name)
    assert blocks_table_name in filtered_names

    # as well as dlt columns
    dlt_load_id_column = _normalized_column_name(ethereum_schema, "_dlt_load_id")
    dlt_id_column = _normalized_column_name(ethereum_schema, "_dlt_id")
    assert dlt_load_id_column in blocks["columns"]
    assert dlt_id_column in blocks["columns"]


@pytest.mark.parametrize("name_normalizer_ref", NAME_NORMALIZER_REFS)
def test_exclude_dlt_entities_remove_tables_only(
    ethereum_schema: Schema, name_normalizer_ref: str
) -> None:
    _apply_name_normalizer(ethereum_schema, name_normalizer_ref)

    tables = list(ethereum_schema.tables.values())
    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=True,
        exclude_dlt_columns=False,
    )

    filtered_names = {table["name"] for table in filtered}

    # verify tables got removed
    blocks_table_name = _normalized_table_name(ethereum_schema, "blocks")
    assert ethereum_schema.version_table_name not in filtered_names
    assert ethereum_schema.loads_table_name not in filtered_names
    assert blocks_table_name in filtered_names

    # no columns got removed
    dlt_load_id_column = _normalized_column_name(ethereum_schema, "_dlt_load_id")
    dlt_id_column = _normalized_column_name(ethereum_schema, "_dlt_id")
    blocks = _get_table(filtered, blocks_table_name)
    assert dlt_load_id_column in blocks["columns"]
    assert dlt_id_column in blocks["columns"]


@pytest.mark.parametrize("name_normalizer_ref", NAME_NORMALIZER_REFS)
def test_exclude_dlt_entities_remove_columns_only(
    ethereum_schema: Schema, name_normalizer_ref: str
) -> None:
    _apply_name_normalizer(ethereum_schema, name_normalizer_ref)
    blocks_table_name = _normalized_table_name(ethereum_schema, "blocks")
    dlt_load_id_column = _normalized_column_name(ethereum_schema, "_dlt_load_id")
    dlt_id_column = _normalized_column_name(ethereum_schema, "_dlt_id")
    tables = list(ethereum_schema.tables.values())

    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=False,
        exclude_dlt_columns=True,
    )

    filtered_names = {table["name"] for table in filtered}
    assert ethereum_schema.version_table_name in filtered_names
    assert ethereum_schema.loads_table_name in filtered_names

    blocks = _get_table(filtered, blocks_table_name)
    block_column_names = set(blocks["columns"])
    assert dlt_load_id_column not in block_column_names
    assert dlt_id_column not in block_column_names


@pytest.mark.parametrize("name_normalizer_ref", NAME_NORMALIZER_REFS)
def test_exclude_dlt_entities_remove_tables_and_columns(
    ethereum_schema: Schema, name_normalizer_ref: str
) -> None:
    _apply_name_normalizer(ethereum_schema, name_normalizer_ref)
    blocks_table_name = _normalized_table_name(ethereum_schema, "blocks")
    dlt_load_id_column = _normalized_column_name(ethereum_schema, "_dlt_load_id")
    dlt_id_column = _normalized_column_name(ethereum_schema, "_dlt_id")
    tables = list(ethereum_schema.tables.values())

    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=True,
        exclude_dlt_columns=True,
    )

    filtered_names = {table["name"] for table in filtered}
    assert ethereum_schema.version_table_name not in filtered_names
    assert ethereum_schema.loads_table_name not in filtered_names
    assert blocks_table_name in filtered_names

    blocks = _get_table(filtered, blocks_table_name)
    block_column_names = set(blocks["columns"])
    assert dlt_load_id_column not in block_column_names
    assert dlt_id_column not in block_column_names


# Helper functions
def _get_table(tables: List[TTableSchema], name: str) -> TTableSchema:
    return next(table for table in tables if table["name"] == name)


def _normalized_table_name(schema: Schema, table_name: str) -> str:
    return schema.naming.normalize_table_identifier(table_name)


def _normalized_column_name(schema: Schema, column_name: str) -> str:
    return schema.naming.normalize_identifier(column_name)


def _apply_name_normalizer(schema: Schema, name_normalizer_ref: str) -> None:
    schema._normalizers_config["names"] = name_normalizer_ref
    schema._normalizers_config["allow_identifier_change_on_table_with_data"] = True
    schema.update_normalizers()


# Tests for internal table column descriptions
def test_version_table_has_column_descriptions() -> None:
    """Test that _dlt_version table has descriptions for all columns"""
    table = version_table()

    # Verify table has description
    assert "description" in table
    assert table["description"] == "Created by DLT. Tracks schema updates"

    # Verify all columns have descriptions
    expected_columns = [
        "version",
        "engine_version",
        "inserted_at",
        "schema_name",
        "version_hash",
        "schema"
    ]
    for col_name in expected_columns:
        assert col_name in table["columns"], f"Column {col_name} missing from version_table"
        col = table["columns"][col_name]
        assert "description" in col, f"Column {col_name} missing description"
        assert isinstance(col["description"], str), f"Column {col_name} description should be a string"
        assert len(col["description"]) > 0, f"Column {col_name} description should not be empty"


def test_loads_table_has_column_descriptions() -> None:
    """Test that _dlt_loads table has descriptions for all columns"""
    table = loads_table()

    # Verify table has description
    assert "description" in table
    assert table["description"] == "Created by DLT. Tracks completed loads"

    # Verify all columns have descriptions
    expected_columns = [
        "load_id",
        "schema_name",
        "status",
        "inserted_at",
        "schema_version_hash"
    ]
    for col_name in expected_columns:
        assert col_name in table["columns"], f"Column {col_name} missing from loads_table"
        col = table["columns"][col_name]
        assert "description" in col, f"Column {col_name} missing description"
        assert isinstance(col["description"], str), f"Column {col_name} description should be a string"
        assert len(col["description"]) > 0, f"Column {col_name} description should not be empty"


def test_pipeline_state_table_has_column_descriptions() -> None:
    """Test that _dlt_pipeline_state table has descriptions for all columns"""
    table = pipeline_state_table(add_dlt_id=True)

    # Verify table has description
    assert "description" in table
    assert table["description"] == "Created by DLT. Tracks pipeline state"

    # Verify all columns have descriptions
    expected_columns = [
        "version",
        "engine_version",
        "pipeline_name",
        "state",
        "created_at",
        "version_hash",
        "_dlt_load_id",
        "_dlt_id",
    ]
    for col_name in expected_columns:
        assert col_name in table["columns"], f"Column {col_name} missing from pipeline_state_table"
        col = table["columns"][col_name]
        assert "description" in col, f"Column {col_name} missing description"
        assert isinstance(col["description"], str), f"Column {col_name} description should be a string"
        assert len(col["description"]) > 0, f"Column {col_name} description should not be empty"


def test_pipeline_state_table_without_dlt_id() -> None:
    """Test that _dlt_pipeline_state table without dlt_id has correct columns"""
    table = pipeline_state_table(add_dlt_id=False)

    # Verify _dlt_id is not present when add_dlt_id=False
    assert "_dlt_id" not in table["columns"]

    # Verify _dlt_load_id is still present and has description
    assert "_dlt_load_id" in table["columns"]
    assert "description" in table["columns"]["_dlt_load_id"]


def test_internal_tables_column_descriptions_are_not_empty() -> None:
    """Test that all internal table columns have non-empty descriptions"""
    tables = [
        ("_dlt_version", version_table()),
        ("_dlt_loads", loads_table()),
        ("_dlt_pipeline_state", pipeline_state_table(add_dlt_id=True)),
    ]

    for table_name, table in tables:
        for col_name, col in table["columns"].items():
            assert "description" in col, (
                f"Table {table_name}, column {col_name} missing description"
            )
            assert col["description"] is not None and col["description"].strip() != "", (
                f"Table {table_name}, column {col_name} has empty description"
            )