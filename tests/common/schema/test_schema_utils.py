from copy import deepcopy
from typing import List

import pytest

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TTableSchema, TTableSchemaColumns
from dlt.common.schema.utils import exclude_dlt_entities
from tests.common.utils import load_yml_case


@pytest.fixture
def ethereum_schema() -> Schema:
    return Schema.from_dict(load_yml_case("schemas/eth/ethereum_schema_v11"))


def _get_table(tables: List[TTableSchema], name: str) -> TTableSchema:
    return next(table for table in tables if table["name"] == name)


def test_exclude_dlt_entities_remove_none(ethereum_schema: Schema) -> None:
    tables = list(ethereum_schema.tables.values())

    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=False,
        exclude_dlt_columns=False,
    )

    filtered_names = {table["name"] for table in filtered}
    assert ethereum_schema.version_table_name in filtered_names
    assert ethereum_schema.loads_table_name in filtered_names
    assert "blocks" in filtered_names

    blocks = _get_table(filtered, "blocks")
    assert "_dlt_load_id" in blocks["columns"]
    assert "_dlt_id" in blocks["columns"]


def test_exclude_dlt_entities_remove_tables_only(ethereum_schema: Schema) -> None:
    tables = list(ethereum_schema.tables.values())

    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=ethereum_schema._dlt_tables_prefix,
        exclude_dlt_tables=True,
        exclude_dlt_columns=False,
    )

    filtered_names = {table["name"] for table in filtered}
    assert ethereum_schema.version_table_name not in filtered_names
    assert ethereum_schema.loads_table_name not in filtered_names
    assert "blocks" in filtered_names

    blocks = _get_table(filtered, "blocks")
    assert "_dlt_load_id" in blocks["columns"]
    assert "_dlt_id" in blocks["columns"]


def test_exclude_dlt_entities_remove_columns_only(ethereum_schema: Schema) -> None:
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

    blocks = _get_table(filtered, "blocks")
    block_column_names = set(blocks["columns"])
    assert "_dlt_load_id" not in block_column_names
    assert "_dlt_id" not in block_column_names


def test_exclude_dlt_entities_remove_tables_and_columns(ethereum_schema: Schema) -> None:
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
    assert "blocks" in filtered_names

    blocks = _get_table(filtered, "blocks")
    block_column_names = set(blocks["columns"])
    assert "_dlt_load_id" not in block_column_names
    assert "_dlt_id" not in block_column_names


def test_exclude_dlt_entities_honors_custom_prefix(ethereum_schema: Schema) -> None:
    new_prefix = "_NEW_DLT_PREFIX"
    # mimic user changing the schema-level dlt prefix
    ethereum_schema._dlt_tables_prefix = new_prefix
    version_table = deepcopy(ethereum_schema.tables[ethereum_schema.version_table_name])
    blocks_table = deepcopy(ethereum_schema.tables["blocks"])

    # turn the schema version table into a DLT table with the new prefix applied everywhere
    version_table["name"] = f"{new_prefix}version"
    first_version_column = next(iter(version_table["columns"].values()))
    version_table["columns"] = {
        f"{new_prefix}version_id": {
            **first_version_column,
            "name": f"{new_prefix}version_id",
        }
    }

    blocks_columns: TTableSchemaColumns = blocks_table["columns"]
    non_dlt_column_name = next(name for name in blocks_columns if not name.startswith("_dlt_"))
    prefixed_column_name = f"{new_prefix}load_id"
    prefixed_column: TColumnSchema = {
        **blocks_columns["_dlt_load_id"],
        "name": prefixed_column_name,
    }
    # keep one non dlt column so this remains a data table, but also add a new-prefix column
    blocks_table["columns"] = {
        non_dlt_column_name: blocks_columns[non_dlt_column_name],
        prefixed_column_name: prefixed_column,
    }

    tables: List[TTableSchema] = [version_table, blocks_table]

    filtered = exclude_dlt_entities(
        tables,
        normalized_dlt_prefix=new_prefix,
        exclude_dlt_tables=True,
        exclude_dlt_columns=True,
    )

    # only the non dlt table should be left and the prefixed column removed
    assert len(filtered) == 1
    remaining_table = filtered[0]
    assert remaining_table["name"] == blocks_table["name"]
    assert prefixed_column_name not in remaining_table["columns"]
    assert all(not column_name.startswith(new_prefix) for column_name in remaining_table["columns"])
