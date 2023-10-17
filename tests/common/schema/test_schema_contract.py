from typing import cast

import pytest
import copy

from dlt.common.schema import Schema, DEFAULT_SCHEMA_CONTRACT_MODE, TSchemaContractDict
from dlt.common.schema.schema import resolve_contract_settings_for_table
from dlt.common.schema.exceptions import SchemaFrozenException
from dlt.common.schema.typing import TTableSchema

def get_schema() -> Schema:
    s = Schema("event")

    columns =  {
        "column_1": {
            "name": "column_1",
            "data_type": "text"
        },
        "column_2": {
            "name": "column_2",
            "data_type": "bigint",
            "is_variant": True
        }
    }

    incomplete_columns = {
        "incomplete_column_1": {
            "name": "incomplete_column_1",
        },
        "incomplete_column_2": {
            "name": "incomplete_column_2",
        }
    }


    # add some tables
    s.update_table(cast(TTableSchema, {
        "name": "tables",
        "columns": columns
    }))

    s.update_table(cast(TTableSchema, {
        "name": "child_table",
        "parent": "tables",
        "columns": columns
    }))

    s.update_table(cast(TTableSchema, {
        "name": "incomplete_table",
        "columns": incomplete_columns
    }))

    s.update_table(cast(TTableSchema, {
        "name": "mixed_table",
        "columns": {**incomplete_columns, **columns}
    }))

    return s


def test_resolve_contract_settings() -> None:

    # defaults
    schema = get_schema()
    assert resolve_contract_settings_for_table(None, "tables", schema) == DEFAULT_SCHEMA_CONTRACT_MODE
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == DEFAULT_SCHEMA_CONTRACT_MODE

    # table specific full setting
    schema = get_schema()
    schema.tables["tables"]["schema_contract"] = "freeze"
    assert resolve_contract_settings_for_table(None, "tables", schema) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }

    # table specific single setting
    schema = get_schema()
    schema.tables["tables"]["schema_contract"] = {
        "tables": "freeze",
        "columns": "discard_value",
    }
    assert resolve_contract_settings_for_table(None, "tables", schema) == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve"
    }
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve"
    }

    # schema specific full setting
    schema = get_schema()
    schema._settings["schema_contract"] = "freeze"
    assert resolve_contract_settings_for_table(None, "tables", schema) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }

    # schema specific single setting
    schema = get_schema()
    schema._settings["schema_contract"] = {
        "tables": "freeze",
        "columns": "discard_value",
    }
    assert resolve_contract_settings_for_table(None, "tables", schema) == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve"
    }
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve"
    }

    # mixed settings: table setting always prevails
    schema = get_schema()
    schema._settings["schema_contract"] = "freeze"
    schema.tables["tables"]["schema_contract"] = {
        "tables": "evolve",
        "columns": "discard_value",
    }
    assert resolve_contract_settings_for_table(None, "tables", schema) == {
        "tables": "evolve",
        "columns": "discard_value",
        "data_type": "evolve"
    }
    assert resolve_contract_settings_for_table("tables", "child_table", schema) == {
        "tables": "evolve",
        "columns": "discard_value",
        "data_type": "evolve"
    }

    # current and incoming schema
    current_schema = get_schema()
    current_schema._settings["schema_contract"] = "discard_value"
    incoming_schema = get_schema()
    incoming_schema._settings["schema_contract"] = "discard_row"
    incoming_table: TTableSchema = {"name": "incomplete_table", "schema_contract": "freeze"}


    # incoming schema overrides
    assert resolve_contract_settings_for_table(None, "tables", current_schema, incoming_schema) == {
        "tables": "discard_row",
        "columns": "discard_row",
        "data_type": "discard_row"
    }

    # direct incoming table overrides
    assert resolve_contract_settings_for_table(None, "tables", current_schema, incoming_schema, incoming_table) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }

    # table defined on existing schema overrided incoming schema setting
    current_schema.tables["tables"]["schema_contract"] = "discard_value"
    assert resolve_contract_settings_for_table(None, "tables", current_schema, incoming_schema) == {
        "tables": "discard_value",
        "columns": "discard_value",
        "data_type": "discard_value"
    }

    # but table on incoming schema overrides again
    incoming_schema.tables["tables"]["schema_contract"] = "discard_row"
    assert resolve_contract_settings_for_table(None, "tables", current_schema, incoming_schema) == {
        "tables": "discard_row",
        "columns": "discard_row",
        "data_type": "discard_row"
    }

    # incoming table still overrides all
    assert resolve_contract_settings_for_table(None, "tables", current_schema, incoming_schema, incoming_table) == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }

# ensure other settings do not interfere with the main setting we are testing
base_settings = [{
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "evolve"
    },{
        "tables": "discard_row",
        "columns": "discard_row",
        "data_type": "discard_row"
    }, {
        "tables": "discard_value",
        "columns": "discard_value",
        "data_type": "discard_value"
    }, {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze"
    }
]


@pytest.mark.parametrize("base_settings", base_settings)
def test_check_adding_table(base_settings) -> None:

    schema = get_schema()
    data = {
        "column_1": "some string",
        "column_2": 123
    }
    new_table = copy.deepcopy(schema.tables["tables"])
    new_table["name"] = "new_table"

    #
    # check adding new table
    #
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"tables": "evolve"}}), "new_table", data, new_table) == (data, new_table)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"tables": "discard_row"}}), "new_table", data, new_table) == (None, None)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"tables": "discard_value"}}), "new_table", data, new_table) == (None, None)

    with pytest.raises(SchemaFrozenException):
        schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"tables": "freeze"}}), "new_table", data, new_table)


@pytest.mark.parametrize("base_settings", base_settings)
def test_check_adding_new_columns(base_settings) -> None:
    schema = get_schema()

    #
    # check adding new column
    #
    data = {
        "column_1": "some string",
        "column_2": 123
    }
    data_with_new_row = {
        **data,
        "new_column": "some string"
    }
    table_update: TTableSchema = {
        "name": "tables",
        "columns": {
            "new_column": {
                "name": "new_column",
                "data_type": "text"
            }
        }
    }
    popped_table_update = copy.deepcopy(table_update)
    popped_table_update["columns"].pop("new_column")

    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "evolve"}}), "tables", copy.deepcopy(data_with_new_row), table_update) == (data_with_new_row, table_update)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_row"}}), "tables", copy.deepcopy(data_with_new_row), table_update) == (None, None)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_value"}}), "tables", copy.deepcopy(data_with_new_row), table_update) == (data, popped_table_update)

    with pytest.raises(SchemaFrozenException):
        schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "freeze"}}), "tables", copy.deepcopy(data_with_new_row), table_update)


    #
    # check adding new column if target column is not complete
    #
    data = {
        "column_1": "some string",
        "column_2": 123,
    }
    data_with_new_row = {
        **data,
        "incomplete_column_1": "some other string",
    }
    table_update = {
        "name": "mixed_table",
        "columns": {
            "incomplete_column_1": {
                "name": "incomplete_column_1",
                "data_type": "text"
            }
        }
    }
    popped_table_update = copy.deepcopy(table_update)
    popped_table_update["columns"].pop("incomplete_column_1")

    # incomplete columns should be treated like new columns
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "evolve"}}), "mixed_table", copy.deepcopy(data_with_new_row), table_update) == (data_with_new_row, table_update)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_row"}}), "mixed_table", copy.deepcopy(data_with_new_row), table_update) == (None, None)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_value"}}), "mixed_table", copy.deepcopy(data_with_new_row), table_update) == (data, popped_table_update)

    with pytest.raises(SchemaFrozenException):
        schema.apply_schema_contract(cast(TSchemaContractDict, {**base_settings, **{"columns": "freeze"}}), "mixed_table", copy.deepcopy(data_with_new_row), table_update)



def test_check_adding_new_variant() -> None:
    schema = get_schema()

    #
    # check adding new variant column
    #
    data = {
        "column_1": "some string",
        "column_2": 123
    }
    data_with_new_row = {
        **data,
        "column_2_variant": 345345
    }
    table_update: TTableSchema = {
        "name": "tables",
        "columns": {
            "column_2_variant": {
                "name": "column_2_variant",
                "data_type": "bigint",
                "variant": True
            }
        }
    }
    popped_table_update = copy.deepcopy(table_update)
    popped_table_update["columns"].pop("column_2_variant")

    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (data_with_new_row, table_update)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "discard_row"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (None, None)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "discard_value"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (data, popped_table_update)

    with pytest.raises(SchemaFrozenException):
        schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "freeze"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update))

    # check interaction with new columns settings, variants are new columns..
    with pytest.raises(SchemaFrozenException):
        assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve", "columns": "freeze"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (data_with_new_row, table_update)

    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve", "columns": "discard_row"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (None, None)
    assert schema.apply_schema_contract(cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve", "columns": "discard_value"}}), "tables", copy.deepcopy(data_with_new_row), copy.deepcopy(table_update)) == (data, popped_table_update)