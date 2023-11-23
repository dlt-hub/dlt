from typing import cast

import pytest
import copy

from dlt.common.schema import Schema, DEFAULT_SCHEMA_CONTRACT_MODE, TSchemaContractDict
from dlt.common.schema.exceptions import DataValidationError
from dlt.common.schema.typing import TTableSchema


def get_schema() -> Schema:
    s = Schema("event")

    columns = {
        "column_1": {"name": "column_1", "data_type": "text"},
        "column_2": {"name": "column_2", "data_type": "bigint", "is_variant": True},
    }

    incomplete_columns = {
        "incomplete_column_1": {
            "name": "incomplete_column_1",
        },
        "incomplete_column_2": {
            "name": "incomplete_column_2",
        },
    }

    # add some tables
    s.update_table(cast(TTableSchema, {"name": "tables", "columns": columns}))

    s.update_table(
        cast(TTableSchema, {"name": "child_table", "parent": "tables", "columns": columns})
    )

    s.update_table(cast(TTableSchema, {"name": "incomplete_table", "columns": incomplete_columns}))

    s.update_table(
        cast(TTableSchema, {"name": "mixed_table", "columns": {**incomplete_columns, **columns}})
    )

    s.update_table(
        cast(
            TTableSchema,
            {
                "name": "evolve_once_table",
                "x-normalizer": {"evolve-columns-once": True},
                "columns": {**incomplete_columns, **columns},
            },
        )
    )

    return s


def test_resolve_contract_settings() -> None:
    # defaults
    schema = get_schema()
    assert schema.resolve_contract_settings_for_table("tables") == DEFAULT_SCHEMA_CONTRACT_MODE
    assert schema.resolve_contract_settings_for_table("child_table") == DEFAULT_SCHEMA_CONTRACT_MODE

    # table specific full setting
    schema = get_schema()
    schema.tables["tables"]["schema_contract"] = "freeze"
    assert schema.resolve_contract_settings_for_table("tables") == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze",
    }
    assert schema.resolve_contract_settings_for_table("child_table") == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze",
    }

    # table specific single setting
    schema = get_schema()
    schema.tables["tables"]["schema_contract"] = {
        "tables": "freeze",
        "columns": "discard_value",
    }
    assert schema.resolve_contract_settings_for_table("tables") == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve",
    }
    assert schema.resolve_contract_settings_for_table("child_table") == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve",
    }

    # schema specific full setting
    schema = get_schema()
    schema._settings["schema_contract"] = "freeze"
    assert schema.resolve_contract_settings_for_table("tables") == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze",
    }
    assert schema.resolve_contract_settings_for_table("child_table") == {
        "tables": "freeze",
        "columns": "freeze",
        "data_type": "freeze",
    }

    # schema specific single setting
    schema = get_schema()
    schema._settings["schema_contract"] = {
        "tables": "freeze",
        "columns": "discard_value",
    }
    assert schema.resolve_contract_settings_for_table("tables") == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve",
    }
    assert schema.resolve_contract_settings_for_table("child_table") == {
        "tables": "freeze",
        "columns": "discard_value",
        "data_type": "evolve",
    }

    # mixed settings: table setting always prevails
    schema = get_schema()
    schema._settings["schema_contract"] = "freeze"
    schema.tables["tables"]["schema_contract"] = {
        "tables": "evolve",
        "columns": "discard_value",
    }
    assert schema.resolve_contract_settings_for_table("tables") == {
        "tables": "evolve",
        "columns": "discard_value",
        "data_type": "evolve",
    }
    assert schema.resolve_contract_settings_for_table("child_table") == {
        "tables": "evolve",
        "columns": "discard_value",
        "data_type": "evolve",
    }


# ensure other settings do not interfere with the main setting we are testing
base_settings = [
    {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
    {"tables": "discard_row", "columns": "discard_row", "data_type": "discard_row"},
    {"tables": "discard_value", "columns": "discard_value", "data_type": "discard_value"},
    {"tables": "freeze", "columns": "freeze", "data_type": "freeze"},
]


@pytest.mark.parametrize("base_settings", base_settings)
def test_check_adding_table(base_settings) -> None:
    schema = get_schema()
    new_table = copy.deepcopy(schema.tables["tables"])
    new_table["name"] = "new_table"

    #
    # check adding new table
    #
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**base_settings, **{"tables": "evolve"}}), new_table
    )
    assert (partial, filters) == (new_table, [])
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**base_settings, **{"tables": "discard_row"}}), new_table
    )
    assert (partial, filters) == (None, [("tables", "new_table", "discard_row")])
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**base_settings, **{"tables": "discard_value"}}), new_table
    )
    assert (partial, filters) == (None, [("tables", "new_table", "discard_value")])
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**base_settings, **{"tables": "freeze"}}),
        new_table,
        raise_on_freeze=False,
    )
    assert (partial, filters) == (None, [("tables", "new_table", "freeze")])

    with pytest.raises(DataValidationError) as val_ex:
        schema.apply_schema_contract(
            cast(TSchemaContractDict, {**base_settings, **{"tables": "freeze"}}),
            new_table,
            data_item={"item": 1},
        )
    assert val_ex.value.schema_name == schema.name
    assert val_ex.value.table_name == "new_table"
    assert val_ex.value.column_name is None
    assert val_ex.value.contract_entity == "tables"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema is None  # there's no validating schema on new table
    assert val_ex.value.data_item == {"item": 1}


@pytest.mark.parametrize("base_settings", base_settings)
def test_check_adding_new_columns(base_settings) -> None:
    schema = get_schema()

    def assert_new_column(table_update: TTableSchema, column_name: str) -> None:
        popped_table_update = copy.deepcopy(table_update)
        popped_table_update["columns"].pop(column_name)

        partial, filters = schema.apply_schema_contract(
            cast(TSchemaContractDict, {**base_settings, **{"columns": "evolve"}}),
            copy.deepcopy(table_update),
        )
        assert (partial, filters) == (table_update, [])
        partial, filters = schema.apply_schema_contract(
            cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_row"}}),
            copy.deepcopy(table_update),
        )
        assert (partial, filters) == (
            popped_table_update,
            [("columns", column_name, "discard_row")],
        )
        partial, filters = schema.apply_schema_contract(
            cast(TSchemaContractDict, {**base_settings, **{"columns": "discard_value"}}),
            copy.deepcopy(table_update),
        )
        assert (partial, filters) == (
            popped_table_update,
            [("columns", column_name, "discard_value")],
        )
        partial, filters = schema.apply_schema_contract(
            cast(TSchemaContractDict, {**base_settings, **{"columns": "freeze"}}),
            copy.deepcopy(table_update),
            raise_on_freeze=False,
        )
        assert (partial, filters) == (popped_table_update, [("columns", column_name, "freeze")])

        with pytest.raises(DataValidationError) as val_ex:
            schema.apply_schema_contract(
                cast(TSchemaContractDict, {**base_settings, **{"columns": "freeze"}}),
                copy.deepcopy(table_update),
                {column_name: 1},
            )
        assert val_ex.value.schema_name == schema.name
        assert val_ex.value.table_name == table_update["name"]
        assert val_ex.value.column_name == column_name
        assert val_ex.value.contract_entity == "columns"
        assert val_ex.value.contract_mode == "freeze"
        assert val_ex.value.table_schema == schema.get_table(table_update["name"])
        assert val_ex.value.data_item == {column_name: 1}

    #
    # check adding new column
    #
    table_update: TTableSchema = {
        "name": "tables",
        "columns": {"new_column": {"name": "new_column", "data_type": "text"}},
    }
    assert_new_column(table_update, "new_column")

    #
    # check adding new column if target column is not complete
    #
    table_update = {
        "name": "mixed_table",
        "columns": {
            "incomplete_column_1": {
                "name": "incomplete_column_1",
            }
        },
    }
    assert_new_column(table_update, "incomplete_column_1")

    #
    # check x-normalize evolve_once behaving as evolve override
    #
    table_update = {
        "name": "evolve_once_table",
        "columns": {
            "new_column": {"name": "new_column", "data_type": "text"},
            "incomplete_column_1": {
                "name": "incomplete_column_1",
            },
        },
    }
    partial, filters = schema.apply_schema_contract(base_settings, copy.deepcopy(table_update))
    assert (partial, filters) == (table_update, [])


def test_check_adding_new_variant() -> None:
    schema = get_schema()

    #
    # check adding new variant column
    #
    table_update: TTableSchema = {
        "name": "tables",
        "columns": {
            "column_2_variant": {"name": "column_2_variant", "data_type": "bigint", "variant": True}
        },
    }
    popped_table_update = copy.deepcopy(table_update)
    popped_table_update["columns"].pop("column_2_variant")

    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve"}}),
        copy.deepcopy(table_update),
    )
    assert (partial, filters) == (table_update, [])
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "discard_row"}}),
        copy.deepcopy(table_update),
    )
    assert (partial, filters) == (
        popped_table_update,
        [("columns", "column_2_variant", "discard_row")],
    )
    partial, filters = schema.apply_schema_contract(
        cast(
            TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "discard_value"}}
        ),
        copy.deepcopy(table_update),
    )
    assert (partial, filters) == (
        popped_table_update,
        [("columns", "column_2_variant", "discard_value")],
    )
    partial, filters = schema.apply_schema_contract(
        cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "freeze"}}),
        copy.deepcopy(table_update),
        raise_on_freeze=False,
    )
    assert (partial, filters) == (popped_table_update, [("columns", "column_2_variant", "freeze")])

    with pytest.raises(DataValidationError) as val_ex:
        schema.apply_schema_contract(
            cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "freeze"}}),
            copy.deepcopy(table_update),
        )
    assert val_ex.value.schema_name == schema.name
    assert val_ex.value.table_name == table_update["name"]
    assert val_ex.value.column_name == "column_2_variant"
    assert val_ex.value.contract_entity == "data_type"
    assert val_ex.value.contract_mode == "freeze"
    assert val_ex.value.table_schema == schema.get_table(table_update["name"])
    assert val_ex.value.data_item is None  # we do not pass it to apply_schema_contract

    # variants are not new columns - new data types
    partial, filters = schema.apply_schema_contract(
        cast(
            TSchemaContractDict,
            {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "evolve", "columns": "freeze"}},
        ),
        copy.deepcopy(table_update),
    )
    assert (partial, filters) == (table_update, [])

    # evolve once does not apply to variant evolution
    table_update["name"] = "evolve_once_table"
    with pytest.raises(DataValidationError):
        schema.apply_schema_contract(
            cast(TSchemaContractDict, {**DEFAULT_SCHEMA_CONTRACT_MODE, **{"data_type": "freeze"}}),
            copy.deepcopy(table_update),
        )
