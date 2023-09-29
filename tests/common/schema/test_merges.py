import pytest
from copy import copy, deepcopy

from dlt.common.schema import Schema, utils
from dlt.common.schema.exceptions import CannotCoerceColumnException, CannotCoerceNullException, TablePropertiesConflictException
from dlt.common.schema.typing import TStoredSchema, TTableSchema


COL_1_HINTS = {
            "cluster": False,
            "foreign_key": True,
            "data_type": "text",
            "name": "test",
            "x-special": True,
            "x-special-int": 100,
            "nullable": False,
            "x-special-bool": False,
            "prop": None
        }

COL_1_HINTS_DEFAULTS = {
        'foreign_key': True,
        'data_type': 'text',
        'name': 'test',
        'x-special': True,
        'x-special-int': 100,
        'nullable': False,
        "x-special-bool": False,
        }

COL_2_HINTS = {
    "nullable": True,
    "name": "test_2",
    "primary_key": False
}


def test_check_column_defaults() -> None:
    assert utils.has_default_column_hint_value("data_type", "text") is False
    assert utils.has_default_column_hint_value("name", 123) is False
    assert utils.has_default_column_hint_value("nullable", True) is True
    assert utils.has_default_column_hint_value("nullable", False) is False
    assert utils.has_default_column_hint_value("x-special", False) is False
    assert utils.has_default_column_hint_value("unique", False) is True
    assert utils.has_default_column_hint_value("unique", True) is False


def test_column_remove_defaults() -> None:
    clean = utils.remove_column_defaults(copy(COL_1_HINTS))
    # mind that nullable default is False and Nones will be removed
    assert clean == COL_1_HINTS_DEFAULTS
    # check nullable True
    assert utils.remove_column_defaults(copy(COL_2_HINTS)) == {"name": "test_2"}


def test_column_add_defaults() -> None:
    # test complete column
    full = utils.add_column_defaults(copy(COL_1_HINTS))
    assert full["unique"] is False
    # remove defaults from full
    clean = utils.remove_column_defaults(copy(full))
    assert clean == COL_1_HINTS_DEFAULTS
    # prop is None and will be removed
    del full["prop"]
    assert utils.add_column_defaults(copy(clean)) == full

    # test incomplete
    complete_full = utils.add_column_defaults(copy(COL_2_HINTS))
    # defaults are added
    assert complete_full["unique"] is False


def test_remove_defaults_stored_schema() -> None:
    table: TTableSchema = {
        "name": "table",
        "parent": "parent",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {
            "test": COL_1_HINTS,
            "test_2": COL_2_HINTS
        }
    }
    stored_schema: TStoredSchema = {
        "name": "schema",
        "tables": {
            "table": deepcopy(table),
            "table_copy": deepcopy(table)
        },
        "x-top-level": True
    }
    # mock the case in table_copy where resource == table_name
    stored_schema["tables"]["table_copy"]["resource"] = stored_schema["tables"]["table_copy"]["name"] = "table_copy"

    default_stored = utils.remove_defaults(stored_schema)
    # nullability always present
    assert default_stored["tables"]["table"]["columns"]["test"]["nullable"] is False
    assert default_stored["tables"]["table"]["columns"]["test_2"]["nullable"] is True
    # not removed in complete column (as it was explicitly set to False)
    assert default_stored["tables"]["table"]["columns"]["test"]["cluster"] is False
    # not removed in incomplete one
    assert default_stored["tables"]["table"]["columns"]["test_2"]["primary_key"] is False
    # resource present
    assert default_stored["tables"]["table"]["resource"] == "🦚Table"
    # resource removed because identical to table name
    assert "resource" not in default_stored["tables"]["table_copy"]

    # apply defaults
    restored_schema = utils.apply_defaults(deepcopy(default_stored))
    # drop all names - they are added
    del restored_schema["tables"]["table"]["name"]
    del restored_schema["tables"]["table"]["columns"]["test"]["name"]
    del restored_schema["tables"]["table"]["columns"]["test_2"]["name"]
    del restored_schema["tables"]["table_copy"]["name"]
    del restored_schema["tables"]["table_copy"]["columns"]["test"]["name"]
    del restored_schema["tables"]["table_copy"]["columns"]["test_2"]["name"]
    assert stored_schema == restored_schema


def test_new_incomplete_column() -> None:
    # no data_type so incomplete
    incomplete_col = utils.new_column("I", nullable=False)
    assert utils.is_complete_column(incomplete_col) is False
    # default hints not there
    assert "primary_key" not in incomplete_col

    incomplete_col["primary_key"] = True
    incomplete_col["x-special"] = "spec"
    table = utils.new_table("table", columns=[incomplete_col])
    # incomplete column must be added without hints
    assert table["columns"]["I"]["primary_key"] is True
    assert table["columns"]["I"]["x-special"] == "spec"
    assert "merge_key" not in incomplete_col


def test_merge_columns() -> None:
    # tab_b overrides non default
    col_a = utils.merge_columns(copy(COL_1_HINTS), copy(COL_2_HINTS), merge_defaults=False)
    # nullable is False - tab_b has it as default and those are not merged
    assert col_a == {
        "name": "test_2",
        "nullable": False,
        'cluster': False,
        'foreign_key': True,
        'data_type': 'text',
        'x-special': True,
        'x-special-int': 100,
        'x-special-bool': False,
        'prop': None
    }

    col_a = utils.merge_columns(copy(COL_1_HINTS), copy(COL_2_HINTS), merge_defaults=True)
    # nullable is True and primary_key is present - default values are merged
    assert col_a == {
        "name": "test_2",
        "nullable": True,
        'cluster': False,
        'foreign_key': True,
        'data_type': 'text',
        'x-special': True,
        'x-special-int': 100,
        'x-special-bool': False,
        'prop': None,
        'primary_key': False
    }


def test_diff_tables() -> None:
    table: TTableSchema = {
        "name": "table",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {
            "test": COL_1_HINTS,
            "test_2": COL_2_HINTS
        }
    }
    empty = utils.new_table("table")
    del empty["resource"]
    print(empty)
    partial = utils.diff_tables(empty, deepcopy(table))
    # partial is simply table
    assert partial == table
    partial = utils.diff_tables(deepcopy(table), empty)
    # partial is empty
    assert partial == empty

    # override name and description
    changed = deepcopy(table)
    changed["description"] = "new description"
    changed["name"] = "new name"
    partial = utils.diff_tables(deepcopy(table), changed)
    print(partial)
    assert partial == {
        "name": "new name",
        "description": "new description",
        "columns": {}
    }

    # ignore identical table props
    existing = deepcopy(table)
    changed["write_disposition"] = "append"
    changed["schema_contract"] = "freeze"
    partial = utils.diff_tables(deepcopy(existing), changed)
    assert partial == {
        "name": "new name",
        "description": "new description",
        "write_disposition": "append",
        "schema_contract": "freeze",
        "columns": {}
    }
    existing["write_disposition"] = "append"
    existing["schema_contract"] = "freeze"
    partial = utils.diff_tables(deepcopy(existing), changed)
    assert partial == {
        "name": "new name",
        "description": "new description",
        "columns": {}
    }

    # detect changed column
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["cluster"] = True
    partial = utils.diff_tables(existing, changed)
    assert "test" in partial["columns"]
    assert "test_2" not in partial["columns"]
    assert existing["columns"]["test"] == table["columns"]["test"] != partial["columns"]["test"]

    # defaults are not ignored
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["foreign_key"] = False
    partial = utils.diff_tables(existing, changed)
    assert "test" in partial["columns"]

    # even if not present in tab_a at all
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["foreign_key"] = False
    del existing["columns"]["test"]["foreign_key"]
    partial = utils.diff_tables(existing, changed)
    assert "test" in partial["columns"]


def test_diff_tables_conflicts() -> None:
    # conflict on parents
    table: TTableSchema = {
        "name": "table",
        "parent": "parent",
        "description": "description",
        "x-special": 128,
        "columns": {
            "test": COL_1_HINTS,
            "test_2": COL_2_HINTS
        }
    }

    other = utils.new_table("table_2")
    with pytest.raises(TablePropertiesConflictException) as cf_ex:
        utils.diff_tables(table, other)
    assert cf_ex.value.table_name == "table"
    assert cf_ex.value.prop_name == "parent"

    # conflict on data types in columns
    changed = deepcopy(table)
    changed["columns"]["test"]["data_type"] = "bigint"
    with pytest.raises(CannotCoerceColumnException):
        utils.diff_tables(table, changed)


def test_merge_tables() -> None:
    table: TTableSchema = {
        "name": "table",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {
            "test": COL_1_HINTS,
            "test_2": COL_2_HINTS
        }
    }
    changed = deepcopy(table)
    changed["x-special"] = 129
    changed["description"] = "new description"
    changed["new-prop-1"] = "A"
    changed["new-prop-2"] = None
    changed["new-prop-3"] = False
    # drop column so partial has it
    del table["columns"]["test"]
    partial = utils.merge_tables(table, changed)
    assert "test" in table["columns"]
    assert table["x-special"] == 129
    assert table["description"] == "new description"
    assert table["new-prop-1"] == "A"
    # None are not merged in
    assert "new-prop-2" not in table
    assert table["new-prop-3"] is False

    # one column in partial
    assert len(partial["columns"]) == 1
    assert partial["columns"]["test"] == COL_1_HINTS
