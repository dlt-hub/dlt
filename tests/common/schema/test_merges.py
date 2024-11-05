from typing import Any
import pytest
from copy import copy, deepcopy

from dlt.common.schema import utils
from dlt.common.schema.exceptions import (
    CannotCoerceColumnException,
    TablePropertiesConflictException,
)
from dlt.common.schema.typing import TColumnSchemaBase, TStoredSchema, TTableSchema, TColumnSchema


COL_1_HINTS: TColumnSchema = {  # type: ignore[typeddict-unknown-key]
    "cluster": False,
    "parent_key": True,
    "data_type": "text",
    "name": "test",
    "x-special": "value",
    "x-special-int": 100,
    "nullable": False,
    "x-special-bool-true": True,
    "x-special-bool": False,
    "prop": None,
}

COL_1_HINTS_NO_DEFAULTS: TColumnSchema = {  # type: ignore[typeddict-unknown-key]
    "parent_key": True,
    "data_type": "text",
    "name": "test",
    "x-special": "value",
    "x-special-int": 100,
    "nullable": False,
    "x-special-bool-true": True,
}

COL_2_HINTS: TColumnSchema = {"nullable": True, "name": "test_2", "primary_key": False}


@pytest.mark.parametrize(
    "prop,value,is_default",
    (
        ("data_type", "text", False),
        ("data_type", None, True),
        ("name", "xyz", False),
        ("name", None, True),
        ("nullable", True, True),
        ("nullable", False, False),
        ("nullable", None, True),
        ("x-special", False, True),
        ("x-special", True, False),
        ("x-special", None, True),
        ("unique", False, True),
        ("unique", True, False),
        ("dedup_sort", "asc", False),
        ("dedup_sort", None, True),
        ("x-active-record-timestamp", None, False),
        ("x-active-record-timestamp", "2100-01-01", False),
    ),
)
def test_check_column_with_props(prop: str, value: Any, is_default: bool) -> None:
    # check default
    assert utils.has_default_column_prop_value(prop, value) is is_default
    # check if column with prop is found
    if prop == "name" and is_default:
        # do not check name not present
        return
    column: TColumnSchema = {"name": "column_a"}
    column[prop] = value  # type: ignore[literal-required]
    table = utils.new_table("test", columns=[column])
    expected_columns = [column["name"]] if not is_default else []
    expected_column = column["name"] if not is_default else None
    assert (
        utils.get_columns_names_with_prop(table, prop, include_incomplete=True) == expected_columns
    )
    assert (
        utils.get_first_column_name_with_prop(table, prop, include_incomplete=True)
        == expected_column
    )
    assert utils.has_column_with_prop(table, prop, include_incomplete=True) is not is_default
    # if data_type is set, column is complete
    if prop == "data_type" and not is_default:
        assert (
            utils.get_columns_names_with_prop(table, prop, include_incomplete=False)
            == expected_columns
        )
    else:
        assert utils.get_columns_names_with_prop(table, prop, include_incomplete=False) == []


def test_column_remove_defaults() -> None:
    clean = utils.remove_column_defaults(copy(COL_1_HINTS))
    # mind that nullable default is False and Nones will be removed
    assert clean == COL_1_HINTS_NO_DEFAULTS
    # check nullable True
    assert utils.remove_column_defaults(copy(COL_2_HINTS)) == {"name": "test_2"}


# def test_column_add_defaults() -> None:
#     # test complete column
#     full = add_column_defaults(copy(COL_1_HINTS))
#     assert full["unique"] is False
#     # remove defaults from full
#     clean = utils.remove_column_defaults(copy(full))
#     assert clean == COL_1_HINTS_DEFAULTS
#     # prop is None and will be removed
#     del full["prop"]  # type: ignore[typeddict-item]
#     assert add_column_defaults(copy(clean)) == full
#     # test incomplete
#     complete_full = add_column_defaults(copy(COL_2_HINTS))
#     # defaults are added
#     assert complete_full["unique"] is False


def test_remove_defaults_stored_schema() -> None:
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "parent": "parent",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }
    stored_schema: TStoredSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "schema",
        "tables": {"table": deepcopy(table), "table_copy": deepcopy(table)},
        "x-top-level": True,
    }
    # mock the case in table_copy where resource == table_name
    stored_schema["tables"]["table_copy"]["resource"] = stored_schema["tables"]["table_copy"][
        "name"
    ] = "table_copy"

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
    assert "resource" in default_stored["tables"]["table_copy"]

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
    incomplete_col["x-special"] = "spec"  # type: ignore[typeddict-unknown-key]
    table = utils.new_table("table", columns=[incomplete_col])
    # incomplete column must be added without hints
    assert table["columns"]["I"]["primary_key"] is True
    assert table["columns"]["I"]["x-special"] == "spec"  # type: ignore[typeddict-item]
    assert "merge_key" not in incomplete_col


def test_merge_column() -> None:
    # tab_b overrides non default
    col_a = utils.merge_column(copy(COL_1_HINTS), copy(COL_2_HINTS), merge_defaults=False)
    # nullable is False - tab_b has it as default and those are not merged
    assert col_a == {
        "name": "test_2",
        "nullable": False,
        "cluster": False,
        "parent_key": True,
        "data_type": "text",
        "x-special": "value",
        "x-special-int": 100,
        "x-special-bool": False,
        "x-special-bool-true": True,
        "prop": None,
    }

    col_a = utils.merge_column(copy(COL_1_HINTS), copy(COL_2_HINTS), merge_defaults=True)
    # nullable is True and primary_key is present - default values are merged
    assert col_a == {
        "name": "test_2",
        "nullable": True,
        "cluster": False,
        "parent_key": True,
        "data_type": "text",
        "x-special": "value",
        "x-special-int": 100,
        "x-special-bool": False,
        "x-special-bool-true": True,
        "prop": None,
        "primary_key": False,
    }


def test_none_resets_on_merge_column() -> None:
    # pops hints with known defaults that are not None (TODO)
    col_a = utils.merge_column(
        col_a={"name": "col1", "primary_key": True}, col_b={"name": "col1", "primary_key": None}
    )
    assert col_a == {"name": "col1", "primary_key": None}

    # leaves props with unknown defaults (assumes None is default)
    col_a = utils.merge_column(
        col_a={"name": "col1", "x-prop": "prop"}, col_b={"name": "col1", "x-prop": None}  # type: ignore[typeddict-unknown-key]
    )
    assert col_a == {"name": "col1", "x-prop": None}


def test_merge_columns() -> None:
    columns = utils.merge_columns({"test": deepcopy(COL_1_HINTS)}, {"test_2": COL_2_HINTS})
    # new columns added ad the end
    assert list(columns.keys()) == ["test", "test_2"]
    assert columns["test"] == COL_1_HINTS
    assert columns["test_2"] == COL_2_HINTS

    # replace test with new test
    columns = utils.merge_columns(
        {"test": deepcopy(COL_1_HINTS)}, {"test": COL_1_HINTS_NO_DEFAULTS}, merge_columns=False
    )
    assert list(columns.keys()) == ["test"]
    assert columns["test"] == COL_1_HINTS_NO_DEFAULTS

    # merge
    columns = utils.merge_columns(
        {"test": deepcopy(COL_1_HINTS)}, {"test": COL_1_HINTS_NO_DEFAULTS}, merge_columns=True
    )
    assert list(columns.keys()) == ["test"]
    assert columns["test"] == utils.merge_column(deepcopy(COL_1_HINTS), COL_1_HINTS_NO_DEFAULTS)


def test_merge_incomplete_columns() -> None:
    incomplete_col_1 = deepcopy(COL_1_HINTS)
    del incomplete_col_1["data_type"]
    incomplete_col_1_nd = deepcopy(COL_1_HINTS_NO_DEFAULTS)
    del incomplete_col_1_nd["data_type"]
    complete_col_2 = deepcopy(COL_2_HINTS)
    complete_col_2["data_type"] = "text"

    # new incomplete added
    columns = utils.merge_columns({"test": deepcopy(incomplete_col_1)}, {"test_2": COL_2_HINTS})
    # new columns added ad the end
    assert list(columns.keys()) == ["test", "test_2"]
    assert columns["test"] == incomplete_col_1
    assert columns["test_2"] == COL_2_HINTS

    # incomplete merged with complete goes at the end
    columns = utils.merge_columns(
        {"test": deepcopy(incomplete_col_1), "test_2": COL_2_HINTS},
        {"test": COL_1_HINTS_NO_DEFAULTS},
    )
    assert list(columns.keys()) == ["test_2", "test"]
    assert columns["test"] == COL_1_HINTS_NO_DEFAULTS
    assert columns["test_2"] == COL_2_HINTS

    columns = utils.merge_columns(
        {"test": deepcopy(incomplete_col_1), "test_2": COL_2_HINTS},
        {"test": COL_1_HINTS_NO_DEFAULTS},
        merge_columns=True,
    )
    assert list(columns.keys()) == ["test_2", "test"]
    assert columns["test"] == utils.merge_column(deepcopy(COL_1_HINTS), COL_1_HINTS_NO_DEFAULTS)

    # incomplete with incomplete
    columns = utils.merge_columns(
        {"test": deepcopy(incomplete_col_1), "test_2": COL_2_HINTS}, {"test": incomplete_col_1_nd}
    )
    assert list(columns.keys()) == ["test", "test_2"]
    assert columns["test"] == incomplete_col_1_nd
    assert columns["test_2"] == COL_2_HINTS


def test_diff_tables() -> None:
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }
    empty = utils.new_table("table")
    del empty["resource"]
    print(empty)
    partial = utils.diff_table("schema", empty, deepcopy(table))
    # partial is simply table
    assert partial == table
    partial = utils.diff_table("schema", deepcopy(table), empty)
    # partial is empty
    assert partial == empty

    # override name and description
    changed = deepcopy(table)
    changed["description"] = "new description"
    changed["name"] = "new name"
    # names must be identical
    renamed_table = deepcopy(table)
    renamed_table["name"] = "new name"
    partial = utils.diff_table("schema", renamed_table, changed)
    print(partial)
    assert partial == {"name": "new name", "description": "new description", "columns": {}}

    # ignore identical table props
    existing = deepcopy(renamed_table)
    changed["write_disposition"] = "append"
    changed["schema_contract"] = "freeze"
    partial = utils.diff_table("schema", deepcopy(existing), changed)
    assert partial == {
        "name": "new name",
        "description": "new description",
        "write_disposition": "append",
        "schema_contract": "freeze",
        "columns": {},
    }
    existing["write_disposition"] = "append"
    existing["schema_contract"] = "freeze"
    partial = utils.diff_table("schema", deepcopy(existing), changed)
    assert partial == {"name": "new name", "description": "new description", "columns": {}}

    # detect changed column
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["cluster"] = True
    partial = utils.diff_table("schema", existing, changed)
    assert "test" in partial["columns"]
    assert "test_2" not in partial["columns"]
    assert existing["columns"]["test"] == table["columns"]["test"] != partial["columns"]["test"]

    # defaults are not ignored
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["parent_key"] = False
    partial = utils.diff_table("schema", existing, changed)
    assert "test" in partial["columns"]

    # even if not present in tab_a at all
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["parent_key"] = False
    del existing["columns"]["test"]["parent_key"]
    partial = utils.diff_table("schema", existing, changed)
    assert "test" in partial["columns"]


def test_diff_tables_conflicts() -> None:
    # conflict on parents
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "parent": "parent",
        "description": "description",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }

    other = utils.new_table("table")
    with pytest.raises(TablePropertiesConflictException) as cf_ex:
        utils.diff_table("schema", table, other)
    assert cf_ex.value.table_name == "table"
    assert cf_ex.value.prop_name == "parent"

    # conflict on name
    other = utils.new_table("other_name")
    with pytest.raises(TablePropertiesConflictException) as cf_ex:
        utils.diff_table("schema", table, other)
    assert cf_ex.value.table_name == "table"
    assert cf_ex.value.prop_name == "name"

    # conflict on data types in columns
    changed = deepcopy(table)
    changed["columns"]["test"]["data_type"] = "bigint"
    with pytest.raises(CannotCoerceColumnException):
        utils.diff_table("schema", table, changed)


def test_merge_tables() -> None:
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }
    print(table)
    changed = deepcopy(table)
    changed["x-special"] = 129  # type: ignore[typeddict-unknown-key]
    changed["description"] = "new description"
    changed["new-prop-1"] = "A"  # type: ignore[typeddict-unknown-key]
    changed["new-prop-2"] = None  # type: ignore[typeddict-unknown-key]
    changed["new-prop-3"] = False  # type: ignore[typeddict-unknown-key]
    # drop column so partial has it
    del table["columns"]["test"]
    partial = utils.merge_table("schema", table, changed)
    assert "test" in table["columns"]
    assert table["x-special"] == 129  # type: ignore[typeddict-item]
    assert table["description"] == "new description"
    assert table["new-prop-1"] == "A"  # type: ignore[typeddict-item]
    # None are not merged in
    assert "new-prop-2" not in table
    assert table["new-prop-3"] is False  # type: ignore[typeddict-item]

    # one column in partial
    assert len(partial["columns"]) == 1
    assert partial["columns"]["test"] == COL_1_HINTS
    # still has incomplete column
    assert table["columns"]["test_2"] == COL_2_HINTS
    # check order, we dropped test so it is added at the end
    print(table)
    assert list(table["columns"].keys()) == ["test_2", "test"]


def test_merge_tables_incomplete_columns() -> None:
    table: TTableSchema = {
        "name": "table",
        "columns": {"test_2": COL_2_HINTS, "test": COL_1_HINTS},
    }
    changed = deepcopy(table)
    # reverse order, this order we want to have at the end
    changed["columns"] = deepcopy({"test": COL_1_HINTS, "test_2": COL_2_HINTS})
    # it is completed now
    changed["columns"]["test_2"]["data_type"] = "bigint"
    partial = utils.merge_table("schema", table, changed)
    assert list(partial["columns"].keys()) == ["test_2"]
    # test_2 goes to the end, it was incomplete in table so it got dropped before update
    assert list(table["columns"].keys()) == ["test", "test_2"]

    table = {
        "name": "table",
        "columns": {"test_2": COL_2_HINTS, "test": COL_1_HINTS},
    }

    changed = deepcopy(table)
    # reverse order, this order we want to have at the end
    changed["columns"] = deepcopy({"test": COL_1_HINTS, "test_2": COL_2_HINTS})
    # still incomplete but changed
    changed["columns"]["test_2"]["nullable"] = False
    partial = utils.merge_table("schema", table, changed)
    assert list(partial["columns"].keys()) == ["test_2"]
    # incomplete -> incomplete stays in place
    assert list(table["columns"].keys()) == ["test_2", "test"]


def test_merge_tables_references() -> None:
    table: TTableSchema = {
        "name": "table",
        "columns": {"test_2": COL_2_HINTS, "test": COL_1_HINTS},
        "references": [
            {
                "columns": ["test"],
                "referenced_table": "other",
                "referenced_columns": ["id"],
            }
        ],
    }
    changed: TTableSchema = deepcopy(table)

    # add new references
    changed["references"].append(  # type: ignore[attr-defined]
        {
            "columns": ["test_2"],
            "referenced_table": "other_2",
            "referenced_columns": ["id"],
        }
    )
    changed["references"].append(  # type: ignore[attr-defined]
        {
            "columns": ["test"],
            "referenced_table": "other_3",
            "referenced_columns": ["id"],
        }
    )

    partial = utils.merge_table("schema", table, changed)

    assert partial["references"] == [
        {
            "columns": ["test"],
            "referenced_table": "other",
            "referenced_columns": ["id"],
        },
        {
            "columns": ["test_2"],
            "referenced_table": "other_2",
            "referenced_columns": ["id"],
        },
        {
            "columns": ["test"],
            "referenced_table": "other_3",
            "referenced_columns": ["id"],
        },
    ]

    # Update existing reference

    table = deepcopy(partial)
    changed = deepcopy(partial)

    changed["references"][1] = {  # type: ignore[index]
        "columns": ["test_3"],
        "referenced_table": "other_2",
        "referenced_columns": ["id"],
    }
    partial = utils.merge_table("schema", partial, changed)

    assert partial["references"] == [
        {
            "columns": ["test"],
            "referenced_table": "other",
            "referenced_columns": ["id"],
        },
        {
            "columns": ["test_3"],
            "referenced_table": "other_2",
            "referenced_columns": ["id"],
        },
        {
            "columns": ["test"],
            "referenced_table": "other_3",
            "referenced_columns": ["id"],
        },
    ]


# def add_column_defaults(column: TColumnSchemaBase) -> TColumnSchema:
#     """Adds default boolean hints to column"""
#     return {
#         **{
#             "nullable": True,
#             "partition": False,
#             "cluster": False,
#             "unique": False,
#             "sort": False,
#             "primary_key": False,
#             "parent_key": False,
#             "root_key": False,
#             "merge_key": False,
#         },
#         **column,
#     }
