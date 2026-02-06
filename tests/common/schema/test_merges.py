from typing import Any
import pytest
from copy import copy, deepcopy

from dlt.common.schema import utils
from dlt.common.schema.exceptions import (
    CannotCoerceColumnException,
    TablePropertiesConflictException,
)
from dlt.common.schema.typing import (
    TColumnSchemaBase,
    TStoredSchema,
    TTableSchema,
    TColumnSchema,
    ColumnPropInfos,
    TTableSchemaColumns,
    TTableReferenceInline,
)


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


COL_3_HINTS: TColumnSchema = {
    "name": "test_3",
    "merge_key": True,
    "nullable": False,
}

COL_4_HINTS: TColumnSchema = {
    "name": "test_4",
    "merge_key": True,
    "nullable": False,
}


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
    # pops hints with known defaults that are not None
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

    columns = utils.merge_columns(
        {"test": deepcopy(COL_1_HINTS)}, {"test": COL_1_HINTS_NO_DEFAULTS}
    )
    assert list(columns.keys()) == ["test"]
    assert columns["test"] == utils.merge_column(deepcopy(COL_1_HINTS), COL_1_HINTS_NO_DEFAULTS)


@pytest.mark.parametrize(
    "prop",
    [prop for prop, info in ColumnPropInfos.items() if info.compound] + ["unknown", "data_type"],
)
def test_is_compound_prop(prop: str) -> None:
    """Test the functionality of is_compound_prop"""
    is_compound = utils.is_compound_prop(prop)
    if prop in ["unknown", "data_type"]:
        assert is_compound is False
    else:
        assert is_compound is True


def test_remove_compound_props() -> None:
    """Test the removal of compound props."""

    columns: TTableSchemaColumns = {
        "col1": deepcopy(COL_1_HINTS),
        "col3": deepcopy(COL_3_HINTS),
        "col4": deepcopy(COL_4_HINTS),
    }

    result = utils.remove_compound_props(columns, {"cluster", "merge_key", "data_type"})

    # ensure specified properties are removed from all columns
    assert all("cluster" not in col_schema for col_schema in result.values())
    assert all("merge_key" not in col_schema for col_schema in result.values())

    # in col3 and col4, nullable should not be reset
    assert columns["col3"]["nullable"] is False
    assert columns["col4"]["nullable"] is False

    # the function is a generic property remover, validation of whether
    # properties are actually compound should be handled upstream
    assert all("data_type" not in col_schema for col_schema in result.values())

    # it modifies in place (returns same object)
    assert result is columns


@pytest.mark.parametrize(
    "prop",
    [prop for prop, info in ColumnPropInfos.items() if info.compound] + ["unknown", "data_type"],
)
def test_collect_and_remove_compound_props(prop: str) -> None:
    """Test collection and removal of compound props."""

    col_1: TTableSchemaColumns = {"test_1": {"name": "test_1", prop: True}}  # type: ignore[misc]
    col_2: TTableSchemaColumns = {"test_2": {"name": "test_2", prop: True}}  # type: ignore[misc]
    utils._collect_and_remove_compound_props(col_1, col_2)
    if prop in ["unknown", "data_type"]:
        # "unknown" is not a recognized prop, "data_type" is not compound
        # both should be ignored by the helper
        assert col_2["test_2"].get(prop) is True
    else:
        assert not col_2["test_2"].get(prop)

    col_3: TTableSchemaColumns = {"test_3": {"name": "test_3", prop: False}}  # type: ignore[misc]
    col_4: TTableSchemaColumns = {"test_4": {"name": "test_4", prop: True}}  # type: ignore[misc]
    utils._collect_and_remove_compound_props(col_3, col_4)
    assert col_4["test_4"].get(prop) is True

    col_5: TTableSchemaColumns = {"test_5": {"name": "test_5", prop: None}}  # type: ignore[misc]
    col_6: TTableSchemaColumns = {"test_6": {"name": "test_6", prop: True}}  # type: ignore[misc]
    utils._collect_and_remove_compound_props(col_5, col_6)
    assert col_6["test_6"].get(prop) is True


@pytest.mark.parametrize(
    "merge_compound_props",
    [True, False],
    ids=["merge_compound_props", "replace_compound_props"],
)
def test_merge_columns_compound_props(merge_compound_props: bool) -> None:
    """Test that compound props are replaced if the config is set so in merge_columns."""

    compound_props = {prop for prop, info in ColumnPropInfos.items() if info.compound}
    assert compound_props == {"merge_key", "primary_key", "cluster", "partition", "incremental"}

    columns_a: TTableSchemaColumns = {
        "col1": {"name": "col1", **{prop: True for prop in compound_props}},  # type: ignore[typeddict-item]
    }

    columns_b: TTableSchemaColumns = {
        "col2": {"name": "col2", **{prop: True for prop in compound_props}},  # type: ignore[typeddict-item]
    }

    result = utils.merge_columns(
        deepcopy(columns_a), columns_b, merge_compound_props=merge_compound_props
    )

    if merge_compound_props:
        for prop in compound_props:
            assert result["col1"].get(prop) is True
            assert result["col2"].get(prop) is True

    else:
        for prop in compound_props:
            assert result["col1"].get(prop) is None
            assert result["col2"].get(prop) is True


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
    assert columns["test"] == COL_1_HINTS
    assert columns["test_2"] == COL_2_HINTS

    columns = utils.merge_columns(
        {"test": deepcopy(incomplete_col_1), "test_2": COL_2_HINTS},
        {"test": COL_1_HINTS_NO_DEFAULTS},
    )
    assert list(columns.keys()) == ["test_2", "test"]
    assert columns["test"] == utils.merge_column(deepcopy(COL_1_HINTS), COL_1_HINTS_NO_DEFAULTS)

    # incomplete with incomplete
    columns = utils.merge_columns(
        {"test": deepcopy(incomplete_col_1), "test_2": COL_2_HINTS}, {"test": incomplete_col_1_nd}
    )
    assert list(columns.keys()) == ["test", "test_2"]
    assert columns["test"] == incomplete_col_1
    assert columns["test_2"] == COL_2_HINTS


@pytest.mark.parametrize(
    "merge_compound_props",
    [True, False],
    ids=["merge_compound_props", "replace_compound_props"],
)
def test_merge_tables_all_cases(merge_compound_props: bool) -> None:
    """Tests merge_table across all typical scenarios: new columns, changed properties,
    compound property handling."""
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "description": "description",
        "resource": "🦚Table",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }

    # merging full table into empty table adds all columns and properties
    empty = utils.new_table("table")
    del empty["resource"]
    utils.merge_table("schema", empty, deepcopy(table), merge_compound_props)
    assert empty["columns"]["test"] == COL_1_HINTS
    assert empty["columns"]["test_2"] == COL_2_HINTS
    assert empty["description"] == "description"
    assert empty["x-special"] == 128  # type: ignore[typeddict-item]

    # merging empty table into full table preserves existing columns
    existing = deepcopy(table)
    empty = utils.new_table("table")
    del empty["resource"]
    utils.merge_table("schema", existing, empty, merge_compound_props)
    assert existing["columns"] == table["columns"]

    # override description
    existing = deepcopy(table)
    existing["name"] = "new name"
    changed = deepcopy(table)
    changed["description"] = "new description"
    changed["name"] = "new name"
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["description"] == "new description"

    # table props are merged, identical props unchanged
    existing = deepcopy(table)
    existing["name"] = "new name"
    changed = deepcopy(table)
    changed["name"] = "new name"
    changed["write_disposition"] = "append"
    changed["schema_contract"] = "freeze"
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["write_disposition"] == "append"
    assert existing["schema_contract"] == "freeze"

    # detect changed column - column property is updated in place
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["cluster"] = True
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["columns"]["test"]["cluster"] is True
    # other column is unchanged
    assert existing["columns"]["test_2"] == table["columns"]["test_2"]

    # defaults are applied
    existing = deepcopy(table)
    changed = deepcopy(table)
    changed["columns"]["test"]["parent_key"] = False
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["columns"]["test"]["parent_key"] is False

    # even if not present in existing at all
    existing = deepcopy(table)
    del existing["columns"]["test"]["parent_key"]
    changed = deepcopy(table)
    changed["columns"]["test"]["parent_key"] = False
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["columns"]["test"]["parent_key"] is False

    # compound property replacement: test has pk in existing, test_2 gets pk in changed
    existing = deepcopy(table)
    existing["columns"]["test"]["primary_key"] = True
    changed = deepcopy(table)
    changed["columns"]["test_2"]["primary_key"] = True
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["columns"]["test_2"]["primary_key"] is True
    if merge_compound_props:
        # test keeps primary_key since compound props are additive
        assert existing["columns"]["test"]["primary_key"] is True
    else:
        # test: primary_key removed, test_2 is now the sole primary key
        assert existing["columns"]["test"].get("primary_key", False) is False

    # both columns have pk in existing, only test_2 in changed
    existing = deepcopy(table)
    existing["columns"]["test"]["primary_key"] = True
    existing["columns"]["test_2"]["primary_key"] = True
    changed = deepcopy(table)
    changed["columns"]["test_2"]["primary_key"] = True
    utils.merge_table("schema", existing, changed, merge_compound_props)
    assert existing["columns"]["test_2"]["primary_key"] is True
    if merge_compound_props:
        # both keep primary_key
        assert existing["columns"]["test"]["primary_key"] is True
    else:
        # test: primary_key removed (not in changed)
        assert existing["columns"]["test"].get("primary_key", False) is False


def test_merge_table_compound_prop_removal() -> None:
    """When merge_compound_props=False, a compound prop present in existing but absent
    from the column in changed must be removed from existing."""
    existing: TTableSchema = {
        "name": "table",
        "columns": {
            "col_a": {"name": "col_a", "data_type": "text", "primary_key": True},
            "col_b": {"name": "col_b", "data_type": "text", "primary_key": True},
        },
    }
    changed: TTableSchema = {
        "name": "table",
        "columns": {
            "col_a": {"name": "col_a", "data_type": "text"},
            "col_b": {"name": "col_b", "data_type": "text", "primary_key": True},
        },
    }

    utils.merge_table("schema", existing, changed, merge_compound_props=False)
    # col_a: primary_key removed
    assert existing["columns"]["col_a"].get("primary_key", False) is False
    # col_b: primary_key preserved
    assert existing["columns"]["col_b"]["primary_key"] is True

    # sanity: with merge compound props, removal doesn't happen
    existing2: TTableSchema = {
        "name": "table",
        "columns": {
            "col_a": {"name": "col_a", "data_type": "text", "primary_key": True},
            "col_b": {"name": "col_b", "data_type": "text", "primary_key": True},
        },
    }
    utils.merge_table("schema", existing2, deepcopy(changed), merge_compound_props=True)
    assert existing2["columns"]["col_a"]["primary_key"] is True
    assert existing2["columns"]["col_b"]["primary_key"] is True


def test_merge_table_compound_prop_partial_overlap() -> None:
    """When existing has multiple compound props on a column and changed only keeps
    some of them, the merge must remove the dropped ones."""
    existing: TTableSchema = {
        "name": "table",
        "columns": {
            "col": {
                "name": "col",
                "data_type": "text",
                "primary_key": True,
                "merge_key": True,
            },
            "col2": {
                "name": "col2",
                "data_type": "bigint",
                "primary_key": True,
                "merge_key": True,
            },
        },
    }
    # changed: col keeps merge_key but drops primary_key,
    # col2 keeps primary_key but drops merge_key
    changed: TTableSchema = {
        "name": "table",
        "columns": {
            "col": {"name": "col", "data_type": "text", "merge_key": True},
            "col2": {"name": "col2", "data_type": "bigint", "primary_key": True},
        },
    }

    utils.merge_table("schema", existing, changed, merge_compound_props=False)
    # col: primary_key removed, merge_key preserved
    assert existing["columns"]["col"].get("merge_key") is True
    assert existing["columns"]["col"].get("primary_key", False) is False
    # col2: merge_key removed, primary_key preserved
    assert existing["columns"]["col2"].get("primary_key") is True
    assert existing["columns"]["col2"].get("merge_key", False) is False


def test_merge_table_compound_removal_with_non_compound_change() -> None:
    """A column that has a compound prop removed and a non-compound prop changed
    must have both changes reflected after merge."""
    existing: TTableSchema = {
        "name": "table",
        "columns": {
            "col": {
                "name": "col",
                "data_type": "text",
                "nullable": True,
                "primary_key": True,
            },
            "col2": {
                "name": "col2",
                "data_type": "bigint",
                "primary_key": True,
            },
        },
    }
    # changed: col drops primary_key AND changes nullable; col2 keeps primary_key
    changed: TTableSchema = {
        "name": "table",
        "columns": {
            "col": {"name": "col", "data_type": "text", "nullable": False},
            "col2": {"name": "col2", "data_type": "bigint", "primary_key": True},
        },
    }

    utils.merge_table("schema", existing, changed, merge_compound_props=False)
    # compound prop removed
    assert existing["columns"]["col"].get("primary_key", False) is False
    # non-compound prop changed
    assert existing["columns"]["col"]["nullable"] is False
    # col2 unchanged
    assert existing["columns"]["col2"]["primary_key"] is True


def test_tables_conflicts() -> None:
    # conflict on parents
    table: TTableSchema = {  # type: ignore[typeddict-unknown-key]
        "name": "table",
        "parent": "parent",
        "description": "description",
        "x-special": 128,
        "columns": {"test": COL_1_HINTS, "test_2": COL_2_HINTS},
    }

    other = utils.new_table("table")
    with pytest.raises(TablePropertiesConflictException):
        utils.merge_table("schema", table, other)
    with pytest.raises(TablePropertiesConflictException) as cf_ex:
        utils.ensure_compatible_tables("schema", table, other)
    assert cf_ex.value.table_name == "table"
    assert cf_ex.value.prop_name == "parent"

    # conflict on name
    other = utils.new_table("other_name")
    with pytest.raises(TablePropertiesConflictException):
        utils.merge_table("schema", table, other)
    with pytest.raises(TablePropertiesConflictException) as cf_ex:
        utils.ensure_compatible_tables("schema", table, other)
    assert cf_ex.value.table_name == "table"
    assert cf_ex.value.prop_name == "name"

    # conflict on data types in columns
    changed = deepcopy(table)
    changed["columns"]["test"]["data_type"] = "bigint"
    with pytest.raises(CannotCoerceColumnException):
        utils.ensure_compatible_tables("schema", table, changed)
    # merge_table accepts different data types (ensure_columns=False)
    existing = deepcopy(table)
    utils.merge_table("schema", existing, changed)
    assert existing["columns"]["test"]["data_type"] == "bigint"


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

    # partial is the full incoming table (not a diff)
    assert "test" in partial["columns"]
    assert "test_2" in partial["columns"]
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
    # partial is the full incoming table
    assert "test" in partial["columns"]
    assert "test_2" in partial["columns"]
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
    # partial is the full incoming table
    assert "test" in partial["columns"]
    assert "test_2" in partial["columns"]
    # incomplete -> incomplete stays in place
    assert list(table["columns"].keys()) == ["test_2", "test"]


def test_diff_table_references() -> None:
    """Test diff_table_references computes correct diff."""
    ref_a: TTableReferenceInline = {
        "columns": ["a"],
        "referenced_table": "t1",
        "referenced_columns": ["id"],
    }
    ref_b: TTableReferenceInline = {
        "columns": ["b"],
        "referenced_table": "t2",
        "referenced_columns": ["id"],
    }
    ref_c: TTableReferenceInline = {
        "columns": ["c"],
        "referenced_table": "t3",
        "referenced_columns": ["id"],
    }
    ref_a_modified: TTableReferenceInline = {
        "columns": ["a_new"],
        "referenced_table": "t1",
        "referenced_columns": ["id"],
    }

    # new ref in b
    diff = utils.diff_table_references([ref_a], [ref_a, ref_b])
    assert diff == [ref_b]

    # changed ref in b (same referenced_table, different columns)
    diff = utils.diff_table_references([ref_a], [ref_a_modified])
    assert diff == [ref_a_modified]

    # no changes
    diff = utils.diff_table_references([ref_a, ref_b], [ref_a, ref_b])
    assert diff == []

    # empty a, all refs in b are new
    diff = utils.diff_table_references([], [ref_a, ref_b])
    assert diff == [ref_a, ref_b]

    # b is empty, no new refs
    diff = utils.diff_table_references([ref_a, ref_b], [])
    assert diff == []

    # mixed: one new, one changed, one unchanged
    diff = utils.diff_table_references([ref_a, ref_b], [ref_a_modified, ref_b, ref_c])
    assert len(diff) == 2
    assert ref_a_modified in diff
    assert ref_c in diff


def test_merge_table_references_additive() -> None:
    """Test that merge_table merges references additively (not replaces)."""
    # table has refs to t1 and t2
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["col"], "referenced_table": "t1", "referenced_columns": ["id"]},
            {"columns": ["col"], "referenced_table": "t2", "referenced_columns": ["id"]},
        ],
    }
    # partial_table only mentions t3 (a new ref)
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["col"], "referenced_table": "t3", "referenced_columns": ["id"]},
        ],
    }

    utils.merge_table("schema", table, partial)

    # table should have all three refs (t1, t2 preserved, t3 added)
    assert len(table["references"]) == 3
    ref_tables = {r["referenced_table"] for r in table["references"]}
    assert ref_tables == {"t1", "t2", "t3"}


def test_merge_table_references_update() -> None:
    """Test that merge_table updates existing refs by referenced_table."""
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["old_col"], "referenced_table": "t1", "referenced_columns": ["id"]},
        ],
    }
    # partial updates the ref to t1 with different columns
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["new_col"], "referenced_table": "t1", "referenced_columns": ["id"]},
        ],
    }

    utils.merge_table("schema", table, partial)

    # t1 ref should be updated
    assert len(table["references"]) == 1
    assert table["references"][0]["columns"] == ["new_col"]


def test_merge_table_references_mixed() -> None:
    """Test merge with new refs, updated refs, and preserved refs."""
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["a"], "referenced_table": "t1", "referenced_columns": ["id"]},
            {"columns": ["b"], "referenced_table": "t2", "referenced_columns": ["id"]},
        ],
    }
    # partial: update t1, add t3, don't mention t2
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["a_new"], "referenced_table": "t1", "referenced_columns": ["id"]},
            {"columns": ["c"], "referenced_table": "t3", "referenced_columns": ["id"]},
        ],
    }

    utils.merge_table("schema", table, partial)

    assert len(table["references"]) == 3
    refs_by_table = {r["referenced_table"]: r for r in table["references"]}
    # t1 updated
    assert refs_by_table["t1"]["columns"] == ["a_new"]
    # t2 preserved
    assert refs_by_table["t2"]["columns"] == ["b"]
    # t3 added
    assert refs_by_table["t3"]["columns"] == ["c"]


def test_merge_table_no_references_in_partial() -> None:
    """Test that existing refs are preserved when partial has no references."""
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "references": [
            {"columns": ["a"], "referenced_table": "t1", "referenced_columns": ["id"]},
        ],
    }
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
    }

    utils.merge_table("schema", table, partial)

    # refs should be preserved
    assert len(table["references"]) == 1
    assert table["references"][0]["referenced_table"] == "t1"


def test_merge_table_x_hints_replaced() -> None:
    """Test that x- table hints are replaced, not merged (real adapter examples)."""
    # x-iceberg-partition: list of partition specs - should be replaced entirely
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-iceberg-partition": [  # type: ignore[typeddict-unknown-key]
            {"transform": "identity", "source_column": "old_col"},
            {"transform": "year", "source_column": "timestamp"},
        ],
    }
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-iceberg-partition": [  # type: ignore[typeddict-unknown-key]
            {"transform": "month", "source_column": "new_col"},
        ],
    }

    utils.merge_table("schema", table, partial)

    # iceberg partition should be fully replaced, not merged
    assert table["x-iceberg-partition"] == [  # type: ignore[typeddict-item]
        {"transform": "month", "source_column": "new_col"}
    ]

    # x-databricks-table-properties: dict - should be replaced entirely
    table = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-databricks-table-properties": {"prop1": "value1", "prop2": "value2"},  # type: ignore[typeddict-unknown-key]
    }
    partial = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-databricks-table-properties": {"prop3": "value3"},  # type: ignore[typeddict-unknown-key]
    }

    utils.merge_table("schema", table, partial)

    # properties dict should be replaced, not merged
    assert table["x-databricks-table-properties"] == {"prop3": "value3"}  # type: ignore[typeddict-item]

    # x-bigquery-table-description: scalar - should be replaced
    table = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-bigquery-table-description": "old description",  # type: ignore[typeddict-unknown-key]
    }
    partial = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-bigquery-table-description": "new description",  # type: ignore[typeddict-unknown-key]
    }

    utils.merge_table("schema", table, partial)

    assert table["x-bigquery-table-description"] == "new description"  # type: ignore[typeddict-item]


def test_merge_table_x_hints_preserved_when_not_in_partial() -> None:
    """Test that existing x- hints are preserved when partial doesn't mention them."""
    table: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        "x-iceberg-partition": [{"transform": "identity", "source_column": "col"}],  # type: ignore[typeddict-unknown-key]
        "x-bigquery-table-description": "my description",
    }
    partial: TTableSchema = {
        "name": "table",
        "columns": {"col": {"name": "col", "data_type": "text"}},
        # no x- hints
    }

    utils.merge_table("schema", table, partial)

    # both x- hints should be preserved
    assert table["x-iceberg-partition"] == [  # type: ignore[typeddict-item]
        {"transform": "identity", "source_column": "col"}
    ]
    assert table["x-bigquery-table-description"] == "my description"  # type: ignore[typeddict-item]


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
