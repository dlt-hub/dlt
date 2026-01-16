from typing import Union, Any, Dict
import os
from pandas._libs import properties
import pytest

import dlt
from dlt.common.schema import Schema
from dlt.common.schema.typing import ColumnPropInfos
from dlt.extract.resource import DltResource
from tests.utils import TestDataItemFormat, ALL_TEST_DATA_ITEM_FORMATS

import pandas as pd
import pyarrow as pa


def test_schema_updates() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"  # make it complete immediately
    p = dlt.pipeline(pipeline_name="test_schema_updates", dev_mode=True, destination="dummy")

    @dlt.source()
    def source():
        @dlt.resource()
        def resource():
            yield [1, 2, 3]

        return resource

    # test without normalizer attributes
    s = source()
    p.run(s, table_name="items", write_disposition="append")
    assert "config" not in p.default_schema._normalizers_config["json"]

    # add table propagation
    s = source()
    p.run(s, table_name="items", write_disposition="merge")
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "propagation": {"tables": {"items": {"_dlt_id": "_dlt_root_id"}}}
    }

    # set root key
    s = source()
    s.root_key = True
    p.run(s, table_name="items", write_disposition="merge")
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "root_key_propagation": True,
        "propagation": {
            "tables": {"items": {"_dlt_id": "_dlt_root_id"}},
        },
    }

    # root key prevails even if not set
    s = source()
    s.root_key = False
    p.run(s, table_name="items", write_disposition="merge")
    # source schema overwrites normalizer settings so `root` propagation is gone
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "root_key_propagation": False,
    }

    # set max nesting
    s = source()
    s.max_table_nesting = 5
    p.run(s, table_name="items", write_disposition="merge")
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "max_nesting": 5,
        "propagation": {
            "tables": {"items": {"_dlt_id": "_dlt_root_id"}},
        },
    }

    # update max nesting and new table
    s = source()
    s.max_table_nesting = 50
    p.run(s, table_name="items2", write_disposition="merge")
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "propagation": {
            "tables": {
                "items": {"_dlt_id": "_dlt_root_id"},
                "items2": {"_dlt_id": "_dlt_root_id"},
            }
        },
        "max_nesting": 50,
    }


def _get_item_with_format(
    item: Dict[str, Any], item_format: TestDataItemFormat
) -> Union[Dict[str, Any], pd.DataFrame, pa.Table, pa.RecordBatch]:
    if item_format == "object":
        return item
    elif item_format == "pandas":
        return pd.DataFrame({k: [v] for k, v in item.items()})
    elif item_format == "arrow-table":
        return pa.table({k: [v] for k, v in item.items()})
    else:  # arrow-batch
        return pa.RecordBatch.from_pydict({k: [v] for k, v in item.items()})


def _get_resource(with_apply_hints: bool, data: Dict[str, Any], **hints: Any) -> DltResource:
    if with_apply_hints:

        @dlt.resource
        def get_resource():
            yield data

        my_resource = get_resource().apply_hints(**hints)
    else:

        @dlt.resource(**hints)
        def get_resource():
            yield data

        my_resource = get_resource()

    return my_resource


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize(
    "key_hint_as_list", [True, False], ids=["key_hint_as_list", "key_hint_as_string"]
)
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_key_replaces_column_hints(
    key_hint: str,
    with_apply_hints: bool,
    key_hint_as_list: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Ensure that key hints on table level take precedence over hints on column level."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_changing_merge_key_between_runs", destination="dummy")

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    my_resource = _get_resource(
        with_apply_hints,
        item,
        columns={"other_id": {key_hint: True}},
        **{key_hint: ["id"] if key_hint_as_list else "id"},
    )

    # Initially hints are set as is:
    # - "other_id" receives key hint on column level
    # - "id" is set as key hint on table level
    assert my_resource.columns == {"other_id": {key_hint: True, "name": "other_id"}}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints.get(key_hint) == ["id"] if key_hint_as_list else "id"

    # Table level key hint takes precedence during schema computation:
    # - "other_id" is not key
    # - "id" is key
    expected = {
        "other_id": {"name": "other_id"},
        "id": {"name": "id", "nullable": False, key_hint: True},
    }
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == expected

    p.run(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True
    assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is False
    assert not p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint)
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"]["nullable"] is True


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_empty_value_as_key(
    key_hint: str,
    empty_value: Union[str, None],
    with_apply_hints: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Show that empty value key hints aren't propagated."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_empty_key_replaces_column_hints", destination="dummy")

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    my_resource = _get_resource(
        with_apply_hints,
        item,
        **{key_hint: empty_value},
    )

    assert my_resource.columns == {}
    assert my_resource._hints["columns"] == my_resource.columns
    assert not my_resource._hints.get(key_hint)

    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {}

    p.run(my_resource)
    assert all(
        not column.get(key_hint)
        for column in p.default_schema.tables["get_resource"]["columns"].values()
    )


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_empty_value_as_key_replace_column_hints(
    key_hint: str,
    empty_value: Union[str, None],
    with_apply_hints: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Ensure that empty value key hints on table level take precedence over hints on column level."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(
        pipeline_name="test_empty_value_as_key_replace_column_hints", destination="dummy"
    )

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    my_resource = _get_resource(
        with_apply_hints,
        item,
        columns={"other_id": {key_hint: True}},
        **{key_hint: empty_value},
    )

    # Initially hints are set as is:
    # - "other_id" receives key hint on column level
    # - empty value is set as key hint on table level
    assert my_resource.columns == {"other_id": {key_hint: True, "name": "other_id"}}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints.get(key_hint) == empty_value

    # Table level key hint takes precedence during schema computation:
    # - "other_id" is not key
    # - empty value is key, meaning no keys
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {"other_id": {"name": "other_id"}}

    p.run(my_resource)
    assert not p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint)
    assert not p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint)


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize(
    "key_hint_as_list", [True, False], ids=["key_hint_as_list", "key_hint_as_string"]
)
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_new_key_hints_replace_previous_keys(
    key_hint: str,
    with_apply_hints: bool,
    key_hint_as_list: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Show that new key hints on existing resource replace previous ones."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_new_key_hints_replace_previous_keys", destination="dummy")

    # Initially "id" is set as key
    @dlt.resource(**{key_hint: "id"})  # type: ignore[call-overload]
    def get_resource():
        yield {"id": 1, "other_id": 2}

    p.run(get_resource())
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True
    assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is False

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    # We change key to "other_id"
    my_resource = _get_resource(
        with_apply_hints,
        item,
        **{key_hint: ["other_id"] if key_hint_as_list else "other_id"},
    )
    p.run(my_resource)

    # "id" should no longer be key
    assert not p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint)
    # NOTE: Due to the following two reasons:
    #   1. Removal of compound does not automatically resets nullable to True.
    #   2. Arrow formats re-infer nullable from the data schema, while the default json
    #      format preserves the nullable setting from the schema
    # There's inconsistent behavior: json format objects retain nullable=False
    # (orphaned NOT NULL constraint from when it was a key), whereas Arrow/pandas
    # formats have nullable=True (re-inferred from data).
    if item_format == "object":
        assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is False
    else:
        assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is True
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint) is True
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"]["nullable"] is False


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_explicit_schema(
    key_hint: str,
    with_apply_hints: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Test that resource-level key hints override explicit schema definitions.

    When a schema explicitly sets "id" as key, but the resource defines "other_id"
    as key, the resource hint should win and properly update the schema.
    """
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_new_key_hints_replace_previous_keys", destination="dummy")

    schema = Schema("test_schema")
    schema.update_table(
        {
            "name": "get_resource",
            "columns": {
                "id": {"data_type": "bigint", "name": "id", key_hint: True, "nullable": False}  # type: ignore[misc]
            },
        }
    )

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)
    my_resource = _get_resource(
        with_apply_hints,
        item,
        **{key_hint: "other_id"},
    )

    @dlt.source(schema=schema)
    def my_source():
        return my_resource

    # NOTE: Due to the following two reasons:
    #   1. Removal of compound does not automatically resets nullable to True.
    #   2. Arrow formats re-infer nullable from the data schema, while the default json
    #      format preserves the nullable setting from the schema
    # There's inconsistent behavior: json format objects retain nullable=False
    # (orphaned NOT NULL constraint from when it was a key), whereas Arrow/pandas
    # formats have nullable=True (re-inferred from data).
    p.run(my_source())
    assert not p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint)
    if item_format == "object":
        assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is False
    else:
        assert p.default_schema.tables["get_resource"]["columns"]["id"]["nullable"] is True
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint) is True
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"]["nullable"] is False


@pytest.mark.parametrize(
    "compound_prop",
    [
        prop
        for prop, info in ColumnPropInfos.items()
        if info.compound
        if prop not in ["primary_key", "merge_key"]
    ],
)
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_new_compound_prop_hints_replace_previous_compound_props(
    compound_prop: str,
    item_format: TestDataItemFormat,
) -> None:
    """Show what new compound prop hints on existing resource replace previous ones."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(
        pipeline_name="test_new_compound_prop_hints_replace_previous_compound_props",
        destination="dummy",
    )

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    # Initially "id" and "other_id" are set with compound_prop
    my_resource = _get_resource(
        with_apply_hints=False,
        data=item,
        columns={"other_id": {compound_prop: True}, "id": {compound_prop: True}},
    )

    p.run(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(compound_prop) is True
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"].get(compound_prop) is True

    # We change to "other_id" only
    my_resource = _get_resource(
        with_apply_hints=False,
        data=item,
        columns={"other_id": {compound_prop: True}},
    )
    p.run(my_resource)
    # "id" should no longer be key
    assert not p.default_schema.tables["get_resource"]["columns"]["id"].get(compound_prop)
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"].get(compound_prop) is True


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
@pytest.mark.parametrize("item_format", ALL_TEST_DATA_ITEM_FORMATS)
def test_empty_value_as_key_does_not_replace_previous_key(
    key_hint: str,
    empty_value: Union[str, None],
    with_apply_hints: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Show that empty value key hints on existing resource CURRENTLY do nothing."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(
        pipeline_name="test_empty_value_as_key_does_not_replaces_previous_key", destination="dummy"
    )

    # Initially "id" is set as key
    @dlt.resource(**{key_hint: "id"})  # type: ignore[call-overload]
    def get_resource():
        yield {"id": 1, "other_id": 2}

    p.run(get_resource())
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True

    item = _get_item_with_format({"id": 1, "other_id": 2}, item_format)

    # We try to remove the key with empty_value
    my_resource = _get_resource(with_apply_hints, item, **{key_hint: empty_value})

    # "id" is still key
    p.run(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True


@pytest.mark.parametrize(
    "compound_prop",
    [prop for prop, info in ColumnPropInfos.items() if info.compound],
)
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
def test_consecutive_column_hints(compound_prop: str, with_apply_hints: bool) -> None:
    """Show that consecutive provision of compound property hints on column level via apply_hints accumulates."""

    my_resource = _get_resource(
        with_apply_hints,
        {"id": 1, "other_id": 2},
        columns={"id": {compound_prop: True}},
        **{},
    )

    assert my_resource.columns == {"id": {compound_prop: True, "name": "id"}}

    my_resource.apply_hints(columns={"other_id": {compound_prop: True}})  # type: ignore

    assert my_resource.columns == {
        "id": {compound_prop: True, "name": "id"},
        "other_id": {compound_prop: True, "name": "other_id"},
    }
