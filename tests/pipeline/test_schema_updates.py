from typing import Union, Any, Dict
import os
import pytest

import dlt
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

    if item_format == "object":
        item = {"id": 1, "other_id": 2}
    elif item_format == "pandas":
        item = pd.DataFrame({"id": [1], "other_id": [2]})
    elif item_format == "arrow-table":
        item = pa.table({"id": [1], "other_id": [2]})
    else:  # arrow-batch
        item = pa.RecordBatch.from_pydict({"id": [1], "other_id": [2]})

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
    assert not p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint)


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

    if item_format == "object":
        item = {"id": 1, "other_id": 2}
    elif item_format == "pandas":
        item = pd.DataFrame({"id": [1], "other_id": [2]})
    elif item_format == "arrow-table":
        item = pa.table({"id": [1], "other_id": [2]})
    else:  # arrow-batch
        item = pa.RecordBatch.from_pydict({"id": [1], "other_id": [2]})

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

    if item_format == "object":
        item = {"id": 1, "other_id": 2}
    elif item_format == "pandas":
        item = pd.DataFrame({"id": [1], "other_id": [2]})
    elif item_format == "arrow-table":
        item = pa.table({"id": [1], "other_id": [2]})
    else:  # arrow-batch
        item = pa.RecordBatch.from_pydict({"id": [1], "other_id": [2]})

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
def test_new_hints_replace_previous_key(
    key_hint: str,
    with_apply_hints: bool,
    key_hint_as_list: bool,
    item_format: TestDataItemFormat,
) -> None:
    """Show that new key hints on existing resource replace previous ones."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_new_hints_replace_previous_key", destination="dummy")

    # Initially "id" is set as key
    @dlt.resource(**{key_hint: "id"})  # type: ignore[call-overload]
    def get_resource():
        yield {"id": 1, "other_id": 2}

    p.run(get_resource())
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True

    if item_format == "object":
        item = {"id": 1, "other_id": 2}
    elif item_format == "pandas":
        item = pd.DataFrame({"id": [1], "other_id": [2]})
    elif item_format == "arrow-table":
        item = pa.table({"id": [1], "other_id": [2]})
    else:  # arrow-batch
        item = pa.RecordBatch.from_pydict({"id": [1], "other_id": [2]})

    # We change key to "other_id"
    my_resource = _get_resource(
        with_apply_hints,
        item,
        **{key_hint: ["other_id"] if key_hint_as_list else "other_id"},
    )

    # "id" should no longer be key
    p.run(my_resource)
    assert not p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["get_resource"]["columns"]["other_id"].get(key_hint) is True


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
    """Show that empty value key hints on existing resource currently do nothing."""
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

    if item_format == "object":
        item = {"id": 1, "other_id": 2}
    elif item_format == "pandas":
        item = pd.DataFrame({"id": [1], "other_id": [2]})
    elif item_format == "arrow-table":
        item = pa.table({"id": [1], "other_id": [2]})
    else:  # arrow-batch
        item = pa.RecordBatch.from_pydict({"id": [1], "other_id": [2]})

    # We try to remove the key with empty_value
    my_resource = _get_resource(with_apply_hints, item, **{key_hint: empty_value})

    # "id" is still key
    p.run(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"]["id"].get(key_hint) is True


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_def"])
def test_consecutive_column_hints(key_hint: str, with_apply_hints: bool) -> None:
    """Show that consecutive provision of hints on column level via apply_hints accumulates."""

    my_resource = _get_resource(
        with_apply_hints,
        {"id": 1, "other_id": 2},
        columns={"id": {key_hint: True}},
        **{},
    )

    assert my_resource.columns == {"id": {key_hint: True, "name": "id"}}

    my_resource.apply_hints(columns={"other_id": {key_hint: True}})  # type: ignore

    assert my_resource.columns == {
        "id": {key_hint: True, "name": "id"},
        "other_id": {key_hint: True, "name": "other_id"},
    }
