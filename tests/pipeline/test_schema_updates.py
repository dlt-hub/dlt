from typing import Union
import os
import pytest

import dlt


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


@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
def test_changing_merge_key_between_runs(key_hint: str) -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name=f"test_changing_{key_hint}_between_runs", destination="dummy")

    # table level key argument in the resource decorator should be authoritative
    # over what's in the columns argument
    @dlt.resource(  # type: ignore[call-overload]
        columns={"other_id": {key_hint: True}}, write_disposition="merge", **{key_hint: ["id"]}
    )
    def my_resource():
        yield {"id": 1, "other_id": 2, "name": "Bob"}

    k = my_resource()

    p.run(k)
    assert p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint) is True
    assert not p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint)

    # changing table level key argument should be authoritative
    # over what exists in the schema (no key merging)
    @dlt.resource(  # type: ignore[no-redef, call-overload]
        write_disposition="merge",
        **{key_hint: ["other_id"]},
    )
    def my_resource():
        yield {"id": 1, "other_id": 2, "name": "Bob"}

    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint) is True

    # changing table level key with apply_hints should be authoritative
    # over what exists in the schema (no key merging)
    my_resource.apply_hints(**{key_hint: "id"})
    p.run(my_resource())
    assert p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint) is True
    assert not p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint)

    my_resource.apply_hints(**{key_hint: "other_id"})
    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint) is True

    # changing table level key with apply_hints should be authoritative
    # over what exists in the schema (no key merging)
    # as well as what's passed to the columns argument in apply_hints
    my_resource.apply_hints(**{key_hint: "id"}, columns={"other_id": {key_hint: True}})
    p.run(my_resource())
    assert p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint) is True
    assert not p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint)

    my_resource.apply_hints(**{key_hint: "other_id"}, columns={"id": {key_hint: True}})
    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint) is True

    # empty value as key hint removes previous keys
    my_resource.apply_hints(**{key_hint: ""})
    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert not p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint)

    @dlt.resource(  # type: ignore[no-redef, call-overload]
        columns={"other_id": {key_hint: True}},
        write_disposition="merge",
        **{key_hint: []},
    )
    def my_resource():
        yield {"id": 1, "other_id": 2, "name": "Bob"}

    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert not p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint)


@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_redef"])
@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
def test_key_replaces_column_hints(key_hint: str, with_apply_hints: bool) -> None:
    """Ensure that key hints on table level take precedence over hints on column level."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_changing_merge_key_between_runs", destination="dummy")

    if with_apply_hints:

        @dlt.resource
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource().apply_hints(
            columns={"other_id": {key_hint: True}}, **{key_hint: ["id"]}  # type: ignore
        )
    else:

        @dlt.resource(columns={"other_id": {key_hint: True}}, **{key_hint: ["id"]})  # type: ignore
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource()

    # Initially hints are set as is: "other_id" receives key hint
    assert my_resource.columns == {"other_id": {key_hint: True, "name": "other_id"}}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints[key_hint] == ["id"]  # type: ignore

    # Table level key hint takes precedence in _merge_keys: "other_id" no longer key
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {
        "other_id": {"name": "other_id"},
        "id": {"name": "id", "nullable": False, key_hint: True},
    }

    p.extract(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"] == table_schema["columns"]


@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_redef"])
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
def test_empty_value_as_key(
    key_hint: str, empty_value: Union[str, None], with_apply_hints: bool
) -> None:
    """Ensure that empty value key hints aren't propagated through the pipeline."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(pipeline_name="test_empty_key_replaces_column_hints", destination="dummy")

    if with_apply_hints:

        @dlt.resource
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource().apply_hints(**{key_hint: empty_value})  # type: ignore
    else:

        @dlt.resource(**{key_hint: empty_value})  # type: ignore[call-overload]
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource()

    # Initially hints are set as is: empty_value as key hint
    assert my_resource.columns == {}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints[key_hint] == empty_value  # type: ignore

    # Empty value key hint is propagated to extract
    # in case table exists and empty value key is used to replace previous hints
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {"": {"name": "", "nullable": False, key_hint: True}}

    # Empty value key hint should not survive extract
    p.extract(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"] == {}


@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_redef"])
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
def test_empty_value_as_key_replace_column_hints(
    key_hint: str, empty_value: Union[str, None], with_apply_hints: bool
) -> None:
    """Ensure that empty value key hints on table level take precedence over hints on column level."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(
        pipeline_name="test_empty_value_as_key_replace_column_hints", destination="dummy"
    )

    if with_apply_hints:

        @dlt.resource
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource().apply_hints(
            columns={"other_id": {key_hint: True}}, **{key_hint: empty_value}  # type: ignore
        )
    else:

        @dlt.resource(columns={"other_id": {key_hint: True}}, **{key_hint: empty_value})  # type: ignore[call-overload]
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource()

    # Initially hints are set as is: "other_id" receives key hint
    assert my_resource.columns == {"other_id": {key_hint: True, "name": "other_id"}}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints[key_hint] == empty_value  # type: ignore

    # Table level key hint takes precedence in _merge_keys: "other_id" no longer key
    # Empty value key hint is propagated to extract
    # in case table exists and empty value key is used to replace previous hints
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {
        "other_id": {"name": "other_id"},
        "": {"name": "", "nullable": False, key_hint: True},
    }

    p.extract(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"] == {
        "other_id": {"name": "other_id"},
    }


@pytest.mark.parametrize("with_apply_hints", [True, False], ids=["apply_hints", "resource_redef"])
@pytest.mark.parametrize("empty_value", ["", []], ids=["empty_string", "empty_list"])
@pytest.mark.parametrize(
    "key_hint",
    ["merge_key", "primary_key"],
)
def test_empty_value_as_key_replaces_previous_key(
    key_hint: str, empty_value: Union[str, None], with_apply_hints: bool
) -> None:
    """Ensure that empty value key hint on an existing resource removes key hints."""
    os.environ["COMPLETED_PROB"] = "1.0"
    p = dlt.pipeline(
        pipeline_name="test_empty_value_as_key_replaces_previous_key", destination="dummy"
    )

    # Initially "id" is set as key
    @dlt.resource(**{key_hint: "id"})  # type: ignore[call-overload]
    def get_resource():
        yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

    my_resource = get_resource()
    assert my_resource.columns == {}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints[key_hint] == "id"

    p.run(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"] == {
        "id": {"name": "id", "nullable": False, key_hint: True, "data_type": "bigint"},
        "other_id": {"name": "other_id", "data_type": "bigint", "nullable": True},
        "name": {"name": "name", "data_type": "text", "nullable": True},
        "age": {"name": "age", "data_type": "bigint", "nullable": True},
        "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
        "_dlt_id": {
            "name": "_dlt_id",
            "data_type": "text",
            "nullable": False,
            "unique": True,
            "row_key": True,
        },
    }

    # We remove the key with empty_value
    if with_apply_hints:
        my_resource.apply_hints(**{key_hint: empty_value})

    else:

        @dlt.resource(**{key_hint: empty_value})  # type: ignore[call-overload]
        def get_resource():
            yield {"id": 1, "other_id": 2, "name": "Bob", "age": 32}

        my_resource = get_resource()

    assert my_resource.columns == {}
    assert my_resource._hints["columns"] == my_resource.columns
    assert my_resource._hints[key_hint] == empty_value

    # Empty value key hint is propagated to extract so that it can be used to replace previous hints
    table_schema = my_resource.compute_table_schema()
    assert table_schema["columns"] == {"": {"name": "", "nullable": False, key_hint: True}}

    p.extract(my_resource)
    assert p.default_schema.tables["get_resource"]["columns"] == {
        "id": {"name": "id", "nullable": False, "data_type": "bigint"},  # key hint removed
        "other_id": {"name": "other_id", "data_type": "bigint", "nullable": True},
        "name": {"name": "name", "data_type": "text", "nullable": True},
        "age": {"name": "age", "data_type": "bigint", "nullable": True},
        "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
        "_dlt_id": {
            "name": "_dlt_id",
            "data_type": "text",
            "nullable": False,
            "unique": True,
            "row_key": True,
        },
    }
