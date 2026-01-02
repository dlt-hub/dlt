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

    # empty value as key hint does nothing
    my_resource.apply_hints(**{key_hint: ""})
    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint) is True

    @dlt.resource(  # type: ignore[no-redef, call-overload]
        write_disposition="merge",
        **{key_hint: []},
    )
    def my_resource():
        yield {"id": 1, "other_id": 2, "name": "Bob"}

    p.run(my_resource())
    assert not p.default_schema.tables["my_resource"]["columns"]["id"].get(key_hint)
    assert p.default_schema.tables["my_resource"]["columns"]["other_id"].get(key_hint) is True
