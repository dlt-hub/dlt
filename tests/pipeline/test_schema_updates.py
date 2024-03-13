import os

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
        "propagation": {
            "tables": {"items": {"_dlt_id": "_dlt_root_id"}},
            "root": {"_dlt_id": "_dlt_root_id"},
        }
    }

    # root key prevails even if not set
    s = source()
    s.root_key = False
    p.run(s, table_name="items", write_disposition="merge")
    # source schema overwrites normalizer settings so `root` propagation is gone
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "propagation": {"tables": {"items": {"_dlt_id": "_dlt_root_id"}}}
    }

    # set max nesting
    s = source()
    s.max_table_nesting = 5
    p.run(s, table_name="items", write_disposition="merge")
    assert p.default_schema._normalizers_config["json"]["config"] == {
        "propagation": {"tables": {"items": {"_dlt_id": "_dlt_root_id"}}},
        "max_nesting": 5,
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
