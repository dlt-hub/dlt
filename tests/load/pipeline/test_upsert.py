import pytest

import dlt

from tests.pipeline.utils import assert_load_info, load_tables_to_dicts
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    load_table_counts,
    assert_records_as_set,
)


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_upsert_core(destination_config: DestinationTestConfiguration) -> None:
    def get_table(pipeline: dlt.Pipeline, table_name: str):
        dicts = load_tables_to_dicts(pipeline, table_name)[table_name]
        return [{k: v for k, v in d.items() if not k.startswith("_dlt")} for d in dicts]

    p = destination_config.setup_pipeline("upsert", full_refresh=True)

    @dlt.resource(
        table_name="upsert",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id",
    )
    def r(data):
        yield data

    l1 = [  # initial load
        {
            "id": 1,
            "foo": "foo",
            "child1": [1, 2],
            "child2": [{"grandchild1": 1, "grandchild2": [1, 2]}],
            "nested": {"foo": 1},
        },
        {
            "id": 2,
            "foo": "bar",
            "child1": [3],
            "child2": [{"grandchild1": 2, "grandchild2": [3]}],
            "nested": {"foo": 2},
        },
    ]
    info = p.run(r(l1))
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "upsert"),
        [{"id": 1, "foo": "foo", "nested__foo": 1}, {"id": 2, "foo": "bar", "nested__foo": 2}],
    )
    assert_records_as_set(
        get_table(p, "upsert__child1"), [{"value": 1}, {"value": 2}, {"value": 3}]
    )
    assert_records_as_set(get_table(p, "upsert__child2"), [{"grandchild1": 1}, {"grandchild1": 2}])
    assert_records_as_set(
        get_table(p, "upsert__child2__grandchild2"), [{"value": 1}, {"value": 2}, {"value": 3}]
    )
    assert load_table_counts(
        p, "upsert", "upsert__child1", "upsert__child2", "upsert__child2__grandchild2"
    ) == {"upsert": 2, "upsert__child1": 3, "upsert__child2": 2, "upsert__child2__grandchild2": 3}

    l2 = [  # insert new primary key value
        {
            "id": 1,
            "foo": "foo",
            "child1": [1, 2],
            "child2": [{"grandchild1": 1, "grandchild2": [1, 2]}],
            "nested": {"foo": 1},
        },
        {
            "id": 3,
            "foo": "baz",
            "child1": [4],
            "child2": [{"grandchild1": 3, "grandchild2": [4]}],
            "nested": {"foo": 3},
        },
    ]
    info = p.run(r(l2))
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "upsert"),
        [
            {"id": 1, "foo": "foo", "nested__foo": 1},
            {"id": 2, "foo": "bar", "nested__foo": 2},
            {"id": 3, "foo": "baz", "nested__foo": 3},
        ],
    )
    assert_records_as_set(
        get_table(p, "upsert__child1"), [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}]
    )
    assert_records_as_set(
        get_table(p, "upsert__child2"), [{"grandchild1": 1}, {"grandchild1": 2}, {"grandchild1": 3}]
    )
    assert_records_as_set(
        get_table(p, "upsert__child2__grandchild2"),
        [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}],
    )
    assert load_table_counts(
        p, "upsert", "upsert__child1", "upsert__child2", "upsert__child2__grandchild2"
    ) == {"upsert": 3, "upsert__child1": 4, "upsert__child2": 3, "upsert__child2__grandchild2": 4}

    l3 = [  # update existing record — change not in child table
        {
            "id": 3,
            "foo": "baz_updated",  # column `foo` changed
            "child1": [4],
            "child2": [{"grandchild1": 3, "grandchild2": [4]}],
            "nested": {"foo": 3},
        }
    ]
    info = p.run(r(l3))
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "upsert"),
        [
            {"id": 1, "foo": "foo", "nested__foo": 1},
            {"id": 2, "foo": "bar", "nested__foo": 2},
            {"id": 3, "foo": "baz_updated", "nested__foo": 3},  # column `foo` changed
        ],
    )
    assert load_table_counts(
        p, "upsert", "upsert__child1", "upsert__child2", "upsert__child2__grandchild2"
    ) == {"upsert": 3, "upsert__child1": 4, "upsert__child2": 3, "upsert__child2__grandchild2": 4}

    l4 = [  # update existing record — change in child table
        {
            "id": 3,
            "foo": "baz_updated",
            "child1": [4, 5],  # 5 added to list
            "child2": [{"grandchild1": 3, "grandchild2": [4]}],
            "nested": {"foo": 3},
        }
    ]
    info = p.run(r(l4))
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "upsert__child1"),
        [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}, {"value": 5}],  # new record for 5
    )
    assert load_table_counts(
        p, "upsert", "upsert__child1", "upsert__child2", "upsert__child2__grandchild2"
    ) == {"upsert": 3, "upsert__child1": 5, "upsert__child2": 3, "upsert__child2__grandchild2": 4}

    l5 = [  # update existing record — change in child table
        {
            "id": 3,
            "foo": "baz_updated",
            "child1": [4],  # 5 removed from list
            "child2": [{"grandchild1": 3, "grandchild2": [4]}],
            "nested": {"foo": 3},
        }
    ]
    info = p.run(r(l5))
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "upsert"),
        [
            {"id": 1, "foo": "foo", "nested__foo": 1},
            {"id": 2, "foo": "bar", "nested__foo": 2},
            {"id": 3, "foo": "baz_updated", "nested__foo": 3},
        ],
    )
    assert_records_as_set(
        get_table(p, "upsert__child1"), [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}]
    )
    assert_records_as_set(
        get_table(p, "upsert__child2"), [{"grandchild1": 1}, {"grandchild1": 2}, {"grandchild1": 3}]
    )
    assert_records_as_set(
        get_table(p, "upsert__child2__grandchild2"),
        [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}],  # 5 is no longer here
    )
    assert load_table_counts(
        p, "upsert", "upsert__child1", "upsert__child2", "upsert__child2__grandchild2"
    ) == {"upsert": 3, "upsert__child1": 4, "upsert__child2": 3, "upsert__child2__grandchild2": 4}
