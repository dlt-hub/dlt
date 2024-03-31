# timezone is removed from all datetime objects in these tests to simplify comparison

import pytest
from typing import List, Dict, Any
from datetime import datetime, timezone  # noqa: I251

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.destinations.sql_jobs import HIGH_TS

from tests.pipeline.utils import assert_load_info
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    load_tables_to_dicts,
)


ACTIVE_TS = datetime.fromisoformat(HIGH_TS.isoformat()).replace(tzinfo=None)
h = DataItemNormalizer.get_row_hash


def get_load_package_created_at(pipeline: dlt.Pipeline, load_info: LoadInfo) -> datetime:
    """Returns `created_at` property of load package state."""
    load_id = load_info.asdict()["loads_ids"][0]
    return datetime.strptime(
        pipeline.get_load_package_state(load_id)["created_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
    ).replace(tzinfo=None)


def strip_timezone(ts: datetime) -> datetime:
    """Converts timezone of datetime object to UTC and removes timezone awareness."""
    if ts.replace(tzinfo=None) == HIGH_TS:
        return ts.replace(tzinfo=None)
    else:
        return ts.astimezone(tz=timezone.utc).replace(tzinfo=None)


def get_table(pipeline: dlt.Pipeline, table_name: str, sort_column: str) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries."""
    return sorted(
        [
            {
                k: strip_timezone(v) if isinstance(v, datetime) else v
                for k, v in r.items()
                if not k.startswith("_dlt") or k == "_dlt_root_id"
            }
            for r in load_tables_to_dicts(pipeline, table_name)[table_name]
        ],
        key=lambda d: d[sort_column],
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("simple", [True, False])
def test_core_functionality(destination_config: DestinationTestConfiguration, simple: bool) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(table_name="dim_test", write_disposition="merge", merge_strategy="scd2")
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo" if simple else {"nc1": "foo"}},
        {"nk": 2, "c1": "bar", "c2": "bar" if simple else {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap))
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    cname = "c2" if simple else "c2__nc1"
    assert get_table(p, "dim_test", cname) == [
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 2, "c1": "bar", cname: "bar"},
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo", cname: "foo"},
    ]

    # load 2 — update a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
        {"nk": 2, "c1": "bar", "c2": "bar" if simple else {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap))
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 2, "c1": "bar", cname: "bar"},
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {"valid_from": ts_2, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]

    # load 3 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
    ]
    info = p.run(r(dim_snap))
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {"valid_from": ts_1, "valid_to": ts_3, "nk": 2, "c1": "bar", cname: "bar"},
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {"valid_from": ts_2, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]

    # load 4 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
        {"nk": 3, "c1": "baz", "c2": "baz" if simple else {"nc1": "baz"}},
    ]
    info = p.run(r(dim_snap))
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {"valid_from": ts_1, "valid_to": ts_3, "nk": 2, "c1": "bar", cname: "bar"},
        {"valid_from": ts_4, "valid_to": ACTIVE_TS, "nk": 3, "c1": "baz", cname: "baz"},
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {"valid_from": ts_2, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("simple", [True, False])
def test_child_table(destination_config: DestinationTestConfiguration, simple: bool) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(table_name="dim_test", write_disposition="merge", merge_strategy="scd2")
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap: List[Dict[str, Any]] = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [1] if simple else [{"cc1": 1}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap))
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 2, "c1": "bar"},
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo"},
    ]
    cname = "value" if simple else "cc1"
    assert get_table(p, "dim_test__c2", cname) == [
        {"_dlt_root_id": h(l1_1), cname: 1},
        {"_dlt_root_id": h(l1_2), cname: 2},
        {"_dlt_root_id": h(l1_2), cname: 3},
    ]

    # load 2 — update a record — change not in complex column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [1] if simple else [{"cc1": 1}]},
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap))
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 2, "c1": "bar"},
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo"},  # updated
        {"valid_from": ts_2, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo_updated"},  # new
    ]
    assert get_table(p, "dim_test__c2", cname) == [
        {"_dlt_root_id": h(l1_1), cname: 1},
        {"_dlt_root_id": h(l2_1), cname: 1},  # new
        {"_dlt_root_id": h(l1_2), cname: 2},
        {"_dlt_root_id": h(l1_2), cname: 3},
    ]

    # load 3 — update a record — change in complex column
    dim_snap = [
        l3_1 := {
            "nk": 1,
            "c1": "foo_updated",
            "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}],
        },
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap))
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {"valid_from": ts_1, "valid_to": ACTIVE_TS, "nk": 2, "c1": "bar"},
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo"},
        {"valid_from": ts_2, "valid_to": ts_3, "nk": 1, "c1": "foo_updated"},  # updated
        {"valid_from": ts_3, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo_updated"},  # new
    ]
    exp_3 = [
        {"_dlt_root_id": h(l1_1), cname: 1},
        {"_dlt_root_id": h(l2_1), cname: 1},
        {"_dlt_root_id": h(l3_1), cname: 1},  # new
        {"_dlt_root_id": h(l1_2), cname: 2},
        {"_dlt_root_id": h(l3_1), cname: 2},  # new
        {"_dlt_root_id": h(l1_2), cname: 3},
    ]
    assert get_table(p, "dim_test__c2", cname) == exp_3

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap))
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {"valid_from": ts_1, "valid_to": ts_4, "nk": 2, "c1": "bar"},  # updated
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo"},
        {"valid_from": ts_2, "valid_to": ts_3, "nk": 1, "c1": "foo_updated"},
        {"valid_from": ts_3, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo_updated"},
    ]
    assert get_table(p, "dim_test__c2", cname) == exp_3  # deletes should not alter child tables

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap))
    ts_5 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {"valid_from": ts_1, "valid_to": ts_4, "nk": 2, "c1": "bar"},
        {"valid_from": ts_5, "valid_to": ACTIVE_TS, "nk": 3, "c1": "baz"},  # new
        {"valid_from": ts_1, "valid_to": ts_2, "nk": 1, "c1": "foo"},
        {"valid_from": ts_2, "valid_to": ts_3, "nk": 1, "c1": "foo_updated"},
        {"valid_from": ts_3, "valid_to": ACTIVE_TS, "nk": 1, "c1": "foo_updated"},
    ]
    assert get_table(p, "dim_test__c2", cname) == [
        {"_dlt_root_id": h(l1_1), cname: 1},
        {"_dlt_root_id": h(l2_1), cname: 1},
        {"_dlt_root_id": h(l3_1), cname: 1},
        {"_dlt_root_id": h(l5_3), cname: 1},  # new
        {"_dlt_root_id": h(l1_2), cname: 2},
        {"_dlt_root_id": h(l3_1), cname: 2},
        {"_dlt_root_id": h(l5_3), cname: 2},  # new
        {"_dlt_root_id": h(l1_2), cname: 3},
    ]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_grandchild_table(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(table_name="dim_test", write_disposition="merge", merge_strategy="scd2")
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap))
    assert_load_info(info)
    assert get_table(p, "dim_test__c2__cc1", "value") == [
        {"_dlt_root_id": h(l1_1), "value": 1},
        {"_dlt_root_id": h(l1_2), "value": 1},
        {"_dlt_root_id": h(l1_2), "value": 2},
    ]

    # load 2 — update a record — change not in complex column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap))
    assert_load_info(info)
    assert get_table(p, "dim_test__c2__cc1", "value") == [
        {"_dlt_root_id": h(l1_1), "value": 1},
        {"_dlt_root_id": h(l1_2), "value": 1},
        {"_dlt_root_id": h(l2_1), "value": 1},  # new
        {"_dlt_root_id": h(l1_2), "value": 2},
    ]

    # load 3 — update a record — change in complex column
    dim_snap = [
        l3_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap))
    assert_load_info(info)
    exp_3 = [
        {"_dlt_root_id": h(l1_1), "value": 1},
        {"_dlt_root_id": h(l1_2), "value": 1},
        {"_dlt_root_id": h(l2_1), "value": 1},
        {"_dlt_root_id": h(l3_1), "value": 1},  # new
        {"_dlt_root_id": h(l1_2), "value": 2},
        {"_dlt_root_id": h(l3_1), "value": 2},  # new
    ]
    assert get_table(p, "dim_test__c2__cc1", "value") == exp_3

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap))
    assert_load_info(info)
    assert get_table(p, "dim_test__c2__cc1", "value") == exp_3

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [{"cc1": [1]}]},
    ]
    info = p.run(r(dim_snap))
    assert_load_info(info)
    assert get_table(p, "dim_test__c2__cc1", "value") == [
        {"_dlt_root_id": h(l1_1), "value": 1},
        {"_dlt_root_id": h(l1_2), "value": 1},
        {"_dlt_root_id": h(l2_1), "value": 1},
        {"_dlt_root_id": h(l3_1), "value": 1},
        {"_dlt_root_id": h(l5_3), "value": 1},  # new
        {"_dlt_root_id": h(l1_2), "value": 2},
        {"_dlt_root_id": h(l3_1), "value": 2},
    ]
