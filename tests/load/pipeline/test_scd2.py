# timezone is removed from all datetime objects in these tests to simplify comparison

import pytest
from typing import List, Dict, Any
from datetime import datetime, timezone  # noqa: I251

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.exceptions import ColumnNameConflictException
from dlt.common.schema.utils import DEFAULT_VALIDITY_COLUMN_NAMES
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.time import ensure_pendulum_datetime, reduce_pendulum_datetime_precision
from dlt.destinations.sql_jobs import HIGH_TS
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.pipeline.utils import assert_load_info
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    load_tables_to_dicts,
)

get_row_hash = DataItemNormalizer.get_row_hash


def get_active_ts(pipeline: dlt.Pipeline) -> datetime:
    caps = pipeline._get_destination_capabilities()
    active_ts = HIGH_TS.in_timezone(tz="UTC").replace(tzinfo=None)
    return reduce_pendulum_datetime_precision(active_ts, caps.timestamp_precision)


def get_load_package_created_at(pipeline: dlt.Pipeline, load_info: LoadInfo) -> datetime:
    """Returns `created_at` property of load package state."""
    load_id = load_info.asdict()["loads_ids"][0]
    created_at = (
        pipeline.get_load_package_state(load_id)["created_at"]
        .in_timezone(tz="UTC")
        .replace(tzinfo=None)
    )
    caps = pipeline._get_destination_capabilities()
    return reduce_pendulum_datetime_precision(created_at, caps.timestamp_precision)


def strip_timezone(ts: datetime) -> datetime:
    """Converts timezone of datetime object to UTC and removes timezone awareness."""
    ts = ensure_pendulum_datetime(ts)
    if ts.replace(tzinfo=None) == HIGH_TS:
        return ts.replace(tzinfo=None)
    else:
        return ts.astimezone(tz=timezone.utc).replace(tzinfo=None)


def get_table(
    pipeline: dlt.Pipeline, table_name: str, sort_column: str, include_root_id: bool = True
) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries."""
    return sorted(
        [
            {
                k: strip_timezone(v) if isinstance(v, datetime) else v
                for k, v in r.items()
                if not k.startswith("_dlt")
                or k in DEFAULT_VALIDITY_COLUMN_NAMES
                or (k == "_dlt_root_id" if include_root_id else False)
            }
            for r in load_tables_to_dicts(pipeline, table_name)[table_name]
        ],
        key=lambda d: d[sort_column],
    )


def assert_records_as_set(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]) -> None:
    """Compares two lists of dicts regardless of order"""
    actual_set = set(frozenset(dict_.items()) for dict_ in actual)
    expected_set = set(frozenset(dict_.items()) for dict_ in expected)
    assert actual_set == expected_set


@pytest.mark.parametrize(
    "destination_config,simple,validity_column_names",
    [  # test basic case for alle SQL destinations supporting merge
        (dconf, True, None)
        for dconf in destinations_configs(default_sql_configs=True, supports_merge=True)
    ]
    + [  # test nested columns and validity column name configuration only for postgres
        (
            dconf,
            False,
            ["from", "to"],
        )  # "from" is a SQL keyword, so this also tests if columns are escaped
        for dconf in destinations_configs(default_sql_configs=True, subset=["postgres", "duckdb"])
    ]
    + [
        (dconf, False, ["ValidFrom", "ValidTo"])
        for dconf in destinations_configs(default_sql_configs=True, subset=["postgres", "duckdb"])
    ],
    ids=lambda x: (
        x.name
        if isinstance(x, DestinationTestConfiguration)
        else (x[0] + "-" + x[1] if isinstance(x, list) else x)
    ),
)
def test_core_functionality(
    destination_config: DestinationTestConfiguration,
    simple: bool,
    validity_column_names: List[str],
) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "mode": "merge",
            "strategy": "scd2",
            "validity_column_names": validity_column_names,
        },
    )
    def r(data):
        yield data

    # get validity column names
    from_, to = (
        DEFAULT_VALIDITY_COLUMN_NAMES
        if validity_column_names is None
        else map(SnakeCaseNamingConvention().normalize_identifier, validity_column_names)
    )

    # load 1 — initial load
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo" if simple else {"nc1": "foo"}},
        {"nk": 2, "c1": "bar", "c2": "bar" if simple else {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)

    # assert x-hints
    table = p.default_schema.get_table("dim_test")
    assert table["x-merge-strategy"] == "scd2"  # type: ignore[typeddict-item]
    assert table["columns"][from_]["x-valid-from"]  # type: ignore[typeddict-item]
    assert table["columns"][to]["x-valid-to"]  # type: ignore[typeddict-item]
    assert table["columns"]["_dlt_id"]["x-row-version"]  # type: ignore[typeddict-item]
    # _dlt_id is still unique
    assert table["columns"]["_dlt_id"]["unique"]

    # assert load results
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    cname = "c2" if simple else "c2__nc1"
    assert get_table(p, "dim_test", cname) == [
        {from_: ts_1, to: get_active_ts(p), "nk": 2, "c1": "bar", cname: "bar"},
        {from_: ts_1, to: get_active_ts(p), "nk": 1, "c1": "foo", cname: "foo"},
    ]

    # load 2 — update a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
        {"nk": 2, "c1": "bar", "c2": "bar" if simple else {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {from_: ts_1, to: get_active_ts(p), "nk": 2, "c1": "bar", cname: "bar"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {from_: ts_2, to: get_active_ts(p), "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]

    # load 3 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {from_: ts_1, to: ts_3, "nk": 2, "c1": "bar", cname: "bar"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {from_: ts_2, to: get_active_ts(p), "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]

    # load 4 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": "foo_updated" if simple else {"nc1": "foo_updated"}},
        {"nk": 3, "c1": "baz", "c2": "baz" if simple else {"nc1": "baz"}},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", cname) == [
        {from_: ts_1, to: ts_3, "nk": 2, "c1": "bar", cname: "bar"},
        {from_: ts_4, to: get_active_ts(p), "nk": 3, "c1": "baz", cname: "baz"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {from_: ts_2, to: get_active_ts(p), "nk": 1, "c1": "foo", cname: "foo_updated"},
    ]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("simple", [True, False])
def test_child_table(destination_config: DestinationTestConfiguration, simple: bool) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(table_name="dim_test", write_disposition={"mode": "merge", "strategy": "scd2"})
    def r(data):
        yield data

    # get validity column names
    from_, to = DEFAULT_VALIDITY_COLUMN_NAMES

    # load 1 — initial load
    dim_snap: List[Dict[str, Any]] = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [1] if simple else [{"cc1": 1}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {from_: ts_1, to: get_active_ts(p), "nk": 2, "c1": "bar"},
        {from_: ts_1, to: get_active_ts(p), "nk": 1, "c1": "foo"},
    ]
    cname = "value" if simple else "cc1"
    assert get_table(p, "dim_test__c2", cname) == [
        {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
        {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
        {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
    ]

    # load 2 — update a record — change not in complex column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [1] if simple else [{"cc1": 1}]},
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1") == [
        {from_: ts_1, to: get_active_ts(p), "nk": 2, "c1": "bar"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},  # updated
        {from_: ts_2, to: get_active_ts(p), "nk": 1, "c1": "foo_updated"},  # new
    ]
    assert_records_as_set(
        get_table(p, "dim_test__c2", cname),
        [
            {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
            {"_dlt_root_id": get_row_hash(l2_1), cname: 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
            {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
        ],
    )

    # load 3 — update a record — change in complex column
    dim_snap = [
        l3_1 := {
            "nk": 1,
            "c1": "foo_updated",
            "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}],
        },
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", "c1"),
        [
            {from_: ts_1, to: get_active_ts(p), "nk": 2, "c1": "bar"},
            {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},
            {from_: ts_2, to: ts_3, "nk": 1, "c1": "foo_updated"},  # updated
            {from_: ts_3, to: get_active_ts(p), "nk": 1, "c1": "foo_updated"},  # new
        ],
    )
    exp_3 = [
        {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
        {"_dlt_root_id": get_row_hash(l2_1), cname: 1},
        {"_dlt_root_id": get_row_hash(l3_1), cname: 1},  # new
        {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
        {"_dlt_root_id": get_row_hash(l3_1), cname: 2},  # new
        {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
    ]
    assert_records_as_set(get_table(p, "dim_test__c2", cname), exp_3)

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", "c1"),
        [
            {from_: ts_1, to: ts_4, "nk": 2, "c1": "bar"},  # updated
            {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},
            {from_: ts_2, to: ts_3, "nk": 1, "c1": "foo_updated"},
            {from_: ts_3, to: get_active_ts(p), "nk": 1, "c1": "foo_updated"},
        ],
    )
    assert_records_as_set(
        get_table(p, "dim_test__c2", cname), exp_3
    )  # deletes should not alter child tables

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_5 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", "c1"),
        [
            {from_: ts_1, to: ts_4, "nk": 2, "c1": "bar"},
            {from_: ts_5, to: get_active_ts(p), "nk": 3, "c1": "baz"},  # new
            {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},
            {from_: ts_2, to: ts_3, "nk": 1, "c1": "foo_updated"},
            {from_: ts_3, to: get_active_ts(p), "nk": 1, "c1": "foo_updated"},
        ],
    )
    assert_records_as_set(
        get_table(p, "dim_test__c2", cname),
        [
            {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
            {"_dlt_root_id": get_row_hash(l2_1), cname: 1},
            {"_dlt_root_id": get_row_hash(l3_1), cname: 1},
            {"_dlt_root_id": get_row_hash(l5_3), cname: 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
            {"_dlt_root_id": get_row_hash(l3_1), cname: 2},
            {"_dlt_root_id": get_row_hash(l5_3), cname: 2},  # new
            {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
        ],
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_grandchild_table(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(table_name="dim_test", write_disposition={"mode": "merge", "strategy": "scd2"})
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test__c2__cc1", "value"),
        [
            {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        ],
    )

    # load 2 — update a record — change not in complex column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert_records_as_set(
        (get_table(p, "dim_test__c2__cc1", "value")),
        [
            {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
            {"_dlt_root_id": get_row_hash(l2_1), "value": 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        ],
    )

    # load 3 — update a record — change in complex column
    dim_snap = [
        l3_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    exp_3 = [
        {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
        {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
        {"_dlt_root_id": get_row_hash(l2_1), "value": 1},
        {"_dlt_root_id": get_row_hash(l3_1), "value": 1},  # new
        {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        {"_dlt_root_id": get_row_hash(l3_1), "value": 2},  # new
    ]
    assert_records_as_set(get_table(p, "dim_test__c2__cc1", "value"), exp_3)

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert_records_as_set(get_table(p, "dim_test__c2__cc1", "value"), exp_3)

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [{"cc1": [1]}]},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test__c2__cc1", "value"),
        [
            {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
            {"_dlt_root_id": get_row_hash(l2_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l3_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l5_3), "value": 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
            {"_dlt_root_id": get_row_hash(l3_1), "value": 2},
        ],
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_validity_column_name_conflict(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "mode": "merge",
            "strategy": "scd2",
            "validity_column_names": ["from", "to"],
        },
    )
    def r(data):
        yield data

    # configuring a validity column name that appears in the data should cause an exception
    dim_snap = {"nk": 1, "foo": 1, "from": 1}  # conflict on "from" column
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert isinstance(pip_ex.value.__context__.__context__, ColumnNameConflictException)
    dim_snap = {"nk": 1, "foo": 1, "to": 1}  # conflict on "to" column
    with pytest.raises(PipelineStepFailed):
        p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert isinstance(pip_ex.value.__context__.__context__, ColumnNameConflictException)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_user_provided_row_hash(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", full_refresh=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "mode": "merge",
            "strategy": "scd2",
            "row_hash_column_name": "row_hash",
        },
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap: List[Dict[str, Any]] = [
        {"nk": 1, "c1": "foo", "c2": [1], "row_hash": "mocked_hash_1"},
        {"nk": 2, "c1": "bar", "c2": [2, 3], "row_hash": "mocked_hash_2"},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_1 = get_load_package_created_at(p, info)
    table = p.default_schema.get_table("dim_test")
    assert table["columns"]["row_hash"]["x-row-version"]  # type: ignore[typeddict-item]
    assert "x-row-version" not in table["columns"]["_dlt_id"]

    # load 2 — update and delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_upd", "c2": [1], "row_hash": "mocked_hash_1_upd"},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    ts_2 = get_load_package_created_at(p, info)

    # assert load results
    from_, to = DEFAULT_VALIDITY_COLUMN_NAMES
    assert get_table(p, "dim_test", "c1") == [
        {from_: ts_1, to: ts_2, "nk": 2, "c1": "bar", "row_hash": "mocked_hash_2"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", "row_hash": "mocked_hash_1"},
        {
            from_: ts_2,
            to: get_active_ts(p),
            "nk": 1,
            "c1": "foo_upd",
            "row_hash": "mocked_hash_1_upd",
        },
    ]
    # root id is not deterministic when a user provided row hash is used
    assert get_table(p, "dim_test__c2", "value", include_root_id=False) == [
        {"value": 1},
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]
