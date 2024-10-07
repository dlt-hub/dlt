# timezone is removed from all datetime objects in these tests to simplify comparison

import pytest
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timezone  # noqa: I251
from contextlib import nullcontext as does_not_raise

import dlt
from dlt.common.typing import TAnyDateTime
from dlt.common.pendulum import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import DEFAULT_VALIDITY_COLUMN_NAMES
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.time import ensure_pendulum_datetime, reduce_pendulum_datetime_precision
from dlt.extract.resource import DltResource

from tests.cases import arrow_table_all_data_types
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)
from tests.pipeline.utils import (
    load_tables_to_dicts,
    assert_load_info,
    load_table_counts,
    assert_records_as_set,
)

from tests.utils import TPythonTableFormat

get_row_hash = DataItemNormalizer.get_row_hash
FROM, TO = DEFAULT_VALIDITY_COLUMN_NAMES


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


def strip_timezone(ts: TAnyDateTime) -> pendulum.DateTime:
    """Converts timezone of datetime object to UTC and removes timezone awareness."""
    return ensure_pendulum_datetime(ts).astimezone(tz=timezone.utc).replace(tzinfo=None)


def get_table(
    pipeline: dlt.Pipeline,
    table_name: str,
    sort_column: str = None,
    include_root_id: bool = True,
    ts_columns: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries."""
    ts_columns = ts_columns or []

    table = [
        {
            k: (
                strip_timezone(v)
                if isinstance(v, datetime) or (k in ts_columns and v is not None)
                else v
            )
            for k, v in r.items()
            if not k.startswith("_dlt")
            or k in DEFAULT_VALIDITY_COLUMN_NAMES
            or (k == "_dlt_root_id" if include_root_id else False)
        }
        for r in load_tables_to_dicts(pipeline, table_name)[table_name]
    ]

    if sort_column is None:
        return table
    return sorted(table, key=lambda d: d[sort_column])


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "validity_column_names",
    [None, ["from", "to"], ["ValidFrom", "ValidTo"]],
    ids=lambda x: x[0] + "-" + x[1] if isinstance(x, list) else x,
)
def test_core_functionality(
    destination_config: DestinationTestConfiguration,
    validity_column_names: List[str],
) -> None:
    if validity_column_names is not None and destination_config.destination_type != "postgres":
        pytest.skip("test `validity_column_names` configuration only for `postgres`")
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "disposition": "merge",
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
        {"nk": 1, "c1": "foo", "c2": {"nc1": "foo"}},
        {"nk": 2, "c1": "bar", "c2": {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    # assert x-hints
    table = p.default_schema.get_table("dim_test")
    assert table["x-merge-strategy"] == "scd2"  # type: ignore[typeddict-item]
    assert table["columns"][from_]["x-valid-from"]  # type: ignore[typeddict-item]
    assert table["columns"][to]["x-valid-to"]  # type: ignore[typeddict-item]
    assert table["columns"]["_dlt_id"]["x-row-version"]  # type: ignore[typeddict-item]
    # root table _dlt_id is not unique with `scd2` merge strategy
    assert not table["columns"]["_dlt_id"]["unique"]

    # assert load results
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c2__nc1", ts_columns=[from_, to]) == [
        {
            from_: ts_1,
            to: None,
            "nk": 2,
            "c1": "bar",
            "c2__nc1": "bar",
        },
        {
            from_: ts_1,
            to: None,
            "nk": 1,
            "c1": "foo",
            "c2__nc1": "foo",
        },
    ]

    # load 2 — update a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": {"nc1": "foo_updated"}},
        {"nk": 2, "c1": "bar", "c2": {"nc1": "bar"}},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c2__nc1", ts_columns=[from_, to]) == [
        {
            from_: ts_1,
            to: None,
            "nk": 2,
            "c1": "bar",
            "c2__nc1": "bar",
        },
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", "c2__nc1": "foo"},
        {
            from_: ts_2,
            to: None,
            "nk": 1,
            "c1": "foo",
            "c2__nc1": "foo_updated",
        },
    ]

    # load 3 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": {"nc1": "foo_updated"}},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c2__nc1", ts_columns=[from_, to]) == [
        {from_: ts_1, to: ts_3, "nk": 2, "c1": "bar", "c2__nc1": "bar"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", "c2__nc1": "foo"},
        {
            from_: ts_2,
            to: None,
            "nk": 1,
            "c1": "foo",
            "c2__nc1": "foo_updated",
        },
    ]

    # load 4 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo", "c2": {"nc1": "foo_updated"}},
        {"nk": 3, "c1": "baz", "c2": {"nc1": "baz"}},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c2__nc1", ts_columns=[from_, to]) == [
        {from_: ts_1, to: ts_3, "nk": 2, "c1": "bar", "c2__nc1": "bar"},
        {
            from_: ts_4,
            to: None,
            "nk": 3,
            "c1": "baz",
            "c2__nc1": "baz",
        },
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", "c2__nc1": "foo"},
        {
            from_: ts_2,
            to: None,
            "nk": 1,
            "c1": "foo",
            "c2__nc1": "foo_updated",
        },
    ]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("simple", [True, False])
def test_child_table(destination_config: DestinationTestConfiguration, simple: bool) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test", write_disposition={"disposition": "merge", "strategy": "scd2"}
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap: List[Dict[str, Any]] = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [1] if simple else [{"cc1": 1}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_1 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1", ts_columns=[FROM, TO]) == [
        {FROM: ts_1, TO: None, "nk": 2, "c1": "bar"},
        {FROM: ts_1, TO: None, "nk": 1, "c1": "foo"},
    ]
    cname = "value" if simple else "cc1"
    assert get_table(p, "dim_test__c2", cname) == [
        {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
        {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
        {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
    ]

    # load 2 — update a record — change not in nested column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [1] if simple else [{"cc1": 1}]},
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_2 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert get_table(p, "dim_test", "c1", ts_columns=[FROM, TO]) == [
        {FROM: ts_1, TO: None, "nk": 2, "c1": "bar"},
        {FROM: ts_1, TO: ts_2, "nk": 1, "c1": "foo"},  # updated
        {FROM: ts_2, TO: None, "nk": 1, "c1": "foo_updated"},  # new
    ]
    assert_records_as_set(
        get_table(p, "dim_test__c2"),
        [
            {"_dlt_root_id": get_row_hash(l1_1), cname: 1},
            {"_dlt_root_id": get_row_hash(l2_1), cname: 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), cname: 2},
            {"_dlt_root_id": get_row_hash(l1_2), cname: 3},
        ],
    )

    # load 3 — update a record — change in nested column
    dim_snap = [
        l3_1 := {
            "nk": 1,
            "c1": "foo_updated",
            "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}],
        },
        {"nk": 2, "c1": "bar", "c2": [2, 3] if simple else [{"cc1": 2}, {"cc1": 3}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_3 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", ts_columns=[FROM, TO]),
        [
            {FROM: ts_1, TO: None, "nk": 2, "c1": "bar"},
            {FROM: ts_1, TO: ts_2, "nk": 1, "c1": "foo"},
            {FROM: ts_2, TO: ts_3, "nk": 1, "c1": "foo_updated"},  # updated
            {FROM: ts_3, TO: None, "nk": 1, "c1": "foo_updated"},  # new
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
    assert_records_as_set(get_table(p, "dim_test__c2"), exp_3)

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_4 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", ts_columns=[FROM, TO]),
        [
            {FROM: ts_1, TO: ts_4, "nk": 2, "c1": "bar"},  # updated
            {FROM: ts_1, TO: ts_2, "nk": 1, "c1": "foo"},
            {FROM: ts_2, TO: ts_3, "nk": 1, "c1": "foo_updated"},
            {FROM: ts_3, TO: None, "nk": 1, "c1": "foo_updated"},
        ],
    )
    assert_records_as_set(
        get_table(p, "dim_test__c2"), exp_3
    )  # deletes should not alter child tables

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [1, 2] if simple else [{"cc1": 1}, {"cc1": 2}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    ts_5 = get_load_package_created_at(p, info)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test", ts_columns=[FROM, TO]),
        [
            {FROM: ts_1, TO: ts_4, "nk": 2, "c1": "bar"},
            {FROM: ts_5, TO: None, "nk": 3, "c1": "baz"},  # new
            {FROM: ts_1, TO: ts_2, "nk": 1, "c1": "foo"},
            {FROM: ts_2, TO: ts_3, "nk": 1, "c1": "foo_updated"},
            {FROM: ts_3, TO: None, "nk": 1, "c1": "foo_updated"},
        ],
    )
    assert_records_as_set(
        get_table(p, "dim_test__c2"),
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
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test", write_disposition={"disposition": "merge", "strategy": "scd2"}
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        l1_1 := {"nk": 1, "c1": "foo", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test__c2__cc1"),
        [
            {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        ],
    )

    # load 2 — update a record — change not in nested column
    dim_snap = [
        l2_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1]}]},
        l1_2 := {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert_records_as_set(
        (get_table(p, "dim_test__c2__cc1")),
        [
            {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
            {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
            {"_dlt_root_id": get_row_hash(l2_1), "value": 1},  # new
            {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        ],
    )

    # load 3 — update a record — change in nested column
    dim_snap = [
        l3_1 := {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        {"nk": 2, "c1": "bar", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    exp_3 = [
        {"_dlt_root_id": get_row_hash(l1_1), "value": 1},
        {"_dlt_root_id": get_row_hash(l1_2), "value": 1},
        {"_dlt_root_id": get_row_hash(l2_1), "value": 1},
        {"_dlt_root_id": get_row_hash(l3_1), "value": 1},  # new
        {"_dlt_root_id": get_row_hash(l1_2), "value": 2},
        {"_dlt_root_id": get_row_hash(l3_1), "value": 2},  # new
    ]
    assert_records_as_set(get_table(p, "dim_test__c2__cc1"), exp_3)

    # load 4 — delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert_records_as_set(get_table(p, "dim_test__c2__cc1"), exp_3)

    # load 5 — insert a record
    dim_snap = [
        {"nk": 1, "c1": "foo_updated", "c2": [{"cc1": [1, 2]}]},
        l5_3 := {"nk": 3, "c1": "baz", "c2": [{"cc1": [1]}]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert_records_as_set(
        get_table(p, "dim_test__c2__cc1"),
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
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_record_reinsert(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test", write_disposition={"disposition": "merge", "strategy": "scd2"}
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        r1 := {"nk": 1, "c1": "foo", "c2": "foo", "child": [1]},
        r2 := {"nk": 2, "c1": "bar", "c2": "bar", "child": [2, 3]},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    assert load_table_counts(p, "dim_test__child")["dim_test__child"] == 3
    ts_1 = get_load_package_created_at(p, info)

    # load 2 — delete natural key 1
    dim_snap = [r2]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    assert load_table_counts(p, "dim_test__child")["dim_test__child"] == 3
    ts_2 = get_load_package_created_at(p, info)

    # load 3 — reinsert natural key 1
    dim_snap = [r1, r2]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 3
    assert load_table_counts(p, "dim_test__child")["dim_test__child"] == 3  # no new record
    ts_3 = get_load_package_created_at(p, info)

    # assert parent records
    r1_no_child = {k: v for k, v in r1.items() if k != "child"}
    r2_no_child = {k: v for k, v in r2.items() if k != "child"}
    expected = [
        {**{FROM: ts_1, TO: ts_2}, **r1_no_child},
        {**{FROM: ts_3, TO: None}, **r1_no_child},
        {**{FROM: ts_1, TO: None}, **r2_no_child},
    ]
    assert_records_as_set(get_table(p, "dim_test", ts_columns=[FROM, TO]), expected)

    # assert child records
    expected = [
        {"_dlt_root_id": get_row_hash(r1), "value": 1},  # links to two records in parent
        {"_dlt_root_id": get_row_hash(r2), "value": 2},
        {"_dlt_root_id": get_row_hash(r2), "value": 3},
    ]
    assert_records_as_set(get_table(p, "dim_test__child"), expected)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_validity_column_name_conflict(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "validity_column_names": ["from", "to"],
        },
    )
    def r(data):
        yield data

    # a schema check against an items got dropped because it was very costly and done on each row
    dim_snap = {"nk": 1, "foo": 1, "from": "X"}  # conflict on "from" column
    p.run(r(dim_snap), **destination_config.run_kwargs)
    dim_snap = {"nk": 1, "foo": 1, "to": 1}  # conflict on "to" column
    p.run(r(dim_snap), **destination_config.run_kwargs)

    # instead the variant columns got generated
    dim_test_table = p.default_schema.tables["dim_test"]
    assert "from__v_text" in dim_test_table["columns"]

    # but `to` column was coerced and then overwritten, this is the cost of dropping the check


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "active_record_timestamp",
    [
        date(9999, 12, 31),
        datetime(9999, 12, 31),
        pendulum.Date(9999, 12, 31),
        pendulum.DateTime(9999, 12, 31),
        "9999-12-31",
        "9999-12-31T00:00:00",
        "9999-12-31T00:00:00+00:00",
        "9999-12-31T00:00:00+01:00",
        "i_am_not_a_timestamp",
    ],
)
def test_active_record_timestamp(
    destination_config: DestinationTestConfiguration,
    active_record_timestamp: Optional[TAnyDateTime],
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    context = does_not_raise()
    if active_record_timestamp == "i_am_not_a_timestamp":
        context = pytest.raises(ValueError)  # type: ignore[assignment]

    with context:

        @dlt.resource(
            table_name="dim_test",
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "active_record_timestamp": active_record_timestamp,
            },
        )
        def r():
            yield {"foo": "bar"}

        p.run(r(), **destination_config.run_kwargs)
        actual_active_record_timestamp = ensure_pendulum_datetime(
            load_tables_to_dicts(p, "dim_test")["dim_test"][0]["_dlt_valid_to"]
        )
        assert actual_active_record_timestamp == ensure_pendulum_datetime(active_record_timestamp)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_boundary_timestamp(
    destination_config: DestinationTestConfiguration,
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    ts1 = "2024-08-21T12:15:00+00:00"
    ts2 = "2024-08-22"
    ts3 = date(2024, 8, 20)  # earlier than ts1 and ts2
    ts4 = "i_am_not_a_timestamp"

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "boundary_timestamp": ts1,
        },
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        l1_1 := {"nk": 1, "foo": "foo"},
        l1_2 := {"nk": 2, "foo": "foo"},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    expected = [
        {**{FROM: strip_timezone(ts1), TO: None}, **l1_1},
        {**{FROM: strip_timezone(ts1), TO: None}, **l1_2},
    ]
    assert get_table(p, "dim_test", "nk") == expected

    # load 2 — different source records, different boundary timestamp
    r.apply_hints(
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "boundary_timestamp": ts2,
        }
    )
    dim_snap = [
        l2_1 := {"nk": 1, "foo": "bar"},  # natural key 1 updated
        # l1_2,  # natural key 2 no longer present
        l2_3 := {"nk": 3, "foo": "foo"},  # new natural key
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 4
    expected = [
        {**{FROM: strip_timezone(ts1), TO: strip_timezone(ts2)}, **l1_1},  # retired
        {**{FROM: strip_timezone(ts1), TO: strip_timezone(ts2)}, **l1_2},  # retired
        {**{FROM: strip_timezone(ts2), TO: None}, **l2_1},  # new
        {**{FROM: strip_timezone(ts2), TO: None}, **l2_3},  # new
    ]
    assert_records_as_set(get_table(p, "dim_test"), expected)

    # load 3 — earlier boundary timestamp
    # we naively apply any valid timestamp
    # may lead to "valid from" > "valid to", as in this test case
    r.apply_hints(
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "boundary_timestamp": ts3,
        }
    )
    dim_snap = [l2_1]  # natural key 3 no longer present
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 4
    expected = [
        {**{FROM: strip_timezone(ts1), TO: strip_timezone(ts2)}, **l1_1},  # unchanged
        {**{FROM: strip_timezone(ts1), TO: strip_timezone(ts2)}, **l1_2},  # unchanged
        {**{FROM: strip_timezone(ts2), TO: None}, **l2_1},  # unchanged
        {**{FROM: strip_timezone(ts2), TO: strip_timezone(ts3)}, **l2_3},  # retired
    ]
    assert_records_as_set(get_table(p, "dim_test"), expected)

    # invalid boundary timestamp should raise error
    with pytest.raises(ValueError):
        r.apply_hints(
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "boundary_timestamp": ts4,
            }
        )


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_merge_key_natural_key(
    destination_config: DestinationTestConfiguration,
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        merge_key="nk",
        write_disposition={"disposition": "merge", "strategy": "scd2"},
    )
    def dim_test(data):
        yield data

    # load 1 — initial load
    dim_snap = [
        {"nk": 1, "foo": "foo"},
        {"nk": 2, "foo": "foo"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    # both records should be active (i.e. not retired)
    assert [row[TO] for row in get_table(p, "dim_test")] == [None, None]

    # load 2 — natural key 2 is absent, natural key 1 is unchanged
    dim_snap = [
        {"nk": 1, "foo": "foo"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    # both records should still be active
    assert [row[TO] for row in get_table(p, "dim_test")] == [None, None]

    # load 3 — natural key 2 is absent, natural key 1 has changed
    dim_snap = [
        {"nk": 1, "foo": "bar"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 3
    ts3 = get_load_package_created_at(p, info)
    # natural key 1 should now have two records (one retired, one active)
    actual = [
        {k: v for k, v in row.items() if k in ("nk", TO)}
        for row in get_table(p, "dim_test", ts_columns=[FROM, TO])
    ]
    expected = [{"nk": 1, TO: ts3}, {"nk": 1, TO: None}, {"nk": 2, TO: None}]
    assert_records_as_set(actual, expected)  # type: ignore[arg-type]

    # load 4 — natural key 2 is absent, natural key 1 has changed back to
    # initial version
    dim_snap = [
        {"nk": 1, "foo": "foo"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 4
    ts4 = get_load_package_created_at(p, info)
    # natural key 1 should now have three records (two retired, one active)
    actual = [
        {k: v for k, v in row.items() if k in ("nk", TO)}
        for row in get_table(p, "dim_test", ts_columns=[FROM, TO])
    ]
    expected = [{"nk": 1, TO: ts3}, {"nk": 1, TO: ts4}, {"nk": 1, TO: None}, {"nk": 2, TO: None}]
    assert_records_as_set(actual, expected)  # type: ignore[arg-type]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("key_type", ("text", "bigint"))
def test_merge_key_compound_natural_key(
    destination_config: DestinationTestConfiguration,
    key_type: TDataType,
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        merge_key=["first_name", "last_name"],
        write_disposition={"disposition": "merge", "strategy": "scd2"},
    )
    def dim_test_compound(data):
        yield data

    # vary `first_name` type to test mixed compound `merge_key`
    if key_type == "text":
        first_name = "John"
    elif key_type == "bigint":
        first_name = 1  # type: ignore[assignment]
    # load 1 — initial load
    dim_snap = [
        {"first_name": first_name, "last_name": "Doe", "age": 20},
        {"first_name": first_name, "last_name": "Dodo", "age": 20},
    ]
    info = p.run(dim_test_compound(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test_compound")["dim_test_compound"] == 2
    # both records should be active (i.e. not retired)
    assert [row[TO] for row in get_table(p, "dim_test_compound")] == [None, None]

    # load 2 — "Dodo" is absent, "Doe" has changed
    dim_snap = [
        {"first_name": first_name, "last_name": "Doe", "age": 30},
    ]
    info = p.run(dim_test_compound(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test_compound")["dim_test_compound"] == 3
    ts3 = get_load_package_created_at(p, info)
    # "Doe" should now have two records (one retired, one active)
    actual = [
        {k: v for k, v in row.items() if k in ("first_name", "last_name", TO)}
        for row in get_table(p, "dim_test_compound", ts_columns=[FROM, TO])
    ]
    expected = [
        {"first_name": first_name, "last_name": "Doe", TO: ts3},
        {"first_name": first_name, "last_name": "Doe", TO: None},
        {"first_name": first_name, "last_name": "Dodo", TO: None},
    ]
    assert_records_as_set(actual, expected)  # type: ignore[arg-type]


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_merge_key_partition(
    destination_config: DestinationTestConfiguration,
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        merge_key="date",
        write_disposition={"disposition": "merge", "strategy": "scd2"},
    )
    def dim_test(data):
        yield data

    # load 1 — "2024-01-01" partition
    dim_snap = [
        {"date": "2024-01-01", "name": "a"},
        {"date": "2024-01-01", "name": "b"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 2
    # both records should be active (i.e. not retired)
    assert [row[TO] for row in get_table(p, "dim_test")] == [None, None]

    # load 2 — "2024-01-02" partition
    dim_snap = [
        {"date": "2024-01-02", "name": "c"},
        {"date": "2024-01-02", "name": "d"},
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "dim_test")["dim_test"] == 4
    # two "2024-01-01" records should be untouched, two "2024-01-02" records should
    # be added
    assert [row[TO] for row in get_table(p, "dim_test")] == [None, None, None, None]

    # load 3 — reload "2024-01-01" partition
    dim_snap = [
        {"date": "2024-01-01", "name": "a"},  # unchanged
        {"date": "2024-01-01", "name": "bb"},  # new
    ]
    info = p.run(dim_test(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    # "b" should be retired, "bb" should be added, "2024-01-02" partition
    # should be untouched
    assert load_table_counts(p, "dim_test")["dim_test"] == 5
    ts2 = get_load_package_created_at(p, info)
    actual = [
        {k: v for k, v in row.items() if k in ("date", "name", TO)}
        for row in get_table(p, "dim_test", ts_columns=[TO])
    ]
    expected = [
        {"date": "2024-01-01", "name": "a", TO: None},
        {"date": "2024-01-01", "name": "b", TO: ts2},
        {"date": "2024-01-01", "name": "bb", TO: None},
        {"date": "2024-01-02", "name": "c", TO: None},
        {"date": "2024-01-02", "name": "d", TO: None},
    ]
    assert_records_as_set(actual, expected)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_arrow_custom_hash(
    destination_config: DestinationTestConfiguration, item_type: TPythonTableFormat
) -> None:
    table, _, _ = arrow_table_all_data_types(item_type, num_rows=100, include_json=False)
    orig_table: Any = None
    if item_type == "pandas":
        orig_table = table.copy(deep=True)

    from dlt.sources.helpers.transform import add_row_hash_to_table

    def _make_scd2_r(table_: Any) -> DltResource:
        return dlt.resource(
            table_,
            name="tabular",
            write_disposition={
                "disposition": "merge",
                "strategy": "scd2",
                "row_version_column_name": "row_hash",
            },
        ).add_map(add_row_hash_to_table("row_hash"))

    p = destination_config.setup_pipeline("abstract", dev_mode=True)
    info = p.run(_make_scd2_r(table), **destination_config.run_kwargs)
    assert_load_info(info)
    # make sure we have scd2 columns in schema
    table_schema = p.default_schema.get_table("tabular")
    assert table_schema["x-merge-strategy"] == "scd2"  # type: ignore[typeddict-item]
    assert table_schema["columns"][FROM]["x-valid-from"]  # type: ignore[typeddict-item]
    assert table_schema["columns"][TO]["x-valid-to"]  # type: ignore[typeddict-item]
    assert table_schema["columns"]["row_hash"]["x-row-version"]  # type: ignore[typeddict-item]
    # 100 items in destination
    assert load_table_counts(p, "tabular")["tabular"] == 100

    # modify in place (pandas only)
    if item_type == "pandas":
        table = orig_table
        orig_table = table.copy(deep=True)
        info = p.run(_make_scd2_r(table), **destination_config.run_kwargs)
        assert_load_info(info)
        # no changes (hopefully hash is deterministic)
        assert load_table_counts(p, "tabular")["tabular"] == 100

        # change single row
        orig_table.iloc[0, 0] = "Duck 🦆!"
        info = p.run(_make_scd2_r(orig_table), **destination_config.run_kwargs)
        assert_load_info(info)
        # on row changed
        assert load_table_counts(p, "tabular")["tabular"] == 101


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_user_provided_row_hash(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "row_version_column_name": "row_hash",
        },
    )
    def r(data):
        yield data

    # load 1 — initial load
    dim_snap: List[Dict[str, Any]] = [
        {"nk": 1, "c1": "foo", "c2": [1], "row_hash": "mocked_hash_1"},
        {"nk": 2, "c1": "bar", "c2": [2, 3], "row_hash": "mocked_hash_2"},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    ts_1 = get_load_package_created_at(p, info)
    table = p.default_schema.get_table("dim_test")
    assert table["columns"]["row_hash"]["x-row-version"]  # type: ignore[typeddict-item]
    assert "x-row-version" not in table["columns"]["_dlt_id"]
    # _dlt_id unique constraint should not be dropped when users bring their own hash
    assert table["columns"]["_dlt_id"]["unique"]

    # load 2 — update and delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_upd", "c2": [1], "row_hash": "mocked_hash_1_upd"},
    ]
    info = p.run(r(dim_snap), **destination_config.run_kwargs)
    assert_load_info(info)
    ts_2 = get_load_package_created_at(p, info)

    # assert load results
    assert get_table(p, "dim_test", "c1") == [
        {FROM: ts_1, TO: ts_2, "nk": 2, "c1": "bar", "row_hash": "mocked_hash_2"},
        {FROM: ts_1, TO: ts_2, "nk": 1, "c1": "foo", "row_hash": "mocked_hash_1"},
        {
            FROM: ts_2,
            TO: None,
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
