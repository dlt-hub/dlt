# timezone is removed from all datetime objects in these tests to simplify comparison

import pytest
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timezone  # noqa: I251

import dlt
from dlt.common.typing import TAnyDateTime
from dlt.common.pendulum import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.exceptions import ColumnNameConflictException
from dlt.common.schema.typing import DEFAULT_VALIDITY_COLUMN_NAMES
from dlt.common.normalizers.json.relational import DataItemNormalizer
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.time import ensure_pendulum_datetime, reduce_pendulum_datetime_precision
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

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


def get_table(
    pipeline: dlt.Pipeline, table_name: str, sort_column: str, include_root_id: bool = True
) -> List[Dict[str, Any]]:
    """Returns destination table contents as list of dictionaries."""

    def strip_timezone(ts: datetime) -> datetime:
        """Converts timezone of datetime object to UTC and removes timezone awareness."""
        return ensure_pendulum_datetime(ts).astimezone(tz=timezone.utc).replace(tzinfo=None)

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


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config,simple,validity_column_names,active_record_timestamp",
    # test basic cases for alle SQL destinations supporting merge
    [
        (dconf, True, None, None)
        for dconf in destinations_configs(default_sql_configs=True, supports_merge=True)
    ]
    + [
        (dconf, True, None, pendulum.DateTime(2099, 12, 31, 22, 2, 59))  # arbitrary timestamp
        for dconf in destinations_configs(default_sql_configs=True, supports_merge=True)
    ]
    + [  # test nested columns and validity column name configuration only for postgres and duckdb
        (dconf, False, ["from", "to"], None)
        for dconf in destinations_configs(default_sql_configs=True, subset=["postgres", "duckdb"])
    ]
    + [
        (dconf, False, ["ValidFrom", "ValidTo"], None)
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
    active_record_timestamp: Optional[pendulum.DateTime],
) -> None:
    # somehow destination_config comes through as ParameterSet instead of
    # DestinationTestConfiguration
    destination_config = destination_config.values[0]  # type: ignore[attr-defined]

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    @dlt.resource(
        table_name="dim_test",
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "validity_column_names": validity_column_names,
            "active_record_timestamp": active_record_timestamp,
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
    assert_load_info(info)
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
        {
            from_: ts_1,
            to: active_record_timestamp,
            "nk": 2,
            "c1": "bar",
            cname: "bar",
        },
        {
            from_: ts_1,
            to: active_record_timestamp,
            "nk": 1,
            "c1": "foo",
            cname: "foo",
        },
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
        {
            from_: ts_1,
            to: active_record_timestamp,
            "nk": 2,
            "c1": "bar",
            cname: "bar",
        },
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {
            from_: ts_2,
            to: active_record_timestamp,
            "nk": 1,
            "c1": "foo",
            cname: "foo_updated",
        },
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
        {
            from_: ts_2,
            to: active_record_timestamp,
            "nk": 1,
            "c1": "foo",
            cname: "foo_updated",
        },
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
        {
            from_: ts_4,
            to: active_record_timestamp,
            "nk": 3,
            "c1": "baz",
            cname: "baz",
        },
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", cname: "foo"},
        {
            from_: ts_2,
            to: active_record_timestamp,
            "nk": 1,
            "c1": "foo",
            cname: "foo_updated",
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
        {from_: ts_1, to: None, "nk": 2, "c1": "bar"},
        {from_: ts_1, to: None, "nk": 1, "c1": "foo"},
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
        {from_: ts_1, to: None, "nk": 2, "c1": "bar"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},  # updated
        {from_: ts_2, to: None, "nk": 1, "c1": "foo_updated"},  # new
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
            {from_: ts_1, to: None, "nk": 2, "c1": "bar"},
            {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},
            {from_: ts_2, to: ts_3, "nk": 1, "c1": "foo_updated"},  # updated
            {from_: ts_3, to: None, "nk": 1, "c1": "foo_updated"},  # new
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
            {from_: ts_3, to: None, "nk": 1, "c1": "foo_updated"},
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
            {from_: ts_5, to: None, "nk": 3, "c1": "baz"},  # new
            {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo"},
            {from_: ts_2, to: ts_3, "nk": 1, "c1": "foo_updated"},
            {from_: ts_3, to: None, "nk": 1, "c1": "foo_updated"},
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
    ],
)
def test_active_record_timestamp(
    destination_config: DestinationTestConfiguration,
    active_record_timestamp: Optional[TAnyDateTime],
) -> None:
    p = destination_config.setup_pipeline("abstract", dev_mode=True)

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

    p.run(r())
    actual_active_record_timestamp = ensure_pendulum_datetime(
        load_tables_to_dicts(p, "dim_test")["dim_test"][0]["_dlt_valid_to"]
    )
    assert actual_active_record_timestamp == ensure_pendulum_datetime(active_record_timestamp)


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
    info = p.run(_make_scd2_r(table), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    # make sure we have scd2 columns in schema
    table_schema = p.default_schema.get_table("tabular")
    assert table_schema["x-merge-strategy"] == "scd2"  # type: ignore[typeddict-item]
    from_, to = DEFAULT_VALIDITY_COLUMN_NAMES
    assert table_schema["columns"][from_]["x-valid-from"]  # type: ignore[typeddict-item]
    assert table_schema["columns"][to]["x-valid-to"]  # type: ignore[typeddict-item]
    assert table_schema["columns"]["row_hash"]["x-row-version"]  # type: ignore[typeddict-item]
    # 100 items in destination
    assert load_table_counts(p, "tabular")["tabular"] == 100

    # modify in place (pandas only)
    if item_type == "pandas":
        table = orig_table
        orig_table = table.copy(deep=True)
        info = p.run(_make_scd2_r(table), loader_file_format=destination_config.file_format)
        assert_load_info(info)
        # no changes (hopefully hash is deterministic)
        assert load_table_counts(p, "tabular")["tabular"] == 100

        # change single row
        orig_table.iloc[0, 0] = "Duck 🦆!"
        info = p.run(_make_scd2_r(orig_table), loader_file_format=destination_config.file_format)
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
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    ts_1 = get_load_package_created_at(p, info)
    table = p.default_schema.get_table("dim_test")
    assert table["columns"]["row_hash"]["x-row-version"]  # type: ignore[typeddict-item]
    assert "x-row-version" not in table["columns"]["_dlt_id"]

    # load 2 — update and delete a record
    dim_snap = [
        {"nk": 1, "c1": "foo_upd", "c2": [1], "row_hash": "mocked_hash_1_upd"},
    ]
    info = p.run(r(dim_snap), loader_file_format=destination_config.file_format)
    assert_load_info(info)
    ts_2 = get_load_package_created_at(p, info)

    # assert load results
    from_, to = DEFAULT_VALIDITY_COLUMN_NAMES
    assert get_table(p, "dim_test", "c1") == [
        {from_: ts_1, to: ts_2, "nk": 2, "c1": "bar", "row_hash": "mocked_hash_2"},
        {from_: ts_1, to: ts_2, "nk": 1, "c1": "foo", "row_hash": "mocked_hash_1"},
        {
            from_: ts_2,
            to: None,
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
