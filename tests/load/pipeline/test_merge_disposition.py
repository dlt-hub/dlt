from copy import copy
from datetime import date, datetime, timezone  # noqa: I251
from decimal import Decimal
import os
import pytest
import random
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple
import yaml

import dlt
from dlt.common.metrics import TDataLocation

from dlt.common import json, pendulum
from dlt.common.json import SupportsJson, _orjson, _simplejson
import dlt.common.normalizers.json.helpers as normalizer_helpers
from dlt.common.configuration.container import Container
from dlt.common.pipeline import StateInjectableContext
from dlt.common.schema.utils import has_table_seen_data
from dlt.common.schema.exceptions import (
    SchemaCorruptedException,
    UnboundColumnException,
    CannotCoerceNullException,
)
from dlt.common.schema.typing import (
    TDataType,
    TLoaderMergeStrategy,
    TTableFormat,
    TTableSchemaColumns,
)
from dlt.common.typing import StrAny
from dlt.common.utils import digest128
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import DestinationCapabilitiesException
from dlt.common.libs.pyarrow import row_tuples_to_arrow

from dlt.extract import DltResource, DltSource
from dlt.extract.hints import TResourceHints
from dlt.extract.utils import digest_dedup_value
from dlt.sources.helpers.transform import skip_first, take_first
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.normalize.exceptions import NormalizeJobFailed

from tests.load.pipeline.merge_utils import (
    LOADED_RECORDS,
    NEW_BUCKET,
    NEW_BUCKET_SUBQUERY,
    PARTITION,
    PARTITION_LOCAL_KEY,
    PARTITIONED_INPUT,
    PARTITIONED_TARGET,
    STORED_RECORDS,
    assert_nested_rows_have_parents,
    load_ids_by_key,
    merge_condition,
    merge_resource,
)
from tests.load.pipeline.utils import LOCAL_DESTINATIONS, skip_if_unsupported_merge_strategy
from tests.pipeline.utils import (
    assert_load_info,
    load_table_counts,
    select_data,
    load_tables_to_dicts,
    assert_records_as_set,
)
from tests.load.utils import (
    AWS_BUCKET,
    normalize_storage_table_cols,
    destinations_configs,
    DestinationTestConfiguration,
    FILE_BUCKET,
    ABFS_BUCKET,
)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
        supports_merge=True,
        bucket_subset=(FILE_BUCKET, AWS_BUCKET),  # test one local, one remote
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_merge_on_keys_in_schema_nested_hints(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    """Tests merge disposition on an annotated schema, no annotations on resource"""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    p = destination_config.setup_pipeline("eth_2", dev_mode=True)

    with open("tests/common/cases/schemas/eth/ethereum_schema_v11.yml", "r", encoding="utf-8") as f:
        schema = dlt.Schema.from_dict(yaml.safe_load(f))

    if destination_config.destination_type == "databricks":
        # remove `partition` hint because it conflicts with `cluster` on databricks
        schema.merge_hints({"partition": []}, replace=True)

    if destination_config.destination_type == "clickhouse":
        # remove `partition` hint because it conflicts with `nullable` on clickhouse
        schema.merge_hints({"partition": []}, replace=True)
        # remove `sort` hints because it conflicts with `primary_key` on clickhouse
        for table in schema.tables.values():
            for column in table["columns"].values():
                if "sort" in column:
                    del column["sort"]

    # make block uncles unseen to trigger filtering loader in loader for nested tables
    if has_table_seen_data(schema.tables["blocks__uncles"]):
        del schema.tables["blocks__uncles"]["x-normalizer"]
        assert not has_table_seen_data(schema.tables["blocks__uncles"])

    hints: TResourceHints = {
        "write_disposition": {"disposition": "merge", "strategy": merge_strategy},
        "table_format": destination_config.table_format,
    }
    # NOTE: setting primary key will break nesting chain
    nested_hints = {
        ("transactions",): {**hints, "primary_key": ("block_number", "transaction_index")},
        ("transactions", "logs"): {
            **hints,
            "primary_key": ("block_number", "transaction_index", "log_index"),
        },
    }

    @dlt.source(schema=schema)
    def ethereum(slice_: slice = None, duplicates: int = 0):
        @dlt.resource(**hints, nested_hints=nested_hints)  # type: ignore[call-overload]
        def blocks():
            for _ in range(duplicates + 1):
                with open(
                    "tests/normalize/cases/ethereum.blocks.9c1d9b504ea240a482b007788d5cd61c_2.json",
                    "r",
                    encoding="utf-8",
                ) as f:
                    yield json.load(f) if slice_ is None else json.load(f)[slice_]

        return blocks()

    # take only the first block. the first block does not have uncles so this table should not be created and merged
    info = p.run(
        ethereum(slice(1)),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    eth_1_counts = load_table_counts(p, "blocks")
    # we load a single block
    assert eth_1_counts["blocks"] == 1
    # check root key propagation
    assert (
        p.default_schema.tables["blocks__transactions"]["columns"]["_dlt_root_id"]["root_key"]
        is True
    )
    # now we load the whole dataset. blocks should be created which adds columns to blocks
    # if the table would be created before the whole load would fail because new columns have hints
    info = p.run(
        ethereum(),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    eth_2_counts = load_table_counts(p)
    # we have 2 blocks in dataset
    assert eth_2_counts["blocks"] == 2 if destination_config.supports_merge else 3
    # make sure we have same record after merging full dataset again
    info = p.run(
        ethereum(),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    eth_3_counts = load_table_counts(p)
    assert eth_2_counts == eth_3_counts


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
        bucket_subset=(FILE_BUCKET,),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_merge_record_updates(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    p = destination_config.setup_pipeline("test_merge_record_updates", dev_mode=True)

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key="id",
    )
    def r(data):
        yield data

    # initial load, also use primary key is that must be normalized "ID" -> "id"
    run_1 = [
        {"ID": 1, "foo": 1, "empty_col": None, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
        {"ID": 2, "foo": 1, "empty_col": None, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_1), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 2,
        "parent__child": 2,
        "parent__child__grandchild": 2,
    }
    tables = load_tables_to_dicts(p, "parent", exclude_system_cols=True)
    assert_records_as_set(
        tables["parent"],
        [
            {"id": 1, "foo": 1},
            {"id": 2, "foo": 1},
        ],
    )

    # update record — change at parent level
    run_2 = [
        {"id": 1, "foo": 2, "child": [{"bar": 1, "empty_col": None, "grandchild": [{"baz": 1}]}]},
        {"id": 2, "foo": 1, "child": [{"bar": 1, "empty_col": None, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_2), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 2,
        "parent__child": 2,
        "parent__child__grandchild": 2,
    }
    tables = load_tables_to_dicts(p, "parent", exclude_system_cols=True)
    assert_records_as_set(
        tables["parent"],
        [
            {"id": 1, "foo": 2},
            {"id": 2, "foo": 1},
        ],
    )

    # update record — change at child level
    run_3 = [
        {"id": 1, "foo": 2, "child": [{"bar": 2, "grandchild": [{"baz": 1}]}]},
        {"id": 2, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_3), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 2,
        "parent__child": 2,
        "parent__child__grandchild": 2,
    }
    tables = load_tables_to_dicts(p, "parent", "parent__child", exclude_system_cols=True)
    assert_records_as_set(
        tables["parent__child"],
        [
            {"bar": 2},
            {"bar": 1},
        ],
    )

    # update record — change at grandchild level
    run_3 = [
        {"id": 1, "foo": 2, "child": [{"bar": 2, "grandchild": [{"baz": 2}]}]},
        {"id": 2, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_3), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 2,
        "parent__child": 2,
        "parent__child__grandchild": 2,
    }
    tables = load_tables_to_dicts(p, "parent__child__grandchild", exclude_system_cols=True)
    assert_records_as_set(
        tables["parent__child__grandchild"],
        [
            {"baz": 2},
            {"baz": 1},
        ],
    )


class _PKCase(NamedTuple):
    """A primary key shape, the rows to load and the ids `upsert` derives from them."""

    primary_key: Tuple[str, ...]
    rows: List[Dict[str, Any]]
    values: List[Dict[str, Any]]
    """Expected parent columns, without the dlt system ones, in the order of `rows`."""
    row_ids: List[str]
    child_ids: List[str]
    grandchild_ids: List[str]


# NOTE: the ids below are hashed from the primary key values, they must NEVER change or already
# loaded user data breaks. a new case gets its ids by loading it on `devel`
MERGE_PK_CASES = {
    "text": _PKCase(
        primary_key=("id", "TxId"),
        rows=[
            {"ID": 1, "TxId": "tx1🚀3", "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
            {"ID": 1, "TxId": "tx2Ü+😎üß", "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
        ],
        values=[
            {"id": 1, "tx_id": "tx1🚀3"},
            {"id": 1, "tx_id": "tx2Ü+😎üß"},
        ],
        row_ids=["19GAeLNivYhDqg", "wg/EKJyC+CXo/g"],
        child_ids=["hhWFugO13PeLBQ", "U+o+nLvOjp5DLA"],
        grandchild_ids=["rsgPU8Q0nJ4QVg", "8anfRKWjVjQ7ag"],
    ),
    # a key spanning every type that the typed json encoding renders as a string
    "typed": _PKCase(
        primary_key=("id", "TxId", "Ts", "Dt", "Amount", "Flag"),
        rows=[
            {
                "ID": 1,
                "TxId": "tx1🚀3",
                "Ts": datetime(2024, 1, 15, 23, 30, tzinfo=timezone.utc),
                "Dt": date(2024, 1, 15),
                "Amount": Decimal("10.05"),
                "Flag": True,
                "child": [{"bar": 1, "grandchild": [{"baz": 1}]}],
            },
            {
                "ID": 1,
                "TxId": "tx2Ü+😎üß",
                "Ts": datetime(2024, 7, 1, 0, 0, 0, 123456, tzinfo=timezone.utc),
                "Dt": date(2024, 12, 31),
                "Amount": Decimal("-0.10"),
                "Flag": False,
                "child": [{"bar": 1, "grandchild": [{"baz": 1}]}],
            },
        ],
        values=[
            {
                "id": 1,
                "tx_id": "tx1🚀3",
                "ts": pendulum.datetime(2024, 1, 15, 23, 30),
                "dt": date(2024, 1, 15),
                "amount": Decimal("10.05"),
                "flag": True,
            },
            {
                "id": 1,
                "tx_id": "tx2Ü+😎üß",
                "ts": pendulum.datetime(2024, 7, 1, 0, 0, 0, 123456),
                "dt": date(2024, 12, 31),
                "amount": Decimal("-0.10"),
                "flag": False,
            },
        ],
        row_ids=["QUWLSdBfS6aB2g", "NS9eWna9NpHzFw"],
        child_ids=["zlp9QDlD5sxDhQ", "4IBkbTTIXUXZjA"],
        grandchild_ids=["oRxaMoleNwowSA", "jrXdzCKAWhBbfw"],
    ),
}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
        subset=("postgres", "snowflake", "filesystem", "iceberg"),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
@pytest.mark.parametrize("pk_case", MERGE_PK_CASES.values(), ids=list(MERGE_PK_CASES))
@pytest.mark.parametrize("json_impl", (_orjson, _simplejson), ids=("orjson", "simplejson"))
def test_merge_primary_key_normalization(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
    pk_case: _PKCase,
    json_impl: SupportsJson,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    if merge_strategy == "delete-insert" and json_impl is _simplejson:
        pytest.skip("the json impl only reaches the row id that `upsert` derives from the key")
    # the row id is hashed from a json dump of the key, so both impls must yield the same id
    monkeypatch.setattr(normalizer_helpers, "json", json_impl)
    p = destination_config.setup_pipeline("test_merge_record_updates", dev_mode=True)

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key=pk_case.primary_key,
    )
    def r(data):
        yield data

    # initial load, here we check a bug where normalized primary_key columns were checked against
    # not normalized source data, missing values were ignored leading to duplicate keys
    # on postgres we have PK violation
    info = p.run(r(pk_case.rows), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 2,
        "parent__child": 2,
        "parent__child__grandchild": 2,
    }
    if merge_strategy == "delete-insert":
        tables = load_tables_to_dicts(p, "parent", exclude_system_cols=True)
        assert_records_as_set(tables["parent"], pk_case.values)
    else:
        tables = load_tables_to_dicts(
            p, "parent", "parent__child", "parent__child__grandchild", exclude_system_cols=False
        )
        # remove _dlt_load_id
        parent_data = [
            {k: v for k, v in parent_dict.items() if k != "_dlt_load_id"}
            for parent_dict in tables["parent"]
        ]
        # _dlt_id is created deterministically from values of the PK so we can hardcode it
        # NOTE: this should NEVER change or you will break user data that is already loaded
        assert_records_as_set(
            parent_data,
            [
                {**values, "_dlt_id": row_id}
                for values, row_id in zip(pk_case.values, pk_case.row_ids)
            ],
        )
        child_data = [
            {k: v for k, v in dict_.items() if k != "_dlt_load_id"}
            for dict_ in tables["parent__child"]
        ]
        assert_records_as_set(
            child_data,
            [
                {
                    "bar": 1,
                    "_dlt_root_id": row_id,
                    "_dlt_parent_id": row_id,
                    "_dlt_list_idx": 0,
                    "_dlt_id": child_id,
                }
                for row_id, child_id in zip(pk_case.row_ids, pk_case.child_ids)
            ],
        )
        # grandchild refers to child via parent_id and to root table via root_id, all ids are deterministic for upsert
        grandchild_data = [
            {k: v for k, v in dict_.items() if k != "_dlt_load_id"}
            for dict_ in tables["parent__child__grandchild"]
        ]
        assert_records_as_set(
            grandchild_data,
            [
                {
                    "baz": 1,
                    "_dlt_root_id": row_id,
                    "_dlt_parent_id": child_id,
                    "_dlt_list_idx": 0,
                    "_dlt_id": grandchild_id,
                }
                for row_id, child_id, grandchild_id in zip(
                    pk_case.row_ids, pk_case.child_ids, pk_case.grandchild_ids
                )
            ],
        )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_merge_nested_records_inserted_deleted(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    p = destination_config.setup_pipeline(
        "test_merge_nested_records_inserted_deleted", dev_mode=True
    )

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key="id",
        merge_key="foo",
    )
    def r(data):
        yield data

    # initial load
    run_1 = [
        {"id": 1, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
        {"id": 2, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
        {"id": 3, "foo": 1, "child": [{"bar": 3, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_1), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 3,
        "parent__child": 3,
        "parent__child__grandchild": 3,
    }
    tables = load_tables_to_dicts(p, "parent", exclude_system_cols=True)
    assert_records_as_set(
        tables["parent"],
        [
            {"id": 1, "foo": 1},
            {"id": 2, "foo": 1},
            {"id": 3, "foo": 1},
        ],
    )

    # delete records — delete parent (id 3), child (id 2) and grandchild (id 1)
    # foo is merge key, should delete id = 3
    run_3 = [
        {"id": 1, "foo": 1, "child": [{"bar": 2}]},
        {"id": 2, "foo": 1},
    ]
    info = p.run(r(run_3), **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts = load_table_counts(p, "parent", "parent__child", "parent__child__grandchild")
    table_data = load_tables_to_dicts(p, "parent", "parent__child", exclude_system_cols=True)
    if merge_strategy == "upsert":
        # merge keys will not apply and parent will not be deleted
        if (
            destination_config.table_format in ["delta", "iceberg"]
            and destination_config.destination_type != "athena"
        ):
            # delta merges cannot delete from nested tables
            assert table_counts == {
                "parent": 3,  # id == 3 not deleted (not present in the data)
                "parent__child": 3,  # child not deleted
                "parent__child__grandchild": 3,  # grand child not deleted,
            }
        else:
            assert table_counts == {
                "parent": 3,  # id == 3 not deleted (not present in the data)
                "parent__child": 2,
                "parent__child__grandchild": 1,
            }
            assert_records_as_set(
                table_data["parent__child"],
                [
                    {"bar": 2},  # id 1 updated to bar
                    {"bar": 3},  # id 3 not deleted
                ],
            )
    else:
        assert table_counts == {
            "parent": 2,
            "parent__child": 1,
            "parent__child__grandchild": 0,
        }
        assert_records_as_set(
            table_data["parent__child"],
            [
                {"bar": 2},
            ],
        )

    # insert records id 3 inserted back, id 2 added child, id 1 added grandchild
    run_3 = [
        {"id": 1, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}, {"baz": 4}]}]},
        {"id": 2, "foo": 1, "child": [{"bar": 2, "grandchild": [{"baz": 2}]}, {"bar": 4}]},
        {"id": 3, "foo": 1, "child": [{"bar": 3, "grandchild": [{"baz": 3}]}]},
    ]
    info = p.run(r(run_3), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, "parent", "parent__child", "parent__child__grandchild") == {
        "parent": 3,
        "parent__child": 4,
        "parent__child__grandchild": 4,
    }
    tables = load_tables_to_dicts(
        p, "parent__child", "parent__child__grandchild", exclude_system_cols=True
    )
    assert_records_as_set(
        tables["parent__child__grandchild"],
        [
            {"baz": 2},
            {"baz": 1},
            {"baz": 3},
            {"baz": 4},
        ],
    )
    assert_records_as_set(
        tables["parent__child"],
        [
            {"bar": 2},
            {"bar": 1},
            {"bar": 3},
            {"bar": 4},
        ],
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_bring_your_own_dlt_id(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    p = destination_config.setup_pipeline(
        "test_merge_nested_records_inserted_deleted", dev_mode=True
    )

    # sets _dlt_id as both primary key and row key.
    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key="_dlt_id",
    )
    def r(data):
        yield data

    # initial load
    run_1 = [
        {"_dlt_id": 1, "foo": 1, "child": [{"bar": 1, "grandchild": [{"baz": 1}]}]},
    ]
    info = p.run(r(run_1), **destination_config.run_kwargs)
    assert_load_info(info)
    run_2 = [
        {"_dlt_id": 1, "foo": 2, "child": [{"bar": 2, "grandchild": [{"baz": 2}]}]},
    ]
    info = p.run(r(run_2), **destination_config.run_kwargs)
    assert_load_info(info)
    # _dlt_id is a bigint and a primary key
    parent_dlt_id = p.default_schema.tables["parent"]["columns"]["_dlt_id"]
    assert parent_dlt_id["data_type"] == "bigint"
    assert parent_dlt_id["primary_key"] is True
    assert parent_dlt_id["row_key"] is True
    assert parent_dlt_id["unique"] is True

    # parent_key on child refers to the dlt_id above
    child_parent_id = p.default_schema.tables["parent__child"]["columns"]["_dlt_parent_id"]
    assert child_parent_id["data_type"] == "bigint"
    assert child_parent_id["parent_key"] is True

    # same for root key
    child_root_id = p.default_schema.tables["parent__child"]["columns"]["_dlt_root_id"]
    assert child_root_id["data_type"] == "bigint"
    assert child_root_id["root_key"] is True

    # id on child is regular auto dlt id
    child_dlt_id = p.default_schema.tables["parent__child"]["columns"]["_dlt_id"]
    assert child_dlt_id["data_type"] == "text"

    # check grandchild
    grandchild_parent_id = p.default_schema.tables["parent__child__grandchild"]["columns"][
        "_dlt_parent_id"
    ]
    # refers to child dlt id which is a regular one
    assert grandchild_parent_id["data_type"] == "text"
    assert grandchild_parent_id["parent_key"] is True

    grandchild_root_id = p.default_schema.tables["parent__child__grandchild"]["columns"][
        "_dlt_root_id"
    ]
    # root key still to parent
    assert grandchild_root_id["data_type"] == "bigint"
    assert grandchild_root_id["root_key"] is True

    table_data = load_tables_to_dicts(
        p, "parent", "parent__child", "parent__child__grandchild", exclude_system_cols=False
    )
    # drop dlt load id
    del table_data["parent"][0]["_dlt_load_id"]
    # all the ids are deterministic: on parent is set by the user, on child - is derived from parent
    assert table_data == {
        "parent": [{"_dlt_id": 1, "foo": 2}],
        "parent__child": [
            {
                "bar": 2,
                "_dlt_root_id": 1,
                "_dlt_parent_id": 1,
                "_dlt_list_idx": 0,
                "_dlt_id": "mvMThji/REOKKA",
            }
        ],
        "parent__child__grandchild": [
            {
                "baz": 2,
                "_dlt_root_id": 1,
                "_dlt_parent_id": "mvMThji/REOKKA",
                "_dlt_list_idx": 0,
                "_dlt_id": "KKZaBWTgbZd74A",
            }
        ],
    }


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_merge_on_ad_hoc_primary_key(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    p = destination_config.setup_pipeline("github_1", dev_mode=True)

    @dlt.resource(
        table_name="issues",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key="NodeId",
        table_format=destination_config.table_format,
    )
    def data(slice_: slice = None):
        with open(
            "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
        ) as f:
            yield json.load(f) if slice_ is None else json.load(f)[slice_]

    # note: NodeId will be normalized to "node_id" which exists in the schema
    info = p.run(data(slice(0, 17)), **destination_config.run_kwargs)
    assert_load_info(info)
    github_1_counts = load_table_counts(p)
    # 17 issues
    assert github_1_counts["issues"] == 17
    # primary key set on issues
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["primary_key"] is True
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["data_type"] == "text"
    assert p.default_schema.tables["issues"]["columns"]["node_id"]["nullable"] is False

    info = p.run(data(slice(5, None)), **destination_config.run_kwargs)
    assert_load_info(info)
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    github_2_counts = load_table_counts(p)
    # 100 issues total
    assert github_2_counts["issues"] == 100
    # still 100 after the reload


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True, subset=LOCAL_DESTINATIONS),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "insert-only"))
def test_merge_nested_tables_without_root_key(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    """Merges nested data without `_dlt_root_id` when the merge deletes no nested rows.
    Without keys, `delete-insert` appends, and `insert-only` merges nested tables by row key."""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    @dlt.source(root_key=False)
    def nested_source():
        @dlt.resource(
            primary_key="id" if merge_strategy == "insert-only" else None,
            write_disposition={"disposition": "merge", "strategy": merge_strategy},
        )
        def items():
            yield [{"id": i, "children": [{"a": i, "sub": [{"b": i}]}]} for i in range(2)]

        return items

    p = destination_config.setup_pipeline("merge_no_root_key", dev_mode=True)
    for _ in range(2):
        assert_load_info(p.run(nested_source(), **destination_config.run_kwargs))
    assert "_dlt_root_id" not in p.default_schema.tables["items__children__sub"]["columns"]
    expected = 4 if merge_strategy == "delete-insert" else 2
    assert load_table_counts(p, "items", "items__children", "items__children__sub") == {
        "items": expected,
        "items__children": expected,
        "items__children__sub": expected,
    }


@dlt.source(root_key=True)
def github():
    @dlt.resource(
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
        merge_key=("node_id", "url"),
    )
    def load_issues():
        with open(
            "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
        ) as f:
            for item in json.load(f):
                yield item

    issues = load_issues()
    # a captured page of the github api, so the trace carries the read side of the run too
    issues.add_input(
        TDataLocation(
            kind="rest_api",
            resource_name=issues.name,
            location="https://api.github.com",
        )
    )
    return issues


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_merge_source_compound_keys_and_changes(
    destination_config: DestinationTestConfiguration,
) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)

    info = p.run(github(), **destination_config.run_kwargs)
    assert_load_info(info)
    github_1_counts = load_table_counts(p)
    # 100 issues total
    assert github_1_counts["issues"] == 100
    # check keys created
    assert (
        p.default_schema.tables["issues"]["columns"]["node_id"].items()
        > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    )
    assert (
        p.default_schema.tables["issues"]["columns"]["url"].items()
        > {"merge_key": True, "data_type": "text", "nullable": False}.items()
    )
    assert (
        p.default_schema.tables["issues"]["columns"]["id"].items()
        > {"primary_key": True, "data_type": "bigint", "nullable": False}.items()
    )

    # append load_issues resource
    info = p.run(github().load_issues, write_disposition="append", **destination_config.run_kwargs)
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "append"
    # the counts of all tables must be double
    github_2_counts = load_table_counts(p)
    assert {k: v * 2 for k, v in github_1_counts.items()} == github_2_counts

    # now replace all resources
    info = p.run(github(), write_disposition="replace", **destination_config.run_kwargs)
    assert_load_info(info)
    assert p.default_schema.tables["issues"]["write_disposition"] == "replace"
    # assert p.default_schema.tables["issues__labels"]["write_disposition"] == "replace"
    # the counts of all tables must be double
    github_3_counts = load_table_counts(p)
    assert github_1_counts == github_3_counts


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_merge_no_child_tables(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    github_data = github()
    assert github_data.max_table_nesting is None
    assert github_data.root_key is True
    # set max nesting to 0 so no child tables are generated
    github_data.max_table_nesting = 0
    assert github_data.max_table_nesting == 0
    github_data.root_key = False
    assert github_data.root_key is False

    # take only first 15 elements
    github_data.load_issues.add_filter(take_first(15))
    info = p.run(github_data, **destination_config.run_kwargs)
    assert len(p.default_schema.data_tables()) == 1
    assert "issues" in p.default_schema.tables
    assert_load_info(info)
    github_1_counts = load_table_counts(p)
    assert github_1_counts["issues"] == 15

    # load all
    github_data = github()
    github_data.max_table_nesting = 0
    info = p.run(github_data, **destination_config.run_kwargs)
    assert_load_info(info)
    github_2_counts = load_table_counts(p)
    # 100 issues total, or 115 if merge is not supported
    assert github_2_counts["issues"] == 100 if destination_config.supports_merge else 115


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_merge_no_merge_keys(destination_config: DestinationTestConfiguration) -> None:
    # NOTE: we can test filesystem destination merge behavior here too, will also fallback!
    if destination_config.file_format == "insert_values":
        pytest.skip("Insert values row count checking is buggy, skipping")
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(skip_first(45))
    info = p.run(github_data, **destination_config.run_kwargs)
    assert_load_info(info)
    github_1_counts = load_table_counts(p)
    assert github_1_counts["issues"] == 100 - 45

    # take first 10 rows.
    github_data = github()
    # remove all keys
    github_data.load_issues.apply_hints(merge_key=(), primary_key=())
    # skip first 45 rows
    github_data.load_issues.add_filter(take_first(10))
    info = p.run(github_data, **destination_config.run_kwargs)
    assert_load_info(info)
    github_1_counts = load_table_counts(p)
    # we have 10 rows more, merge falls back to append if no keys present
    assert github_1_counts["issues"] == 100 - 45 + 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        with_file_format="parquet",
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
def test_pipeline_load_parquet(destination_config: DestinationTestConfiguration) -> None:
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    # do not save state to destination so jobs counting is easier
    p.config.restore_from_destination = False
    github_data = github()
    # generate some nested types
    github_data.max_table_nesting = 2
    github_data_copy = github()
    github_data_copy.max_table_nesting = 2
    # iceberg filesystem requires input data without duplicates
    if (
        destination_config.table_format == "iceberg"
        and destination_config.destination_type == "filesystem"
    ):
        info = p.run(
            github_data,
            write_disposition="merge",
            **destination_config.run_kwargs,
        )
    else:
        info = p.run(
            [github_data, github_data_copy],
            write_disposition="merge",
            **destination_config.run_kwargs,
        )
    assert_load_info(info)
    # make sure it was parquet or sql transforms
    expected_formats = ["parquet"]
    if p.staging or destination_config.table_format:
        # allow references if staging is present
        expected_formats.append("reference")
    files = p.get_load_package_info(p.list_completed_load_packages()[0]).jobs["completed_jobs"]
    assert all(f.job_file_info.file_format in expected_formats + ["sql"] for f in files)

    github_1_counts = load_table_counts(p)
    expected_rows = 100
    # if table_format is set to delta we use upsert which does not deduplicate input data
    # otherwise the data is either deduplicated or it's iceberg filesystem for which we didn't pass duplicates at all
    if destination_config.table_format == "delta":
        expected_rows *= 2
    assert github_1_counts["issues"] == expected_rows

    # now retry with replace
    github_data = github()
    # generate some nested types
    github_data.max_table_nesting = 2
    info = p.run(
        github_data,
        write_disposition="replace",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # make sure it was parquet or sql inserts
    files = p.get_load_package_info(p.list_completed_load_packages()[1]).jobs["completed_jobs"]
    if (
        destination_config.destination_type == "athena"
        and destination_config.table_format == "iceberg"
    ):
        # iceberg uses sql to copy tables
        expected_formats.append("sql")
    assert all(f.job_file_info.file_format in expected_formats for f in files)

    github_1_counts = load_table_counts(p)
    assert github_1_counts["issues"] == 100


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=("postgres", "athena", "sqlalchemy"),
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("max_table_nesting", (0, 1))
def test_pipeline_disable_deduplication(
    destination_config: DestinationTestConfiguration, max_table_nesting: int
) -> None:
    pipeline = destination_config.setup_pipeline("github_3", dev_mode=True)
    # do not save state to destination so jobs counting is easier
    pipeline.config.restore_from_destination = False
    github_data = github()
    # generate some nested types
    github_data.max_table_nesting = max_table_nesting
    github_data_copy = github()
    github_data_copy.max_table_nesting = max_table_nesting

    # disable deduplication
    pipeline.run(
        [github_data, github_data_copy],
        write_disposition={
            "disposition": "merge",
            "strategy": "delete-insert",
            "deduplicated": True,
        },
        **destination_config.run_kwargs,
    )
    github_1_counts = load_table_counts(pipeline)
    # dedup disabled
    assert github_1_counts["issues"] == 200
    # make sure we get expected number of tables
    assert len(github_1_counts) == 1 if max_table_nesting == 0 else 3
    if max_table_nesting == 1:
        assert github_1_counts["issues__labels"] == 68
        assert github_1_counts["issues__assignees"] == 62


@dlt.transformer(
    name="github_repo_events",
    primary_key="id",
    write_disposition="merge",
    table_name=lambda i: i["type"],
)
def github_repo_events(
    page: List[StrAny],
    last_created_at=dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z"),
):
    """A transformer taking a stream of github events and dispatching them to tables named by event type. Deduplicates be 'id'. Loads incrementally by 'created_at'"""
    yield page


@dlt.transformer(name="github_repo_events", primary_key="id", write_disposition="merge")
def github_repo_events_table_meta(
    page: List[StrAny],
    last_created_at=dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z"),
):
    """A transformer taking a stream of github events and dispatching them to tables using table meta. Deduplicates be 'id'. Loads incrementally by 'created_at'"""
    yield from [dlt.mark.with_table_name(p, p["type"]) for p in page]


@dlt.resource
def _get_shuffled_events(shuffle: bool = dlt.secrets.value):
    with open(
        "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
    ) as f:
        issues = json.load(f)
        # random order
        if shuffle:
            random.shuffle(issues)
        yield issues


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("github_resource", [github_repo_events, github_repo_events_table_meta])
def test_merge_with_dispatch_and_incremental(
    destination_config: DestinationTestConfiguration, github_resource: DltResource
) -> None:
    if destination_config.destination_name == "sqlalchemy_mysql":
        # TODO: Github events have too many columns for MySQL
        pytest.skip("MySQL can't handle too many columns")

    newest_issues = list(
        sorted(_get_shuffled_events(True), key=lambda x: x["created_at"], reverse=True)
    )
    newest_issue = newest_issues[0]

    @dlt.resource
    def _new_event(node_id):
        new_i = copy(newest_issue)
        new_i["id"] = str(random.randint(0, 2 ^ 32))
        new_i["created_at"] = pendulum.now().isoformat()
        new_i["node_id"] = node_id
        # yield pages
        yield [new_i]

    @dlt.resource
    def _updated_event(node_id):
        new_i = copy(newest_issue)
        new_i["created_at"] = pendulum.now().isoformat()
        new_i["node_id"] = node_id
        # yield pages
        yield [new_i]

    # inject state that we can inspect and will be shared across calls
    with Container().injectable_context(StateInjectableContext(state={})):
        assert len(list(_get_shuffled_events(True) | github_resource)) == 100
        incremental_state = github_resource.state
        assert (
            incremental_state["incremental"]["created_at"]["last_value"]
            == newest_issue["created_at"]
        )
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] == [
            digest_dedup_value(newest_issue["id"])
        ]
        # subsequent load will skip all elements
        assert len(list(_get_shuffled_events(True) | github_resource)) == 0
        # add one more issue
        assert len(list(_new_event("new_node") | github_resource)) == 1
        assert (
            incremental_state["incremental"]["created_at"]["last_value"]
            > newest_issue["created_at"]
        )
        assert incremental_state["incremental"]["created_at"]["unique_hashes"] != [
            digest_dedup_value(newest_issue["id"])
        ]

    # load to destination
    p = destination_config.setup_pipeline("github_3", dev_mode=True)
    info = p.run(
        _get_shuffled_events(True) | github_resource,
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # get top tables
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    # total number of events in all top tables == 100
    assert sum(counts.values()) == 100
    # this should skip all events due to incremental load
    info = p.run(
        _get_shuffled_events(True) | github_resource,
        **destination_config.run_kwargs,
    )
    assert len(info.loads_ids) == 0

    # load one more event with a new id
    info = p.run(_new_event("new_node") | github_resource, **destination_config.run_kwargs)
    assert_load_info(info)
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    assert sum(counts.values()) == 101
    # all the columns have primary keys and merge disposition derived from resource
    for table in p.default_schema.data_tables():
        if table.get("parent") is None:
            assert table["write_disposition"] == "merge"
            assert table["columns"]["id"]["primary_key"] is True

    # load updated event
    info = p.run(
        _updated_event("new_node_X") | github_resource,
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    # still 101
    counts = load_table_counts(
        p, *[t["name"] for t in p.default_schema.data_tables() if t.get("parent") is None]
    )
    assert sum(counts.values()) == 101 if destination_config.supports_merge else 102
    # for non merge destinations we just check that the run passes
    if not destination_config.supports_merge:
        return
    # but we have it updated
    with p.sql_client() as c:
        qual_name = c.make_qualified_table_name("watch_event")
        with c.execute_query(f"SELECT node_id FROM {qual_name} WHERE node_id = 'new_node_X'") as q:
            assert len(list(q.fetchall())) == 1


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("key_hint", ["primary_key", "merge_key"])
def test_deduplicate_single_load(
    destination_config: DestinationTestConfiguration, key_hint: str
) -> None:
    """`delete-insert` deduplicates the staged rows by `primary_key` only, never by `merge_key`."""
    p = destination_config.setup_pipeline("abstract", dev_mode=True)
    dedup = key_hint == "primary_key" and destination_config.supports_merge
    hints: Any = {key_hint: "id"}

    @dlt.resource(write_disposition="merge", **hints)
    def duplicates():
        yield [
            {"id": 1, "name": "row1", "child": [1, 2, 3]},
            {"id": 1, "name": "row2", "child": [4, 5, 6]},
        ]

    info = p.run(duplicates(), **destination_config.run_kwargs)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates", "duplicates__child")
    assert counts["duplicates"] == (1 if dedup else 2)
    assert counts["duplicates__child"] == (3 if dedup else 6)

    compound_hints: Any = {key_hint: ("id", "subkey")}

    @dlt.resource(write_disposition="merge", **compound_hints)
    def duplicates_no_child():
        yield [{"id": 1, "subkey": "AX", "name": "row1"}, {"id": 1, "subkey": "AX", "name": "row2"}]

    info = p.run(duplicates_no_child(), **destination_config.run_kwargs)
    assert_load_info(info)
    counts = load_table_counts(p, "duplicates_no_child")
    assert counts["duplicates_no_child"] == (1 if dedup else 2)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_nested_column_missing(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    if destination_config.table_format:
        pytest.skip(
            "Record updates that involve removing elements from a nested"
            " column is not supported for open table destinations."
        )
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)

    table_name = "test_nested_column_missing"

    @dlt.resource(
        name=table_name,
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        primary_key="id",
        table_format=destination_config.table_format,
    )
    def r(data):
        yield data

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    data = [
        {"id": 1, "simple": "foo", "nested": [1, 2, 3]},
        {"id": 2, "simple": "foo", "nested": [1, 2]},
    ]
    info = p.run(r(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2
    assert load_table_counts(p, table_name + "__nested")[table_name + "__nested"] == 5

    # nested column is missing, previously inserted records should be deleted from child table
    data = [
        {"id": 1, "simple": "bar"},
    ]
    info = p.run(r(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2
    assert load_table_counts(p, table_name + "__nested")[table_name + "__nested"] == 2


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("key_type", ["primary_key", "merge_key", "no_key"])
@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
@pytest.mark.parametrize("hard_delete_type", ["bool", "timestamp"])
def test_hard_delete_hint(
    destination_config: DestinationTestConfiguration,
    key_type: str,
    merge_strategy: TLoaderMergeStrategy,
    hard_delete_type: TDataType,
) -> None:
    """A `bool` column deletes only with `True`, any other column type with every non-NULL
    value. Without keys, flagged records cannot be matched and nothing is deleted."""
    if merge_strategy == "upsert" and key_type != "primary_key":
        pytest.skip("`upsert` merge strategy requires `primary_key`")
    if hard_delete_type == "timestamp" and key_type != "primary_key":
        pytest.skip("The column type does not depend on the key")
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    if (
        destination_config.table_format == "iceberg"
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip("pyiceberg `upsert` does not support the `hard_delete` hint")

    def flag(deleted: Optional[bool]) -> Any:
        if hard_delete_type == "bool":
            return deleted
        return "2024-02-15T17:16:53Z" if deleted else None

    columns: TTableSchemaColumns = {
        "deleted": (
            {"hard_delete": True}
            if hard_delete_type == "bool"
            else {"hard_delete": True, "data_type": "timestamp", "nullable": True}
        )
    }
    table_name = "test_hard_delete_hint"

    @dlt.resource(
        name=table_name,
        table_format=destination_config.table_format,
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        columns=columns,
    )
    def data_resource(data):
        yield data

    if key_type == "primary_key":
        data_resource.apply_hints(primary_key="id", merge_key="")
    elif key_type == "merge_key":
        data_resource.apply_hints(primary_key="", merge_key="id")

    p = destination_config.setup_pipeline(f"abstract_{key_type}", dev_mode=True)

    # insert two records
    data = [
        {"id": 1, "val": "foo", "deleted": flag(False)},
        {"id": 2, "val": "bar", "deleted": flag(False)},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2

    # delete one record
    data = [
        {"id": 1, "deleted": flag(True)},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == (1 if key_type != "no_key" else 2)

    # update one record (None for hard_delete column is treated as "not True")
    data = [
        {"id": 2, "val": "baz", "deleted": flag(None)},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == (1 if key_type != "no_key" else 3)

    # compare observed records with expected records
    if key_type != "no_key":
        observed = [
            {"id": row[0], "val": row[1], "deleted": row[2]}
            for row in select_data(p, f"SELECT id, val, deleted FROM {table_name}")
        ]
        expected = [{"id": 2, "val": "baz", "deleted": None}]
        assert sorted(observed, key=lambda d: d["id"]) == expected

    # insert two records with same key
    data = [
        {"id": 3, "val": "foo", "deleted": flag(False)},
        {"id": 3, "val": "bar", "deleted": flag(False)},
    ]
    if merge_strategy == "upsert":
        del data[0]  # `upsert` requires unique `primary_key`
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    counts = load_table_counts(p, table_name)[table_name]
    if key_type == "primary_key":
        assert counts == 2
    elif key_type == "merge_key":
        assert counts == 3
    elif key_type == "no_key":
        assert counts == 5

    # we do not need to test "no_key" further
    if key_type == "no_key":
        return

    # delete one key, resulting in one (primary key) or two (merge key) deleted records
    data = [
        {"id": 3, "val": "foo", "deleted": flag(True)},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # Delta rejects `hard_delete` with nested tables: test_merge_rejected_before_load
    if destination_config.table_format != "delta":
        table_name = "test_hard_delete_hint_nested"
        data_resource.apply_hints(table_name=table_name)

        # insert two records with childs and grandchilds
        data = [
            {
                "id": 1,
                "child_1": ["foo", "bar"],
                "child_2": [
                    {"grandchild_1": ["foo", "bar"], "grandchild_2": True},
                    {"grandchild_1": ["bar", "baz"], "grandchild_2": False},
                ],
                "deleted": flag(False),
            },
            {
                "id": 2,
                "child_1": ["baz"],
                "child_2": [{"grandchild_1": ["baz"], "grandchild_2": True}],
                "deleted": flag(False),
            },
        ]
        info = p.run(data_resource(data), **destination_config.run_kwargs)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 2
        assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 3
        assert load_table_counts(p, table_name + "__child_2")[table_name + "__child_2"] == 3
        assert (
            load_table_counts(p, table_name + "__child_2__grandchild_1")[
                table_name + "__child_2__grandchild_1"
            ]
            == 5
        )

        # delete first record
        data = [
            {"id": 1, "deleted": flag(True)},
        ]
        info = p.run(data_resource(data), **destination_config.run_kwargs)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 1
        assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 1
        assert (
            load_table_counts(p, table_name + "__child_2__grandchild_1")[
                table_name + "__child_2__grandchild_1"
            ]
            == 1
        )

        # delete second record
        data = [
            {"id": 2, "deleted": flag(True)},
        ]
        info = p.run(data_resource(data), **destination_config.run_kwargs)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 0
        assert load_table_counts(p, table_name + "__child_1")[table_name + "__child_1"] == 0
        assert (
            load_table_counts(p, table_name + "__child_2__grandchild_1")[
                table_name + "__child_2__grandchild_1"
            ]
            == 0
        )

    # more than one `hard_delete` column hint fails the schema verification. The failed package
    # stays pending, so nothing runs on this pipeline afterwards
    @dlt.resource(
        name="test_hard_delete_hint_too_many_hints",
        write_disposition="merge",
        columns={"deleted_1": {"hard_delete": True}, "deleted_2": {"hard_delete": True}},
    )
    def r():
        yield {"id": 1, "val": "foo", "deleted_1": True, "deleted_2": False}

    with pytest.raises(PipelineStepFailed):
        p.run(r(), **destination_config.run_kwargs)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_dedup_sort_hint(destination_config: DestinationTestConfiguration) -> None:
    table_name = "test_dedup_sort_hint"

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key="id",  # sort hints only have effect when a primary key is provided
        columns={
            "sequence": {"dedup_sort": "desc", "nullable": False},
            "val": {"dedup_sort": None},
        },
    )
    def data_resource(data):
        yield data

    p = destination_config.setup_pipeline("abstract", dev_mode=True)

    # three records with same primary key
    data = [
        {"id": 1, "val": "foo", "sequence": 1},
        {"id": 1, "val": "baz", "sequence": 3},
        {"id": 1, "val": "bar", "sequence": 2},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    # record with highest value in sort column is inserted (because "desc")
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {table_name}")
    ]
    expected = [{"id": 1, "val": "baz", "sequence": 3}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    # now test "asc" sorting
    data_resource.apply_hints(columns={"sequence": {"dedup_sort": "asc", "nullable": False}})

    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    # record with highest lowest in sort column is inserted (because "asc")
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {table_name}")
    ]
    expected = [{"id": 1, "val": "foo", "sequence": 1}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    table_name = "test_dedup_sort_hint_nested"
    data_resource.apply_hints(
        table_name=table_name,
        columns={"sequence": {"dedup_sort": "desc", "nullable": False}},
    )

    # three records with same primary key
    # only record with highest value in sort column is inserted
    data = [
        {"id": 1, "val": [1, 2, 3], "sequence": 1},
        {"id": 1, "val": [7, 8, 9], "sequence": 3},
        {"id": 1, "val": [4, 5, 6], "sequence": 2},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1
    assert load_table_counts(p, table_name + "__val")[table_name + "__val"] == 3

    # compare observed records with expected records, now for child table
    observed = [row[0] for row in select_data(p, f"SELECT value FROM {table_name}__val")]
    assert sorted(observed) == [7, 8, 9]  # type: ignore[type-var]

    table_name = "test_dedup_sort_hint_with_hard_delete"
    data_resource.apply_hints(
        table_name=table_name,
        columns={
            "sequence": {"dedup_sort": "desc", "nullable": False},
            "deleted": {"hard_delete": True},
        },
    )

    # three records with same primary key
    # record with highest value in sort column is a delete, so no record will be inserted
    data = [
        {"id": 1, "val": "foo", "sequence": 1, "deleted": False},
        {"id": 1, "val": "baz", "sequence": 3, "deleted": True},
        {"id": 1, "val": "bar", "sequence": 2, "deleted": False},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 0

    # three records with same primary key
    # record with highest value in sort column is not a delete, so it will be inserted
    data = [
        {"id": 1, "val": "foo", "sequence": 1, "deleted": False},
        {"id": 1, "val": "bar", "sequence": 2, "deleted": True},
        {"id": 1, "val": "baz", "sequence": 3, "deleted": False},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # compare observed records with expected records
    observed = [
        {"id": row[0], "val": row[1], "sequence": row[2]}
        for row in select_data(p, f"SELECT id, val, sequence FROM {table_name}")
    ]
    expected = [{"id": 1, "val": "baz", "sequence": 3}]
    assert sorted(observed, key=lambda d: d["id"]) == expected

    # additional tests with two records, run only on duckdb to limit test load
    if destination_config.destination_type == "duckdb":
        # two records with same primary key
        # record with highest value in sort column is a delete
        # existing record is deleted and no record will be inserted
        data = [
            {"id": 1, "val": "foo", "sequence": 1},
            {"id": 1, "val": "bar", "sequence": 2, "deleted": True},
        ]
        info = p.run(data_resource(data), **destination_config.run_kwargs)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 0

        # two records with same primary key
        # record with highest value in sort column is not a delete, so it will be inserted
        data = [
            {"id": 1, "val": "foo", "sequence": 2},
            {"id": 1, "val": "bar", "sequence": 1, "deleted": True},
        ]
        info = p.run(data_resource(data), **destination_config.run_kwargs)
        assert_load_info(info)
        assert load_table_counts(p, table_name)[table_name] == 1

    # test if exception is raised for invalid column schema's
    @dlt.resource(
        name="test_dedup_sort_hint_too_many_hints",
        write_disposition="merge",
        columns={"dedup_sort_1": {"dedup_sort": "this_is_invalid"}},  # type: ignore[call-overload]
    )
    def r():
        yield {"id": 1, "val": "foo", "dedup_sort_1": 1, "dedup_sort_2": 5}

    # invalid value for "dedup_sort" hint
    with pytest.raises(PipelineStepFailed):
        info = p.run(r(), **destination_config.run_kwargs)

    # more than one "dedup_sort" column hints are provided
    r.apply_hints(
        columns={"dedup_sort_1": {"dedup_sort": "desc"}, "dedup_sort_2": {"dedup_sort": "desc"}}
    )
    with pytest.raises(PipelineStepFailed):
        info = p.run(r(), **destination_config.run_kwargs)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_dedup_sort_hint_case_sensitive(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test that dedup_sort column names are properly escaped in merge SQL.

    Reproduces https://github.com/dlt-hub/dlt/issues/3529 where unescaped
    dedup_sort column names cause failures with case-sensitive naming
    conventions on destinations that casefold unquoted identifiers.
    """
    # use direct naming to preserve mixed case in column names
    os.environ["SCHEMA__NAMING"] = "direct"

    table_name = "test_dedup_sort_cs"

    @dlt.resource(
        name=table_name,
        write_disposition="merge",
        primary_key="id",
        columns={"Sequence": {"dedup_sort": "desc", "nullable": False}},
    )
    def data_resource(data):
        yield data

    p = destination_config.setup_pipeline("dedup_sort_cs", dev_mode=True)

    # three records with same primary key
    data = [
        {"id": 1, "val": "foo", "Sequence": 1},
        {"id": 1, "val": "baz", "Sequence": 3},
        {"id": 1, "val": "bar", "Sequence": 2},
    ]
    info = p.run(data_resource(data), **destination_config.run_kwargs)
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 1

    # record with highest value in sort column is inserted (because "desc")
    result = load_tables_to_dicts(p, table_name, exclude_system_cols=True)
    # column name depends on effective naming convention (e.g. s3_tables lowercases)
    seq_col = p.default_schema.naming.normalize_identifier("Sequence")
    assert_records_as_set(
        result[table_name],
        [{"id": 1, "val": "baz", seq_col: 3}],
    )


@pytest.mark.no_load
def test_merge_strategy_config() -> None:
    # merge strategy invalid
    with pytest.raises(ValueError):

        @dlt.resource(write_disposition={"disposition": "merge", "strategy": "foo"})  # type: ignore[call-overload]
        def invalid_resource():
            yield {"foo": "bar"}

    p = dlt.pipeline(
        pipeline_name="dummy_pipeline",
        destination="dummy",
        dev_mode=True,
    )

    # merge strategy not supported by destination
    @dlt.resource(write_disposition={"disposition": "merge", "strategy": "scd2"})
    def r():
        yield {"foo": "bar"}

    assert "scd2" not in p.destination.capabilities().supported_merge_strategies
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r())
    assert (
        pip_ex.value.step == "extract"
    )  # fails when table is added to schema and root key requirements are validated
    # PipelineStepFailed -> NormalizeJobFailed -> DestinationCapabilitiesException
    assert isinstance(pip_ex.value.__cause__, DestinationCapabilitiesException)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
        subset=["postgres", "filesystem"],  # test one SQL and one non-SQL destination
    ),
    ids=lambda x: x.name,
)
def test_upsert_merge_strategy_config(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(write_disposition={"disposition": "merge", "strategy": "upsert"})
    def r():
        yield {"foo": "bar"}

    # `upsert` merge strategy without `primary_key` should error
    p = destination_config.setup_pipeline("upsert_pipeline", dev_mode=True)
    assert "primary_key" not in r._hints
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r(), **destination_config.run_kwargs)
    assert isinstance(pip_ex.value.__context__, SchemaCorruptedException)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_missing_merge_key_column(destination_config: DestinationTestConfiguration) -> None:
    """Merge key is not present in data, error is raised"""

    @dlt.resource(merge_key="not_a_column", write_disposition={"disposition": "merge"})
    def merging_test_table():
        yield {"foo": "bar"}

    p = destination_config.setup_pipeline("abstract", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(merging_test_table(), **destination_config.run_kwargs)

    ex = pip_ex.value
    assert ex.step == "normalize"
    assert isinstance(ex.__context__, UnboundColumnException)

    assert "not_a_column" in str(ex)
    assert "merge key" in str(ex)
    assert "merging_test_table" in str(ex)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_merge_key_null_values(destination_config: DestinationTestConfiguration) -> None:
    """Merge key is present in data, but some rows have null values"""

    @dlt.resource(merge_key="id", write_disposition={"disposition": "merge"})
    def r():
        yield [{"id": 1}, {"id": None}, {"id": 2}]

    p = destination_config.setup_pipeline("abstract", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        p.run(r(), **destination_config.run_kwargs)

    ex = pip_ex.value
    assert ex.step == "normalize"

    assert isinstance(ex.__context__, NormalizeJobFailed)
    assert isinstance(ex.__context__.__context__, CannotCoerceNullException)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "merge_strategy,key_hint",
    [("delete-insert", "primary_key"), ("upsert", "primary_key"), ("delete-insert", "merge_key")],
    ids=["delete_insert", "upsert", "delete_insert_merge_key"],
)
def test_merge_arrow(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
    key_hint: str,
) -> None:
    """Merges Arrow tables by `primary_key` or by `merge_key` only. Without a primary key there is
    no `_dlt_id`, so destinations that delete through a temp table failed (#2248)."""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    if key_hint == "merge_key" and destination_config.destination_type == "athena":
        pytest.skip("Athena requires _dlt_id for merge (no correlated subquery support)")
    hints: Any = {key_hint: "id"}

    @dlt.resource(
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
        table_format=destination_config.table_format,
        **hints,
    )
    def arrow_items(rows, schema_columns, timezone="UTC"):
        yield row_tuples_to_arrow(
            rows,
            DestinationCapabilitiesContext.generic_capabilities(),
            columns=schema_columns,
            tz=timezone,
        )

    schema_columns = {
        "id": {"name": "id", "nullable": False, "data_type": "bigint"},
        "name": {"name": "name", "nullable": True, "data_type": "text"},
    }
    pipeline = destination_config.setup_pipeline("merge_arrow", dev_mode=True)
    load_info = pipeline.run(arrow_items([(1, "foo"), (2, "bar")], schema_columns))
    assert_load_info(load_info)
    tables = load_tables_to_dicts(pipeline, "arrow_items")
    assert_records_as_set(
        tables["arrow_items"], [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
    )

    # update a record
    load_info = pipeline.run(arrow_items([(1, "foo"), (2, "updated bar")], schema_columns))
    assert_load_info(load_info)
    tables = load_tables_to_dicts(pipeline, "arrow_items")
    assert_records_as_set(
        tables["arrow_items"], [{"id": 1, "name": "foo"}, {"id": 2, "name": "updated bar"}]
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_replacing_merge_key(destination_config: DestinationTestConfiguration) -> None:
    """Test that changing merge_key properly deletes records based on the NEW key.
    Records matching the new merge_key in incoming data should replace old ones.
    """
    p = destination_config.setup_pipeline("test_replacing_merge_key", dev_mode=True)

    # load initial data with merge_key "time_off_date"
    @dlt.resource(
        write_disposition={
            "disposition": "merge",
            "strategy": "delete-insert",
        },
        merge_key=["time_off_date"],
    )
    def people(data):
        yield from data

    initial_data = [
        {"email": "user_1@example.com", "time_off_date": "25.07.2025", "month_key": "2025-07"},
        {"email": "user_2@example.com", "time_off_date": "18.08.2025", "month_key": "2025-08"},
    ]

    info = p.run(people(initial_data), **destination_config.run_kwargs)
    assert_load_info(info)

    observed = [
        {"email": row[0], "time_off_date": row[1], "month_key": row[2]}
        for row in select_data(p, "SELECT email, time_off_date, month_key FROM people")
    ]

    assert sorted(observed, key=lambda d: d["email"]) == initial_data

    # change merge_key to "month_key"
    people.apply_hints(merge_key=["month_key"])

    # new data has month_key "2025-08" which exists in old data
    # should delete the old 2025-08 record (18.08.2025) and insert new one (19.08.2025)
    new_data = [
        {"email": "user_2@example.com", "time_off_date": "19.08.2025", "month_key": "2025-08"},
        {"email": "user_2@example.com", "time_off_date": "20.09.2025", "month_key": "2025-09"},
    ]

    info = p.run(people(new_data), **destination_config.run_kwargs)

    observed = [
        {"email": row[0], "time_off_date": row[1], "month_key": row[2]}
        for row in select_data(p, "SELECT email, time_off_date, month_key FROM people")
    ]

    expected = [initial_data[0]] + new_data

    assert sorted(observed, key=lambda d: (d["email"], d["month_key"])) == sorted(
        expected, key=lambda d: (d["email"], d["month_key"])
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
def test_insert_only_with_hard_delete(destination_config: DestinationTestConfiguration) -> None:
    """`insert-only` does not insert records flagged for hard delete and never deletes or updates
    existing records, also when they arrive flagged."""
    skip_if_unsupported_merge_strategy(destination_config, "insert-only")

    def snapshot(data: List[StrAny]) -> DltSource:
        return merge_resource(
            data,
            "insert-only",
            columns={"deleted": {"hard_delete": True}},
            table_format=destination_config.table_format,
        )

    p = destination_config.setup_pipeline("insert_only_hard_delete", dev_mode=True)
    alice = {"id": 1, "name": "Alice", "deleted": False}
    bob = {"id": 2, "name": "Bob", "deleted": False}
    assert_load_info(p.run(snapshot([alice, bob]), **destination_config.run_kwargs))

    # id 1 exists and arrives flagged: insert-only neither updates nor deletes it
    dave = {"id": 4, "name": "Dave", "deleted": False}
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"id": 1, "name": "Alice Deleted", "deleted": True},
                    {"id": 3, "name": "Charlie", "deleted": True},
                    dave,
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    tables = load_tables_to_dicts(p, "items", exclude_system_cols=True)
    assert_records_as_set(tables["items"], [alice, bob, dave])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
def test_insert_only_with_nested_tables(destination_config: DestinationTestConfiguration) -> None:
    """`insert-only` inserts new list elements of existing records and never updates the parent."""
    skip_if_unsupported_merge_strategy(destination_config, "insert-only")

    def snapshot(data: List[StrAny]) -> DltSource:
        return merge_resource(
            data, "insert-only", name="parent_items", table_format=destination_config.table_format
        )

    p = destination_config.setup_pipeline("insert_only_nested", dev_mode=True)
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"id": 1, "name": "Parent1", "children": [{"child_id": 1}]},
                    {"id": 2, "name": "Parent2", "children": [{"child_id": 2}]},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )
    assert load_table_counts(p, "parent_items", "parent_items__children") == {
        "parent_items": 2,
        "parent_items__children": 2,
    }

    # parent 1 changes its name and gains a child, parent 3 is new
    assert_load_info(
        p.run(
            snapshot(
                [
                    {
                        "id": 1,
                        "name": "Parent1_Updated",
                        "children": [{"child_id": 1}, {"child_id": 3}],
                    },
                    {"id": 3, "name": "Parent3", "children": [{"child_id": 4}]},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    tables = load_tables_to_dicts(
        p, "parent_items", "parent_items__children", exclude_system_cols=True
    )
    assert_records_as_set(
        tables["parent_items"],
        [{"id": 1, "name": "Parent1"}, {"id": 2, "name": "Parent2"}, {"id": 3, "name": "Parent3"}],
    )
    # child 1 exists, so only children 3 and 4 are new
    assert sorted(c["child_id"] for c in tables["parent_items__children"]) == [1, 2, 3, 4]


HANDWRITTEN_KEY_SCOPE = "EXISTS (SELECT 1 FROM {staging_table} s WHERE s.id = {table}.id)"


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        default_vector_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "merge_strategy,skip_unchanged_rows",
    [("upsert", False), ("upsert", True), ("insert-only", False), ("cdc", False), ("cdc", True)],
    ids=["upsert", "upsert_skip_unchanged", "insert_only", "cdc", "cdc_skip_unchanged"],
)
def test_merge_strategy_snapshot(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
    skip_unchanged_rows: bool,
) -> None:
    """Seeds three records and loads a snapshot that keeps one, changes one, drops one and adds
    one. Checks the surviving records per strategy and which records the merge rewrote."""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    if (
        skip_unchanged_rows
        and destination_config.table_format == "iceberg"
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip("Iceberg on filesystem rejects `skip_unchanged_rows`")
    if destination_config.destination_type in ("qdrant", "weaviate"):
        pytest.skip("`load_tables_to_dicts` cannot read vector stores without a SQL client")
    if skip_unchanged_rows and destination_config.destination_type in ("lance", "lancedb"):
        pytest.skip("Vector stores reject `skip_unchanged_rows`")
    options: Any = {"skip_unchanged_rows": True} if skip_unchanged_rows else {}

    def snapshot(data: List[StrAny]) -> DltSource:
        return merge_resource(
            data, merge_strategy, table_format=destination_config.table_format, **options
        )

    alice = {"id": 1, "name": "Alice", "value": 100}
    bob = {"id": 2, "name": "Bob", "value": 200}
    charlie = {"id": 3, "name": "Charlie", "value": 300}
    # seed three records
    p = destination_config.setup_pipeline("merge_snapshot", dev_mode=True)
    assert_load_info(p.run(snapshot([alice, bob, charlie]), **destination_config.run_kwargs))
    load_ids = load_ids_by_key(p, "items")

    # load a snapshot where alice is unchanged, bob changed, charlie absent and dave new
    bob_changed = {**bob, "value": 999}
    dave = {"id": 4, "name": "Dave", "value": 400}
    assert_load_info(p.run(snapshot([alice, bob_changed, dave]), **destination_config.run_kwargs))

    # upsert updates bob and keeps charlie, insert-only keeps bob as loaded, cdc deletes charlie
    expected = {
        "upsert": [alice, bob_changed, charlie, dave],
        "insert-only": [alice, bob, charlie, dave],
        "cdc": [alice, bob_changed, dave],
    }[merge_strategy]
    rows = load_tables_to_dicts(p, "items", exclude_system_cols=True)["items"]
    assert_records_as_set(rows, expected)

    # the unchanged alice keeps her load id only if the merge compares rows or never updates
    new_load_ids = load_ids_by_key(p, "items")
    unchanged_kept = skip_unchanged_rows or merge_strategy == "insert-only"
    assert (new_load_ids[1] == load_ids[1]) is unchanged_kept
    # the changed bob is rewritten by every strategy that updates
    assert (new_load_ids[2] == load_ids[2]) is (merge_strategy == "insert-only")


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_vector_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("option", ["skip_unchanged_rows", "source_filter"])
def test_vector_store_rejects_merge_options(
    destination_config: DestinationTestConfiguration, option: str
) -> None:
    """Runs an `upsert` with an option that vector stores cannot apply, because they update every
    matched record and run no SQL condition. Checks that the load is rejected before it starts."""
    value: Any = True if option == "skip_unchanged_rows" else NEW_BUCKET
    p = destination_config.setup_pipeline("vector_merge_options", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as exc:
        p.run(
            merge_resource([{"id": 1, "bucket": "new"}], "upsert", **{option: value}),
            **destination_config.run_kwargs,
        )
    assert isinstance(exc.value.__cause__, SchemaCorruptedException)
    assert f"`{option}`" in str(exc.value.__cause__)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "merge_strategy", ["upsert", "cdc", None], ids=["upsert", "cdc", "default"]
)
@pytest.mark.parametrize("row_version", [False, True], ids=["all_columns", "row_version"])
def test_skip_unchanged_rows(
    destination_config: DestinationTestConfiguration,
    merge_strategy: Optional[TLoaderMergeStrategy],
    row_version: bool,
) -> None:
    """Seeds three versioned records and reloads them with one unchanged, one with a new name and
    one with a new version. Checks which records the merge rewrote when all columns or only the
    row version decide."""
    if merge_strategy:
        skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    elif destination_config.table_format != "delta":
        # without a strategy the option must apply to the strategy that the destination picks
        pytest.skip("Only the delta table format defaults to `upsert`")
    if (
        destination_config.table_format == "iceberg"
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip("Iceberg on filesystem rejects `skip_unchanged_rows`")
    if row_version and destination_config.destination_type not in LOCAL_DESTINATIONS:
        pytest.skip("The row version comparison does not depend on the SQL dialect")
    options: Any = {"skip_unchanged_rows": True}
    if row_version:
        options["row_version_column_name"] = "version"

    def snapshot(data: Sequence[StrAny]) -> DltSource:
        return merge_resource(
            data, merge_strategy, table_format=destination_config.table_format, **options
        )

    rows: List[Dict[str, Any]] = [
        {"id": i, "name": name, "version": 1} for i, name in enumerate("abc", start=1)
    ]
    # seed three records in version 1
    p = destination_config.setup_pipeline("skip_unchanged", dev_mode=True)
    assert_load_info(p.run(snapshot(rows), **destination_config.run_kwargs))
    before = load_ids_by_key(p, "items")

    # reload: 1 is unchanged, 2 changes only its name, 3 changes only its version
    rows[1]["name"] = "b2"
    rows[2]["version"] = 2
    assert_load_info(p.run(snapshot(rows), **destination_config.run_kwargs))

    # the unchanged record is not rewritten, so change consumers (ie. Snowflake Streams) skip it
    after = load_ids_by_key(p, "items")
    assert after[1] == before[1]
    # with a row version column, a changed name alone does not count as a change
    assert (after[2] == before[2]) is row_version
    # a changed version is a change in both modes
    assert after[3] != before[3]
    # the skipped update also keeps the stale name
    names = {r["id"]: r["name"] for r in load_tables_to_dicts(p, "items")["items"]}
    assert names[2] == ("b" if row_version else "b2")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("source_filter", [None, NEW_BUCKET], ids=["no_filter", "filter"])
def test_cdc_composite_primary_key(
    destination_config: DestinationTestConfiguration, source_filter: Optional[str]
) -> None:
    """Seeds records that share an `id` across regions and reloads them with one record changed
    and one moved out of the source filter. Checks that the merge matches on both key columns and
    that `cdc` deletes the discarded record."""
    skip_if_unsupported_merge_strategy(destination_config, "cdc")
    if source_filter:
        source_filter = merge_condition(source_filter, destination_config, "staging_table")

    def snapshot(data: List[StrAny], **options: Any) -> DltSource:
        return merge_resource(
            data,
            "cdc",
            primary_key=["region", "id"],
            table_format=destination_config.table_format,
            **options,
        )

    # seed two eu records and one us record that shares its id with (eu, 1)
    p = destination_config.setup_pipeline("cdc_composite", dev_mode=True)
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"region": "eu", "id": 1, "bucket": "new", "value": 100},
                    {"region": "eu", "id": 2, "bucket": "new", "value": 200},
                    {"region": "us", "id": 1, "bucket": "new", "value": 300},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    # reload: (eu, 1) unchanged, (eu, 2) updated, (us, 1) moves to bucket 'old', (us, 2) new
    us_1 = {"region": "us", "id": 1, "bucket": "old", "value": 301}
    us_2 = {"region": "us", "id": 2, "bucket": "new", "value": 400}
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"region": "eu", "id": 1, "bucket": "new", "value": 100},
                    {"region": "eu", "id": 2, "bucket": "new", "value": 999},
                    us_1,
                    us_2,
                ],
                source_filter=source_filter,
            ),
            **destination_config.run_kwargs,
        )
    )

    # (eu, 2) and (us, 2) share the id and both survive, so the merge matched on both columns
    expected = [
        {"region": "eu", "id": 1, "bucket": "new", "value": 100},
        {"region": "eu", "id": 2, "bucket": "new", "value": 999},
        us_2,
    ]
    # the source filter discards (us, 1), so cdc deletes its stored copy although (eu, 1) stays
    if not source_filter:
        expected.append(us_1)
    rows = load_tables_to_dicts(p, "items", exclude_system_cols=True)["items"]
    assert_records_as_set(rows, expected)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("hard_delete_type", ["bool", "text"])
def test_cdc_hard_delete(
    destination_config: DestinationTestConfiguration, hard_delete_type: TDataType
) -> None:
    """Seeds two records and reloads them with one flagged for hard delete next to a new record
    that arrives flagged. Checks that `cdc` deletes the flagged record and never inserts the new
    one."""
    skip_if_unsupported_merge_strategy(destination_config, "cdc")

    # a text column flags a record with any non-NULL value
    def flag(deleted: bool) -> Any:
        if hard_delete_type == "bool":
            return deleted
        return "D" if deleted else None

    def snapshot(data: List[StrAny]) -> DltSource:
        return merge_resource(
            data,
            "cdc",
            name="accounts",
            table_format=destination_config.table_format,
            columns={"deleted": {"hard_delete": True, "data_type": hard_delete_type}},
        )

    # seed two live records
    p = destination_config.setup_pipeline("cdc_hard_delete", dev_mode=True)
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"id": 1, "name": "Alice", "deleted": flag(False)},
                    {"id": 2, "name": "Bob", "deleted": flag(False)},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    # reload: 1 is flagged, 2 is unchanged, 3 is new and arrives flagged
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"id": 1, "name": "Alice", "deleted": flag(True)},
                    {"id": 2, "name": "Bob", "deleted": flag(False)},
                    {"id": 3, "name": "Charlie", "deleted": flag(True)},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    # the flag deletes 1 although the snapshot contains it, and cdc never inserts the flagged 3
    tables = load_tables_to_dicts(p, "accounts", exclude_system_cols=True)
    assert_records_as_set(tables["accounts"], [{"id": 2, "name": "Bob", "deleted": flag(False)}])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("hard_delete", [False, True], ids=["absent_parent", "flagged_parent"])
@pytest.mark.parametrize(
    "skip_unchanged_rows", [False, True], ids=["update_matched", "skip_unchanged"]
)
def test_cdc_nested_tables(
    destination_config: DestinationTestConfiguration, hard_delete: bool, skip_unchanged_rows: bool
) -> None:
    """Seeds two parents with children and reloads a snapshot that drops or flags one parent and
    replaces a child of the other. Checks that nested rows follow their parents and that change
    detection is per table."""
    skip_if_unsupported_merge_strategy(destination_config, "cdc")
    if hard_delete and destination_config.table_format == "delta":
        pytest.skip("Delta rejects `hard_delete` with nested tables.")

    columns: Optional[TTableSchemaColumns] = (
        {"deleted": {"hard_delete": True, "data_type": "bool"}} if hard_delete else None
    )

    def snapshot(data: List[StrAny]) -> DltSource:
        return merge_resource(
            data,
            "cdc",
            name="parent_items",
            table_format=destination_config.table_format,
            columns=columns,
            skip_unchanged_rows=skip_unchanged_rows,
        )

    # seed two parents with children
    p = destination_config.setup_pipeline("cdc_nested", dev_mode=True)
    assert_load_info(
        p.run(
            snapshot(
                [
                    {"id": 1, "name": "P1", "deleted": False, "children": [{"c": 1}, {"c": 2}]},
                    {"id": 2, "name": "P2", "deleted": False, "children": [{"c": 3}]},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )
    load_ids = load_ids_by_key(p, "parent_items")

    # reload: parent 1 loses child 2 and gains child 4, parent 2 is absent or arrives flagged
    snapshot_rows: List[StrAny] = [
        {"id": 1, "name": "P1", "deleted": False, "children": [{"c": 1}, {"c": 4}]}
    ]
    if hard_delete:
        snapshot_rows.append({"id": 2, "name": "P2", "deleted": True, "children": [{"c": 3}]})
    assert_load_info(p.run(snapshot(snapshot_rows), **destination_config.run_kwargs))

    # parent 2 is deleted with its children
    tables = load_tables_to_dicts(
        p, "parent_items", "parent_items__children", exclude_system_cols=True
    )
    assert [r["id"] for r in tables["parent_items"]] == [1]
    # the list of parent 1 follows the snapshot: child 2 is deleted, child 4 inserted
    assert_records_as_set(tables["parent_items__children"], [{"c": 1}, {"c": 4}])
    assert_nested_rows_have_parents(p, "parent_items", "parent_items__children")
    # change detection is per table: parent 1 did not change although its children did
    assert (load_ids_by_key(p, "parent_items")[1] == load_ids[1]) is skip_unchanged_rows


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "merge_strategy,source_filter,destination_scope,expected_ids",
    [
        # cdc deletes the stored records absent from the load: 1, 3 and 4
        ("cdc", None, None, [2, 5, 6, 7]),
        # the scope limits the deletes to bucket 'new', so only 3 is deleted
        ("cdc", None, NEW_BUCKET, [1, 2, 4, 5, 6, 7]),
        # the filter discards 6 from the load and alone does not limit the deletes
        ("cdc", NEW_BUCKET, None, [2, 5, 7]),
        ("cdc", NEW_BUCKET, NEW_BUCKET, [1, 2, 4, 5, 7]),
        # upsert never deletes, the filter discards 6
        ("upsert", NEW_BUCKET, None, [1, 2, 3, 4, 5, 7]),
        # the scope replaces the key match: 2 and 3 are deleted, the old copy of 7 stays
        ("delete-insert", None, NEW_BUCKET, [1, 2, 4, 5, 6, 7, 7]),
        # the key match deletes 2 and 7, the filter discards 6
        ("delete-insert", NEW_BUCKET, None, [1, 2, 3, 4, 5, 7]),
        ("delete-insert", NEW_BUCKET, NEW_BUCKET, [1, 2, 4, 5, 7, 7]),
        # a scope with placeholders rewrites the key match and gives the same result
        ("delete-insert", None, HANDWRITTEN_KEY_SCOPE, [1, 2, 3, 4, 5, 6, 7]),
    ],
    ids=[
        "cdc",
        "cdc_scope",
        "cdc_filter",
        "cdc_filter_scope",
        "upsert_filter",
        "delete_insert_scope",
        "delete_insert_filter",
        "delete_insert_filter_scope",
        "delete_insert_key_scope",
    ],
)
def test_merge_conditions(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
    source_filter: Optional[str],
    destination_scope: Optional[str],
    expected_ids: List[int],
) -> None:
    """Seeds `STORED_RECORDS` and merges `LOADED_RECORDS` with a source filter, a destination
    scope or both. Checks which records survive: the filter selects the merge source, the scope
    selects the stored records that the merge may delete."""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    if (
        source_filter
        and destination_config.table_format == "iceberg"
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip("Iceberg on filesystem rejects `source_filter`")
    if source_filter:
        source_filter = merge_condition(source_filter, destination_config, "staging_table")
    if destination_scope and "{" in destination_scope:
        if destination_config.destination_type not in LOCAL_DESTINATIONS:
            pytest.skip("Not every destination runs a correlated subquery in a DELETE")
    elif destination_scope:
        destination_scope = merge_condition(destination_scope, destination_config, "table")
    table_format = destination_config.table_format

    # seed the stored records as they are, without a merge
    p = destination_config.setup_pipeline("merge_conditions", dev_mode=True)
    assert_load_info(
        p.run(
            merge_resource(STORED_RECORDS, merge_strategy, append=True, table_format=table_format),
            **destination_config.run_kwargs,
        )
    )
    # merge the loaded records with the conditions
    assert_load_info(
        p.run(
            merge_resource(
                LOADED_RECORDS,
                merge_strategy,
                table_format=table_format,
                source_filter=source_filter,
                destination_scope=destination_scope,
            ),
            **destination_config.run_kwargs,
        )
    )

    # the ids that survive are explained per case in the parametrization
    assert sorted(r["id"] for r in load_tables_to_dicts(p, "items")["items"]) == expected_ids


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
        subset=("duckdb", "filesystem"),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "table_format,hints,record,expected",
    [
        (
            None,
            {"source_filter": "id IN (SELECT id FROM {staging_table})"},
            {"id": 1, "bucket": "new"},
            "unknown placeholders `{staging_table}`",
        ),
        (
            "delta",
            {"merge_key": "bucket"},
            {"id": 1, "bucket": "new"},
            "`merge_key` with the `cdc`",
        ),
        (
            "delta",
            {"destination_scope": "{staging_table}.bucket = 'new'"},
            {"id": 1, "bucket": "new"},
            "unknown placeholders `{staging_table}`",
        ),
        (
            "delta",
            {"source_filter": "{table}.bucket = 'new'"},
            {"id": 1, "bucket": "new"},
            "unknown placeholders `{table}`",
        ),
        (
            "delta",
            {"destination_scope": "{table}.bucket = 'new'"},
            {"id": 1, "bucket": "new", "children": [{"c": 1}]},
            "`source_filter` or `destination_scope` with the `cdc`",
        ),
        (
            "delta",
            {"columns": {"deleted": {"hard_delete": True, "data_type": "bool"}}},
            {"id": 1, "bucket": "new", "deleted": False, "children": [{"c": 1}]},
            "`hard_delete` hint with the `cdc`",
        ),
        (
            "delta",
            {
                "strategy": "upsert",
                "columns": {"deleted": {"hard_delete": True, "data_type": "bool"}},
            },
            {"id": 1, "bucket": "new", "deleted": False, "children": [{"c": 1}]},
            "`hard_delete` hint with the `upsert`",
        ),
        (
            "delta",
            {"strategy": "upsert", "source_filter": "{staging_table}.bucket = 'new'"},
            {"id": 1, "bucket": "new", "children": [{"c": 1}]},
            "`source_filter` or `destination_scope` with the `upsert`",
        ),
        (
            "iceberg",
            {"strategy": "upsert", "source_filter": NEW_BUCKET},
            {"id": 1, "bucket": "new"},
            "`source_filter` with the `upsert` merge strategy on `iceberg` table",
        ),
        (
            "iceberg",
            {"strategy": "upsert", "skip_unchanged_rows": True},
            {"id": 1, "bucket": "new"},
            "`skip_unchanged_rows` with the `upsert` merge strategy on `iceberg` table",
        ),
    ],
    ids=[
        "sql_staging_table_in_source_filter",
        "delta_merge_key",
        "delta_staging_table_in_destination_scope",
        "delta_table_in_source_filter",
        "delta_filter_nested",
        "delta_hard_delete_nested",
        "delta_upsert_hard_delete_nested",
        "delta_upsert_filter_nested",
        "iceberg_upsert_filter",
        "iceberg_upsert_skip_unchanged",
    ],
)
def test_merge_rejected_before_load(
    destination_config: DestinationTestConfiguration,
    table_format: Optional[TTableFormat],
    hints: Dict[str, Any],
    record: StrAny,
    expected: str,
) -> None:
    """Loads one record with merge settings that the resource accepts but the destination cannot
    run. Checks that the schema verification rejects them before any load job."""
    if destination_config.table_format != table_format:
        pytest.skip(f"Checks the {table_format or 'SQL'} rules.")

    disposition: Any = {"disposition": "merge", "strategy": "cdc"}
    resource_hints: Dict[str, Any] = {}
    for key, value in hints.items():
        if key in ("strategy", "skip_unchanged_rows", "source_filter", "destination_scope"):
            disposition[key] = value
        else:
            resource_hints[key] = value

    @dlt.resource(
        name="items",
        primary_key="id",
        table_format=table_format,
        write_disposition=disposition,
        **resource_hints,
    )
    def items():
        yield [record]

    # the resource accepts the settings, the schema verification of the load rejects them
    p = destination_config.setup_pipeline("merge_rejected", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as exc:
        p.run(items(), **destination_config.run_kwargs)
    assert isinstance(exc.value.__cause__, SchemaCorruptedException)
    assert expected in str(exc.value.__cause__)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True, subset=LOCAL_DESTINATIONS),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("strategy", ["cdc", "delete-insert"], ids=["cdc", "delete_insert"])
@pytest.mark.parametrize(
    "conditions",
    [
        {},
        {"source_filter": "value > 5"},
        # this destination scope selects the buckets of all loaded records, discarded ones too
        {
            "source_filter": "value > 5",
            "destination_scope": "bucket IN (SELECT bucket FROM {staging_table})",
        },
    ],
    ids=["merge_key", "filter_merge_key", "filter_scope"],
)
def test_merge_key_source_filter(
    destination_config: DestinationTestConfiguration,
    strategy: TLoaderMergeStrategy,
    conditions: Dict[str, str],
) -> None:
    """Seeds four records in three buckets and reloads two of them with `merge_key` on the
    bucket, a source filter that discards one, and a destination scope. Checks which partitions
    the merge replaces."""
    skip_if_unsupported_merge_strategy(destination_config, strategy)

    def events(data: List[StrAny], **options: Any) -> DltSource:
        return merge_resource(data, strategy, name="events", merge_key="bucket", **options)

    # seed four records in the buckets 'old', 'new' and 'mid'
    p = destination_config.setup_pipeline("merge_key_source_filter", dev_mode=True)
    assert_load_info(
        p.run(
            events(
                [
                    {"id": 1, "bucket": "old", "value": 1},
                    {"id": 2, "bucket": "new", "value": 2},
                    {"id": 3, "bucket": "new", "value": 3},
                    {"id": 4, "bucket": "mid", "value": 4},
                ]
            ),
            **destination_config.run_kwargs,
        )
    )

    # reload: 2 changes in bucket 'new', 5 is new in bucket 'mid' but the source filter discards it
    assert_load_info(
        p.run(
            events(
                [{"id": 2, "bucket": "new", "value": 22}, {"id": 5, "bucket": "mid", "value": 1}],
                **conditions,
            ),
            **destination_config.run_kwargs,
        )
    )

    # in all cases the merge replaces partition 'new' and keeps 1 in partition 'old'
    expected = [{"id": 1, "bucket": "old", "value": 1}, {"id": 2, "bucket": "new", "value": 22}]
    if not conditions:
        expected.append({"id": 5, "bucket": "mid", "value": 1})
    elif "destination_scope" not in conditions:
        # 5 is not in the merge source, so the merge does not replace partition 'mid'
        expected.append({"id": 4, "bucket": "mid", "value": 4})
    # with the destination scope, the merge deletes partition 'mid' and inserts nothing
    tables = load_tables_to_dicts(p, "events", exclude_system_cols=True)
    assert_records_as_set(tables["events"], expected)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        table_format_local_configs=True,
        supports_merge=True,
        subset=LOCAL_DESTINATIONS,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("materialize", [False, True], ids=["no_job", "empty_job"])
@pytest.mark.parametrize(
    "scope",
    [None, "merge_key", "destination_scope"],
    ids=["whole_table", "merge_key", "destination_scope"],
)
def test_cdc_empty_snapshot(
    destination_config: DestinationTestConfiguration, materialize: bool, scope: Optional[str]
) -> None:
    """Seeds three records and loads an empty snapshot as a plain list or as a materialized table
    schema, without scope, with a `merge_key` or with a destination scope. Checks which records
    `cdc` deletes."""
    skip_if_unsupported_merge_strategy(destination_config, "cdc")
    if scope == "merge_key" and destination_config.table_format == "delta":
        pytest.skip("Delta rejects `merge_key` with `cdc`")
    destination_scope = None
    if scope == "destination_scope":
        destination_scope = merge_condition("region = 'eu'", destination_config, "table")

    def snapshot(data: Any) -> DltSource:
        return merge_resource(
            data,
            "cdc",
            merge_key="region" if scope == "merge_key" else None,
            table_format=destination_config.table_format,
            destination_scope=destination_scope,
        )

    # seed two eu records and one us record
    p = destination_config.setup_pipeline("cdc_empty", dev_mode=True)
    seed: List[StrAny] = [
        {"id": 1, "region": "eu"},
        {"id": 2, "region": "eu"},
        {"id": 3, "region": "us"},
    ]
    assert_load_info(p.run(snapshot(seed), **destination_config.run_kwargs))

    # load the empty snapshot: a plain list yields no load job, a materialized schema an empty one
    empty = dlt.mark.materialize_table_schema() if materialize else []
    info = p.run(snapshot(empty), **destination_config.run_kwargs)
    if materialize:
        assert_load_info(info)

    if not materialize:
        # without a load job no merge runs
        expected = 3
    elif scope is None:
        # the empty merge source matches no stored record, so cdc deletes all
        expected = 0
    elif scope == "merge_key":
        # the empty snapshot has no merge key partitions, so cdc deletes nothing
        expected = 3
    else:
        # the destination scope selects the eu records and cdc deletes them
        expected = 1
    assert load_table_counts(p, "items")["items"] == expected


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, supports_merge=True, subset=("duckdb", "sqlalchemy")
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("destination_scope", [None, NEW_BUCKET], ids=["append", "replace"])
def test_delete_insert_without_keys(
    destination_config: DestinationTestConfiguration, destination_scope: Optional[str]
) -> None:
    """Loads the same rows twice without keys, with and without a destination scope. Checks that
    `delete-insert` appends unless the scope selects the earlier rows."""
    # load the same two rows twice
    rows = [{"id": 1, "bucket": "new"}, {"id": 2, "bucket": "new"}]
    p = destination_config.setup_pipeline("di_keyless", dev_mode=True)
    for _ in range(2):
        assert_load_info(
            p.run(
                merge_resource(
                    rows, "delete-insert", primary_key=None, destination_scope=destination_scope
                ),
                **destination_config.run_kwargs,
            )
        )
    # without keys nothing matches and both loads stay, the scope deletes the first load
    assert load_table_counts(p, "items")["items"] == (2 if destination_scope else 4)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "strategy,destination_scope,expected",
    [
        # the scope deletes only the loaded keys within p15, so 2 stays and 3 is inserted twice
        (
            "delete-insert",
            PARTITION_LOCAL_KEY,
            [(1, "a2", "p15"), (2, "b", "p15"), (3, "c", "p16"), (3, "c2", "p15")],
        ),
        # the scope deletes all of p15, the copy of 3 in p16 stays
        ("delete-insert", PARTITION, [(1, "a2", "p15"), (3, "c", "p16"), (3, "c2", "p15")]),
        # cdc matches 3 across partitions and moves it to p15, 2 is absent and deleted
        ("cdc", PARTITION, [(1, "a2", "p15"), (3, "c2", "p15")]),
    ],
    ids=["delete_insert_local_key", "delete_insert_partition", "cdc_partition"],
)
def test_delete_insert_partition_local_primary_key(
    destination_config: DestinationTestConfiguration,
    strategy: TLoaderMergeStrategy,
    destination_scope: str,
    expected: List[Tuple[int, str, str]],
) -> None:
    """Seeds two partitions and merges a load into one of them that carries a key stored in the
    other. Checks whether the key is unique within the partition or globally, per strategy and
    scope."""
    skip_if_unsupported_merge_strategy(destination_config, strategy)

    # seed 1 and 2 in p15 and 3 in p16
    p = destination_config.setup_pipeline("partition_local_key", dev_mode=True)
    assert_load_info(
        p.run(
            merge_resource(PARTITIONED_TARGET, strategy, append=True),
            **destination_config.run_kwargs,
        )
    )
    # merge 1 and 3 into p15
    assert_load_info(
        p.run(
            merge_resource(PARTITIONED_INPUT, strategy, destination_scope=destination_scope),
            **destination_config.run_kwargs,
        )
    )

    # the parametrization explains which copies survive per strategy and scope
    rows = load_tables_to_dicts(p, "items", exclude_system_cols=True)["items"]
    assert sorted((r["id"], r["v"], r["part"]) for r in rows) == sorted(expected)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "strategy,destination_scope",
    [("cdc", None), ("upsert", None), ("delete-insert", NEW_BUCKET)],
    ids=["cdc", "upsert", "delete_insert"],
)
@pytest.mark.parametrize(
    "source_filter", [NEW_BUCKET, NEW_BUCKET_SUBQUERY], ids=["filter", "filter_subquery"]
)
def test_source_filter_nested_tables(
    destination_config: DestinationTestConfiguration,
    strategy: TLoaderMergeStrategy,
    destination_scope: Optional[str],
    source_filter: str,
) -> None:
    """Seeds three records with children and loads records where the source filter discards one.
    Checks that the merge never inserts the discarded record or its children and that nested rows
    outside the destination scope stay."""
    skip_if_unsupported_merge_strategy(destination_config, strategy)

    # seed three records with a child each, 3 is in bucket 'old'
    p = destination_config.setup_pipeline("source_filter_nested", dev_mode=True)
    target = [
        {"id": 1, "bucket": "new", "children": [{"c": "a"}]},
        {"id": 2, "bucket": "new", "children": [{"c": "b"}]},
        {"id": 3, "bucket": "old", "children": [{"c": "c"}]},
    ]
    # cdc and upsert derive root row ids from the primary key. An append seed gives random ids,
    # which the merge cannot match when it deletes nested rows
    seed = merge_resource(target, strategy, append=strategy == "delete-insert")
    assert_load_info(p.run(seed, **destination_config.run_kwargs))

    # load: 1 replaces its child, 4 is in bucket 'old' and the filter discards it, 5 is new
    load = [
        {"id": 1, "bucket": "new", "children": [{"c": "a2"}, {"c": "a3"}]},
        {"id": 4, "bucket": "old", "children": [{"c": "d"}]},
        {"id": 5, "bucket": "new", "children": [{"c": "e"}]},
    ]
    assert_load_info(
        p.run(
            merge_resource(
                load, strategy, source_filter=source_filter, destination_scope=destination_scope
            ),
            **destination_config.run_kwargs,
        )
    )

    # the discarded 4 and its child are never inserted
    tables = load_tables_to_dicts(p, "items", "items__children")
    if strategy == "upsert":
        # upsert keeps 2 and 3 with their children although the load lacks them
        assert sorted(r["id"] for r in tables["items"]) == [1, 2, 3, 5]
        assert sorted(r["c"] for r in tables["items__children"]) == ["a2", "a3", "b", "c", "e"]
    elif strategy == "cdc":
        # without a destination scope, cdc deletes 2 and 3 with their children
        assert sorted(r["id"] for r in tables["items"]) == [1, 5]
        assert sorted(r["c"] for r in tables["items__children"]) == ["a2", "a3", "e"]
    else:
        # the scope replaces bucket 'new' with its children, 3 is outside the scope and stays
        assert sorted(r["id"] for r in tables["items"]) == [1, 3, 5]
        assert sorted(r["c"] for r in tables["items__children"]) == ["a2", "a3", "c", "e"]
    assert_nested_rows_have_parents(p, "items", "items__children")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
def test_delete_insert_destination_scope_nested_tables(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Seeds two partitions with children and reloads one partition several times. Checks that
    `delete-insert` replaces nested rows with their parents, also for parents that the load does
    not carry, and leaves the other partition alone."""
    p = destination_config.setup_pipeline("di_partition_nested", dev_mode=True)

    def load(data: Sequence[StrAny], append: bool = False) -> None:
        resource = merge_resource(
            data, "delete-insert", destination_scope="part = 'A'", append=append
        )
        assert_load_info(p.run(resource, **destination_config.run_kwargs))

    def observed() -> Tuple[List[int], List[str]]:
        assert_nested_rows_have_parents(p, "items", "items__child")
        tables = load_tables_to_dicts(p, "items", "items__child")
        return (
            sorted(r["id"] for r in tables["items"]),
            sorted(r["val"] for r in tables["items__child"]),
        )

    # seed partition A with 1 and 2 and partition B with 3
    load(
        [
            {"id": 1, "part": "A", "child": [{"val": "c1"}]},
            {"id": 2, "part": "A", "child": [{"val": "c2"}]},
            {"id": 3, "part": "B", "child": [{"val": "c3"}, {"val": "c4"}]},
        ],
        append=True,
    )

    # reload partition A: 1 replaces its child. 3 and its children are outside the scope and stay
    update = [
        {"id": 1, "part": "A", "child": [{"val": "c1_updated"}]},
        {"id": 2, "part": "A", "child": [{"val": "c2"}]},
    ]
    load(update)
    assert observed() == ([1, 2, 3], ["c1_updated", "c2", "c3", "c4"])
    # the same load again must not accumulate nested rows
    load(update)
    assert observed() == ([1, 2, 3], ["c1_updated", "c2", "c3", "c4"])

    # 1 gains a second child
    load(
        [
            {"id": 1, "part": "A", "child": [{"val": "c1_v3"}, {"val": "c1_extra"}]},
            {"id": 2, "part": "A", "child": [{"val": "c2"}]},
        ]
    )
    assert observed() == ([1, 2, 3], ["c1_extra", "c1_v3", "c2", "c3", "c4"])

    # 2 is not reloaded, so the scope deletes it with its nested rows
    load([{"id": 1, "part": "A", "child": [{"val": "c1_final"}]}])
    assert observed() == ([1, 3], ["c1_final", "c3", "c4"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True, subset=LOCAL_DESTINATIONS),
    ids=lambda x: x.name,
)
def test_delete_insert_destination_scope_hard_delete(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Seeds two partitions and reloads one with a record flagged for hard delete. Checks that the
    scope deletes the flagged record and the merge does not insert it back."""
    columns: TTableSchemaColumns = {"deleted": {"hard_delete": True, "data_type": "bool"}}

    # seed partition A with 1 and 2 and partition B with 3
    p = destination_config.setup_pipeline("di_partition_hard_delete", dev_mode=True)
    target = [
        {"id": 1, "v": "a", "part": "A", "deleted": False},
        {"id": 2, "v": "b", "part": "A", "deleted": False},
        {"id": 3, "v": "c", "part": "B", "deleted": False},
    ]
    assert_load_info(
        p.run(
            merge_resource(target, "delete-insert", columns=columns, append=True),
            **destination_config.run_kwargs,
        )
    )

    # reload partition A: 1 is flagged, 2 changes
    load = [
        {"id": 1, "v": "a", "part": "A", "deleted": True},
        {"id": 2, "v": "b2", "part": "A", "deleted": False},
    ]
    assert_load_info(
        p.run(
            merge_resource(load, "delete-insert", columns=columns, destination_scope="part = 'A'"),
            **destination_config.run_kwargs,
        )
    )

    # the scope deletes 1 and 2, only 2 is inserted back, 3 is outside the scope and stays
    rows = load_tables_to_dicts(p, "items", exclude_system_cols=True)["items"]
    assert sorted((r["id"], r["v"], r["part"]) for r in rows) == [(2, "b2", "A"), (3, "c", "B")]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True, subset=LOCAL_DESTINATIONS),
    ids=lambda x: x.name,
)
def test_delete_insert_no_keys_hard_delete_nested(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Loads records with children without keys, the second load with a record flagged for hard
    delete. Checks that `delete-insert` appends and never inserts the flagged record or its
    child."""
    columns: TTableSchemaColumns = {"deleted": {"hard_delete": True, "data_type": "bool"}}

    # load 1, then 2 together with the flagged 3
    p = destination_config.setup_pipeline("di_no_keys_hard_delete", dev_mode=True)
    for data in (
        [{"id": 1, "deleted": False, "child": [{"val": "a"}]}],
        [
            {"id": 2, "deleted": False, "child": [{"val": "b"}]},
            {"id": 3, "deleted": True, "child": [{"val": "c"}]},
        ],
    ):
        assert_load_info(
            p.run(
                merge_resource(data, "delete-insert", primary_key=None, columns=columns),
                **destination_config.run_kwargs,
            )
        )

    # the second load appends to the first, the flagged 3 and its child are never inserted
    tables = load_tables_to_dicts(p, "items", "items__child", exclude_system_cols=True)
    assert sorted(r["id"] for r in tables["items"]) == [1, 2]
    assert sorted(r["val"] for r in tables["items__child"]) == ["a", "b"]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True, subset=LOCAL_DESTINATIONS),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("merge_strategy", ["delete-insert", "cdc", "upsert"])
def test_source_filter_with_bool_hard_delete(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    """Loads records with a NULL bool `hard_delete` flag and a source filter. Checks that the NULL
    flag does not bypass the filter."""
    skip_if_unsupported_merge_strategy(destination_config, merge_strategy)
    options: Any = {"source_filter": "id > 0"}
    if merge_strategy == "delete-insert":
        options["destination_scope"] = "1 = 1"
    data = [{"id": i, "v": f"v{i}", "deleted": None} for i in range(3)]

    p = destination_config.setup_pipeline("source_filter_bool_hard_delete", dev_mode=True)
    assert_load_info(
        p.run(
            merge_resource(
                data,
                merge_strategy,
                columns={"deleted": {"hard_delete": True, "data_type": "bool"}},
                **options,
            ),
            **destination_config.run_kwargs,
        )
    )
    rows = load_tables_to_dicts(p, "items", exclude_system_cols=True)["items"]
    # the source filter `id > 0` discards 0
    assert sorted(r["id"] for r in rows) == [1, 2]
