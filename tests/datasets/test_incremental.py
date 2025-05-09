from typing import Union, Generator

import pytest
import duckdb
from sqlglot.executor import execute

import dlt
from dlt.destinations import duckdb as duckdb_dest
from dlt.common.exceptions import DltException
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.destinations.dataset.dataset import (
    ReadableDBAPIDataset,
    LOAD_ID_COL_ON_LOADS_TABLE,
    _list_load_ids_expr,
    _latest_load_id_expr,
    _filter_min_max,
    _filter_root_table_expr,
    _filter_child_table_expr,
)


DATASET_NAME = "dataset_foo"

DLT_LOAD_TABLE_NAME = "_dlt_loads"
DLT_LOADS_ROWS = [
    {LOAD_ID_COL_ON_LOADS_TABLE: "foo", "status": 0},
    {LOAD_ID_COL_ON_LOADS_TABLE: "bar", "status": 0},
    {LOAD_ID_COL_ON_LOADS_TABLE: "baz", "status": 1},
]

ROOT_TABLE_NAME = "orders"
ROW_KEY = "_dlt_id"
ROOT_TABLE_ROWS = [
    {"value": 0, ROW_KEY: "abc", C_DLT_LOAD_ID: "foo"},
    {"value": 1, ROW_KEY: "def", C_DLT_LOAD_ID: "foo"},
    {"value": 2, ROW_KEY: "ghi", C_DLT_LOAD_ID: "bar"},
    {"value": 3, ROW_KEY: "jkl", C_DLT_LOAD_ID: "baz"},
]

CHILD_TABLE_NAME = "items"
ROOT_KEY = "_dlt_root_key"
CHILD_TABLE_ROWS = [
    {"value": 0.0, ROW_KEY: "ZYX", ROOT_KEY: "abc"},
    {"value": 1.0, ROW_KEY: "WVU", ROOT_KEY: "abc"},
    {"value": 2.0, ROW_KEY: "TSR", ROOT_KEY: "ghi"},
]

# TODO simplify this by efficiently generating that schema
MOCK_SCHEMA_DICT = {
    "version": 2,
    "engine_version": 11,
    "previous_hashes": [],
    "version_hash": "abc",
    "normalizers": {
        "names": "snake_case",
        "json": {"module": "dlt.common.normalizers.json.relational"},
    },
    "name": DATASET_NAME,
    "tables": {
        DLT_LOAD_TABLE_NAME: {
            "name": DLT_LOAD_TABLE_NAME,
            "columns": {
                LOAD_ID_COL_ON_LOADS_TABLE: {
                    "name": LOAD_ID_COL_ON_LOADS_TABLE,
                    "data_type": "text",
                },
                "status": {"name": "status", "data_type": "bigint"},
            },
        },
        ROOT_TABLE_NAME: {
            "name": ROOT_TABLE_NAME,
            "columns": {
                "value": {"name": "value", "data_type": "bigint"},
                ROW_KEY: {"name": ROW_KEY, "row_key": True, "data_type": "text"},
                C_DLT_LOAD_ID: {"name": C_DLT_LOAD_ID, "data_type": "text"},
            },
        },
        CHILD_TABLE_NAME: {
            "name": CHILD_TABLE_NAME,
            "columns": {
                "value": {"name": "value", "data_type": "bigint"},
                ROW_KEY: {"name": ROW_KEY, "row_key": True, "data_type": "text"},
                ROOT_KEY: {"name": ROOT_KEY, "root_key": True, "data_type": "text"},
            },
            "parent": ROOT_TABLE_NAME,
        },
        "_dlt_version": {"columns": {}},
    },
}


# TODO could improve test speed my reusing connection across stateless tests
@pytest.fixture(scope="function")
def dataset() -> Generator[ReadableDBAPIDataset, None, None]:
    duckdb_con = duckdb.connect(":memory:")

    duckdb_con.execute(f"CREATE SCHEMA IF NOT EXISTS {DATASET_NAME}")

    # loads table
    duckdb_con.execute(
        f"CREATE TABLE {DATASET_NAME}.{DLT_LOAD_TABLE_NAME} ({LOAD_ID_COL_ON_LOADS_TABLE} TEXT,"
        " status INT)"
    )
    duckdb_con.executemany(
        query=(
            f"INSERT INTO {DATASET_NAME}.{DLT_LOAD_TABLE_NAME} ({LOAD_ID_COL_ON_LOADS_TABLE},"
            " status) VALUES (?, ?)"
        ),
        parameters=[tuple(r.values()) for r in DLT_LOADS_ROWS],
    )

    # root table
    duckdb_con.execute(
        f"CREATE TABLE {DATASET_NAME}.{ROOT_TABLE_NAME} (value INT, {ROW_KEY} TEXT,"
        f" {C_DLT_LOAD_ID} TEXT)"
    )
    duckdb_con.executemany(
        query=(
            f"INSERT INTO {DATASET_NAME}.{ROOT_TABLE_NAME} (value, {ROW_KEY}, {C_DLT_LOAD_ID})"
            " VALUES (?, ?, ?)"
        ),
        parameters=[tuple(r.values()) for r in ROOT_TABLE_ROWS],
    )

    # child table
    duckdb_con.execute(
        f"CREATE TABLE {DATASET_NAME}.{CHILD_TABLE_NAME} (value INT, {ROW_KEY} TEXT,"
        f" {ROOT_KEY} TEXT)"
    )
    duckdb_con.executemany(
        query=(
            f"INSERT INTO {DATASET_NAME}.{CHILD_TABLE_NAME} (value, {ROW_KEY}, {ROOT_KEY}) VALUES"
            " (?, ?, ?)"
        ),
        parameters=[tuple(r.values()) for r in CHILD_TABLE_ROWS],
    )

    duckdb_con.commit()

    # NOTE must set `dataset_type="default"` to avoid using Ibis dataset and relation
    yield ReadableDBAPIDataset(
        destination=duckdb_dest(duckdb_con),
        dataset_name=DATASET_NAME,
        schema=dlt.Schema.from_dict(MOCK_SCHEMA_DICT),
        dataset_type="default",
    )


@pytest.mark.parametrize(
    "kwargs, expected_query",
    [
        (
            {},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads ORDER BY"
                f" {LOAD_ID_COL_ON_LOADS_TABLE} DESC"
            ),
        ),
        (
            {"status": None},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads ORDER BY"
                f" {LOAD_ID_COL_ON_LOADS_TABLE} DESC"
            ),
        ),
        (
            {"status": 0},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads WHERE status = 0 ORDER BY"
                f" {LOAD_ID_COL_ON_LOADS_TABLE} DESC"
            ),
        ),
        (
            {"status": [0]},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads WHERE status IN (0) ORDER BY"
                f" {LOAD_ID_COL_ON_LOADS_TABLE} DESC"
            ),
        ),
        (
            {"status": [0, 1]},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads WHERE status IN (0, 1) ORDER"
                f" BY {LOAD_ID_COL_ON_LOADS_TABLE} DESC"
            ),
        ),
        (
            {"limit": 2},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads ORDER BY"
                f" {LOAD_ID_COL_ON_LOADS_TABLE} DESC LIMIT 2"
            ),
        ),
        (
            {"status": [0, 1], "limit": 2},
            (
                f"SELECT {LOAD_ID_COL_ON_LOADS_TABLE} FROM _dlt_loads WHERE status IN (0, 1) ORDER"
                f" BY {LOAD_ID_COL_ON_LOADS_TABLE} DESC LIMIT 2"
            ),
        ),
        ({"status": []}, DltException()),
        ({"status": [0, "1"]}, DltException()),
        ({"status": "1"}, DltException()),
        ({"limit": 0}, DltException()),
        ({"limit": "1"}, DltException()),
    ],
)
def test_list_load_ids_expr(dataset, kwargs: dict, expected_query: Union[str, Exception]) -> None:
    """Test query generation"""
    if isinstance(expected_query, Exception):
        with pytest.raises(DltException):
            _list_load_ids_expr(**kwargs)
        with pytest.raises(DltException):
            dataset.list_load_ids(**kwargs)
    else:
        query = _list_load_ids_expr(**kwargs)
        relation = dataset.list_load_ids(**kwargs)
        assert query.sql() == relation.query() == expected_query


@pytest.mark.parametrize(
    "kwargs, expected_load_ids",
    [
        ({}, ["foo", "baz", "bar"]),  # NOTE results are ORDER BY DESC
        ({"status": None}, ["foo", "baz", "bar"]),
        ({"status": 0}, ["foo", "bar"]),
        ({"status": [0]}, ["foo", "bar"]),
        ({"status": [0, 1]}, ["foo", "baz", "bar"]),
    ],
)
def test_list_load_ids_execution(
    dataset, kwargs: dict, expected_load_ids: Union[str, Exception]
) -> None:
    """Test query generation"""
    # mock execution
    query = _list_load_ids_expr(**kwargs)
    rows = execute(query.sql(), tables={DLT_LOAD_TABLE_NAME: DLT_LOADS_ROWS})
    assert [r[LOAD_ID_COL_ON_LOADS_TABLE] for r in rows] == expected_load_ids

    # duckdb execution
    relation = dataset.list_load_ids(**kwargs)
    results = relation.fetchall()
    assert [r[0] for r in results] == expected_load_ids


@pytest.mark.parametrize(
    "kwargs, expected_query",
    [
        (
            {},
            f"SELECT MAX({LOAD_ID_COL_ON_LOADS_TABLE}) AS load_id FROM _dlt_loads",
        ),
        (
            {"status": None},
            f"SELECT MAX({LOAD_ID_COL_ON_LOADS_TABLE}) AS load_id FROM _dlt_loads",
        ),
        (
            {"status": 0},
            f"SELECT MAX({LOAD_ID_COL_ON_LOADS_TABLE}) AS load_id FROM _dlt_loads WHERE status = 0",
        ),
        (
            {"status": [0]},
            (
                f"SELECT MAX({LOAD_ID_COL_ON_LOADS_TABLE}) AS load_id FROM _dlt_loads WHERE status"
                " IN (0)"
            ),
        ),
        (
            {"status": [0, 1]},
            (
                f"SELECT MAX({LOAD_ID_COL_ON_LOADS_TABLE}) AS load_id FROM _dlt_loads WHERE status"
                " IN (0, 1)"
            ),
        ),
        ({"status": []}, DltException()),
        ({"status": [0, "1"]}, DltException()),
        ({"status": "1"}, DltException()),
    ],
)
def test_latest_load_ids_expr(dataset, kwargs: dict, expected_query: Union[str, Exception]) -> None:
    """Test query generation"""
    if isinstance(expected_query, Exception):
        with pytest.raises(DltException):
            _latest_load_id_expr(**kwargs)
        with pytest.raises(DltException):
            dataset.latest_load_id(**kwargs)
    else:
        query = _latest_load_id_expr(**kwargs)
        relation = dataset.latest_load_id(**kwargs)
        assert query.sql() == expected_query
        assert query.sql() == relation.query() == expected_query


@pytest.mark.parametrize(
    "kwargs, expected_load_id",
    [
        ({}, "foo"),
        ({"status": None}, "foo"),
        ({"status": 1}, "baz"),
        ({"status": [1]}, "baz"),
        ({"status": [0, 1]}, "foo"),
    ],
)
def test_latest_load_ids_execution(
    dataset, kwargs: dict, expected_load_id: Union[str, Exception]
) -> None:
    """Test query generation"""
    # mock execution
    query = _latest_load_id_expr(**kwargs)
    rows = execute(query.sql(), tables={DLT_LOAD_TABLE_NAME: DLT_LOADS_ROWS})
    assert [r[LOAD_ID_COL_ON_LOADS_TABLE] for r in rows] == [expected_load_id]

    # duckdb execution
    relation = dataset.latest_load_id(**kwargs)
    results = relation.fetchall()
    assert [r[0] for r in results] == [expected_load_id]


@pytest.mark.parametrize(
    "kwargs, expected_load_ids",
    [
        ({"lt": "foo"}, ["bar", "baz"]),
        ({"le": "foo"}, ["bar", "baz", "foo"]),
        ({"gt": "baz"}, ["foo"]),
        ({"ge": "baz"}, ["baz", "foo"]),
        ({"gt": "bar", "lt": "foo"}, ["baz"]),
        ({"gt": "bar", "le": "foo"}, ["baz", "foo"]),
        ({"ge": "baz", "lt": "foo"}, ["baz"]),
        ({"ge": "baz", "le": "foo"}, ["baz", "foo"]),
        # TODO add cases where we expect an exception to raise; i.e., `gt < lt`
    ],
)
def test_filter_min_max_execution(
    dataset, kwargs, expected_load_ids: Union[set[str], Exception]
) -> None:
    query = _list_load_ids_expr()

    query = _filter_min_max(query, **kwargs)
    if "ge" in kwargs and "le" in kwargs:
        assert "BETWEEN" in query.sql()

    # mock execution
    rows = execute(query.sql(), tables={DLT_LOAD_TABLE_NAME: DLT_LOADS_ROWS})
    assert set([r[LOAD_ID_COL_ON_LOADS_TABLE] for r in rows]) == set(expected_load_ids)

    # duckdb execution
    relation = dataset.list_load_ids(**kwargs)
    results = relation.fetchall()
    assert set([r[0] for r in results]) == set(expected_load_ids)


def test_incremental_sets_load_ids(dataset) -> None:
    dataset_pre = dataset

    dataset_post = dataset_pre.incremental(gt="boz")

    assert isinstance(dataset_post, ReadableDBAPIDataset)
    # .incremental() sets an attribute and returns Self, not a copy
    assert dataset_pre is dataset_post
    assert dataset_post._load_ids == set(["foo"])


@pytest.mark.parametrize(
    "load_ids, expected_dlt_ids",
    [
        (set(["bar", "baz"]), ["ghi", "jkl"]),
        (set(["foo", "bar"]), ["abc", "def", "ghi"]),
        (set(["foo"]), ["abc", "def"]),
        (set(), []),  # TODO should this raise? currently returns empty set
        # TODO add case where load_id exists, but no row found
    ],
)
def test_incremental_on_root_table(
    dataset, load_ids: set[str], expected_dlt_ids: Union[list[str], Exception]
) -> None:
    query = _filter_root_table_expr(ROOT_TABLE_NAME, load_ids=load_ids)
    assert all(load_id in query.sql() for load_id in load_ids)

    # mock execution
    rows = execute(
        query.sql(), tables={ROOT_TABLE_NAME: ROOT_TABLE_ROWS, DLT_LOAD_TABLE_NAME: DLT_LOADS_ROWS}
    )
    assert set([r[ROW_KEY] for r in rows]) == set(expected_dlt_ids)

    # duckdb execution
    dataset._load_ids = load_ids
    relation = dataset.table(ROOT_TABLE_NAME)
    results = relation.fetchall()
    assert set([r[1] for r in results]) == set(expected_dlt_ids)


@pytest.mark.parametrize(
    "load_ids, expected_dlt_ids",
    [
        (set(["bar", "baz"]), ["TSR"]),
        (set(["foo", "bar"]), ["ZYX", "WVU", "TSR"]),
        (set(["foo"]), ["ZYX", "WVU"]),
        (set(["baz"]), []),
        (set(), []),  # TODO should this raise? currently returns empty set
    ],
)
def test_incremental_on_child_table(
    dataset, load_ids: set[str], expected_dlt_ids: Union[list[str], Exception]
) -> None:
    query = _filter_child_table_expr(
        table_name=CHILD_TABLE_NAME,
        table_root_key=ROOT_KEY,
        root_table_name=ROOT_TABLE_NAME,
        root_table_row_key=ROW_KEY,
        load_ids=load_ids,
    )
    assert all(load_id in query.sql() for load_id in load_ids)

    # NOTE for some reason, mock execution fails because it can't resolve joins
    
    # mock execution
    # rows = execute(
    #     query.sql(),
    #     tables={
    #         CHILD_TABLE_NAME: CHILD_TABLE_ROWS,
    #         ROOT_TABLE_NAME: ROOT_TABLE_ROWS,
    #         DLT_LOAD_TABLE_NAME: DLT_LOADS_ROWS,
    #     }
    # )
    # assert set([r[ROW_KEY] for r in rows]) == set(expected_dlt_ids)

    # duckdb execution
    dataset._load_ids = load_ids
    relation = dataset.table(CHILD_TABLE_NAME)
    results = relation.fetchall()
    assert set([r[1] for r in results]) == set(expected_dlt_ids)
