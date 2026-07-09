from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

import pytest

duckdb = pytest.importorskip("duckdb")

import pyarrow as pa

import dlt
from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.destinations import hotdata as hotdata_factory
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.hotdata.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
)
from dlt.destinations.impl.hotdata.errors import HotdataTerminalError, HotdataTransientError
from dlt.destinations.impl.hotdata.hotdata import HotdataClient
from dlt.destinations.impl.hotdata.sql_client import HotdataSqlClient

pytestmark = pytest.mark.essential

ITEMS_ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


class _FakeHotdataApi:
    def __init__(self, tables: Dict[str, Optional[pa.Table]]) -> None:
        self.tables = tables
        self.fetch_calls: List[str] = []

    def fetch_table(self, *, database: str, schema: str, table: str) -> Optional[pa.Table]:
        self.fetch_calls.append(f"{database}.{schema}.{table}")
        return self.tables.get(table)


@pytest.fixture
def fake_api(monkeypatch: pytest.MonkeyPatch) -> _FakeHotdataApi:
    api = _FakeHotdataApi({"items": pa.Table.from_pylist(ITEMS_ROWS)})

    @contextmanager
    def _fake_hotdata_api(config: HotdataClientConfiguration) -> Iterator[_FakeHotdataApi]:
        yield api

    monkeypatch.setattr("dlt.destinations.impl.hotdata.sql_client._hotdata_api", _fake_hotdata_api)
    return api


def _make_schema() -> Schema:
    schema = Schema("hot")
    for table_name in ("items", "missing_items"):
        table = new_table(
            table_name,
            columns=[
                {"name": "id", "data_type": "bigint"},
                {"name": "v", "data_type": "text"},
            ],
        )
        table["x-normalizer"] = {"seen-data": True}
        schema.update_table(table)
    return schema


def _make_client(always_refresh_views: bool = False) -> HotdataClient:
    config = HotdataClientConfiguration()
    config.credentials = HotdataCredentials()
    config.credentials.api_key = "key"
    config.credentials.workspace_id = "ws"
    config.always_refresh_views = always_refresh_views
    return HotdataClient(_make_schema(), config, hotdata_factory()._raw_capabilities())


def test_sql_client_wiring() -> None:
    client = _make_client()
    assert client.sql_client_class is HotdataSqlClient
    sql_client = client.sql_client
    assert isinstance(sql_client, HotdataSqlClient)
    # lazily created once, then cached
    assert client.sql_client is sql_client
    replacement = client.sql_client_class(client)
    client.sql_client = replacement
    assert client.sql_client is replacement


@pytest.mark.parametrize(
    "table_ref",
    ["items", "public.items"],
    ids=["unqualified", "schema-qualified"],
)
def test_execute_sql_over_remote_table(fake_api: _FakeHotdataApi, table_ref: str) -> None:
    client = _make_client()
    with client.sql_client as sql_client:
        rows = sql_client.execute_sql(f"SELECT id, v FROM {table_ref} ORDER BY id")
    assert rows == [(1, "a"), (2, "b")]
    assert fake_api.fetch_calls == ["dlt.public.items"]


def test_cursor_arrow_and_df(fake_api: _FakeHotdataApi) -> None:
    client = _make_client()
    with client.sql_client as sql_client:
        with sql_client.execute_query("SELECT id, v FROM items ORDER BY id") as cursor:
            arrow_table = cursor.arrow()
        with sql_client.execute_query("SELECT id, v FROM items ORDER BY id") as cursor:
            df = cursor.df()
    assert arrow_table.to_pylist() == ITEMS_ROWS
    assert df["v"].tolist() == ["a", "b"]


@pytest.mark.parametrize(
    "always_refresh_views,expected_fetches",
    [(False, 1), (True, 2)],
    ids=["cache-per-session", "always-refresh"],
)
def test_snapshot_refresh_policy(
    fake_api: _FakeHotdataApi, always_refresh_views: bool, expected_fetches: int
) -> None:
    client = _make_client(always_refresh_views=always_refresh_views)
    with client.sql_client as sql_client:
        sql_client.execute_sql("SELECT * FROM items")
        sql_client.execute_sql("SELECT * FROM items")
    assert len(fake_api.fetch_calls) == expected_fetches


def test_never_loaded_table_raises_undefined_relation(fake_api: _FakeHotdataApi) -> None:
    client = _make_client()
    with client.sql_client as sql_client:
        with pytest.raises(DatabaseUndefinedRelation):
            sql_client.execute_sql("SELECT * FROM missing_items")


@pytest.mark.parametrize(
    "error,expected_type",
    [
        (KeyError("db not found"), DatabaseUndefinedRelation),
        (HotdataTransientError("throttled"), DatabaseTransientException),
        (HotdataTerminalError("bad request"), DatabaseTerminalException),
        (duckdb.CatalogException("no table"), DatabaseUndefinedRelation),
    ],
    ids=["key-error", "transient", "terminal", "duckdb-catalog"],
)
def test_make_database_exception_mapping(error: Exception, expected_type: type) -> None:
    assert isinstance(HotdataSqlClient._make_database_exception(error), expected_type)


def test_dataset_access(fake_api: _FakeHotdataApi) -> None:
    """The logical dataset name plays no addressing role — data comes from config database/schema."""
    dest = hotdata_factory(credentials={"api_key": "key", "workspace_id": "ws"})
    dataset = dlt.dataset(dest, "logical_name", schema=_make_schema())
    relation = dataset.table("items")
    assert relation.arrow().to_pylist() == ITEMS_ROWS
    assert relation.df()["id"].tolist() == [1, 2]
    assert fake_api.fetch_calls[0] == "dlt.public.items"
