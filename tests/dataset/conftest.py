from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
import dlt
from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    auto_test_run_context,
    deactivate_pipeline,
)

if TYPE_CHECKING:
    import pathlib


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Temporary directory that persist for the lifetime of test `.py` file."""
    return tmp_path_factory.mktemp("pytest-test_relation")


USERS_DATA_0 = [
    {
        "id": 1,
        "name": "Alice",
        "orders": [
            {"order_id": 101, "amount": 100.0, "items": [{"item": "A"}, {"item": "B"}]},
            {"order_id": 102, "amount": 200.0, "items": [{"item": "C"}]},
        ],
    },
    {
        "id": 2,
        "name": "Bob",
        "orders": [{"order_id": 103, "amount": 150.0, "items": [{"item": "D"}]}],
    },
]

USERS_DATA_1 = [
    {
        "id": 3,
        "name": "Charlie",
        "orders": [{"order_id": 104, "amount": 300.0, "items": [{"item": "E"}]}],
    }
]

PRODUCTS_DATA_0 = [{"product_id": 1, "name": "Widget"}, {"product_id": 2, "name": "Gadget"}]
PRODUCTS_DATA_1 = [{"product_id": 3, "name": "Doohickey"}]


@dlt.source(root_key=False)
def crm(i: int = 0):
    @dlt.resource
    def users(i: int):
        if i == 0:
            yield USERS_DATA_0
        elif i == 1:
            yield USERS_DATA_1

    @dlt.resource
    def products(i: int):
        if i == 0:
            yield PRODUCTS_DATA_0
        elif i == 1:
            yield PRODUCTS_DATA_1

    return [users(i), products(i)]


LOAD_0_STATS = {
    "users": len(USERS_DATA_0),
    "products": len(PRODUCTS_DATA_0),
    "users__orders": sum(len(user["orders"]) for user in USERS_DATA_0),  # type: ignore[misc,arg-type]
    "users__orders__items": sum(
        len(order["items"]) for user in USERS_DATA_0 for order in user["orders"]  # type: ignore[misc,attr-defined]
    ),
}
LOAD_1_STATS = {
    "users": len(USERS_DATA_1),
    "products": len(PRODUCTS_DATA_1),
    "users__orders": sum(len(user["orders"]) for user in USERS_DATA_1),  # type: ignore[misc,arg-type]
    "users__orders__items": sum(
        len(order["items"]) for user in USERS_DATA_1 for order in user["orders"]  # type: ignore[misc,attr-defined]
    ),
}


TLoadsFixture = tuple[dlt.Dataset, tuple[str, str], tuple[dict[str, Any], dict[str, Any]]]


@pytest.fixture(scope="module")
def loads_with_root_key(module_tmp_path: pathlib.Path) -> TLoadsFixture:
    pipeline = dlt.pipeline(
        pipeline_name="with_root_key",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "duckdb.db")),
        dev_mode=True,
    )

    source = crm(0)
    source.root_key = True
    pipeline.run(source)
    load_id_1 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    source = crm(1)
    source.root_key = True
    pipeline.run(source)
    load_id_2 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    return (pipeline.dataset(), (load_id_1, load_id_2), (LOAD_0_STATS, LOAD_1_STATS))


@pytest.fixture(scope="module")
def loads_without_root_key(module_tmp_path: pathlib.Path) -> TLoadsFixture:
    pipeline = dlt.pipeline(
        pipeline_name="without_root_key",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "duckdb.db")),
        dev_mode=True,
    )

    source = crm(0)
    source.root_key = False
    pipeline.run(source)
    load_id_1 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    source = crm(1)
    source.root_key = False
    pipeline.run(source)
    load_id_2 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    return (pipeline.dataset(), (load_id_1, load_id_2), (LOAD_0_STATS, LOAD_1_STATS))


@pytest.fixture(params=["with_root_key"])
def dataset_with_loads(
    request: pytest.FixtureRequest,
    loads_with_root_key: TLoadsFixture,
    loads_without_root_key: TLoadsFixture,
) -> TLoadsFixture:
    if request.param == "with_root_key":
        return loads_with_root_key
    if request.param == "without_root_key":
        return loads_without_root_key
    raise ValueError(f"Unknown dataset fixture: {request.param}")


@pytest.fixture(scope="module")
def dataset() -> dlt.Dataset:
    @dlt.resource
    def purchases():
        yield from (
            {"id": 1, "name": "alice", "city": "berlin"},
            {"id": 2, "name": "bob", "city": "paris"},
            {"id": 3, "name": "charlie", "city": "barcelona"},
        )

    pipeline = dlt.pipeline(
        "_relation_to_ibis", destination="duckdb", full_refresh=True, dev_mode=True
    )
    pipeline.run([purchases])
    return pipeline.dataset()


@pytest.fixture
def purchases(dataset: dlt.Dataset) -> dlt.Relation:
    purchases = dataset.table("purchases")
    assert isinstance(purchases, dlt.Relation)
    return purchases
