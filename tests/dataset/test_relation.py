import sys

import pytest

import dlt
from dlt.common import Decimal

# TODO move destination-independent tests from `test_read_interfaces.py` to this module


@pytest.fixture
def dataset() -> dlt.Dataset:
    @dlt.resource
    def purchases():
        yield from (
            {"id": 1, "name": "alice", "city": "berlin"},
            {"id": 2, "name": "bob", "city": "paris"},
            {"id": 3, "name": "charlie", "city": "barcelona"},
        )

    @dlt.resource
    def items():
        yield from (
            {"id": 1, "name": "labubu", "price": Decimal("23.1")},
            {"id": 2, "name": "ububal", "price": Decimal("13.2")},
        )

    pipeline = dlt.pipeline("_relation_to_ibis", destination="duckdb")
    pipeline.run([purchases, items])
    return pipeline.dataset()


@pytest.fixture
def purchases(dataset: dlt.Dataset) -> dlt.Relation:
    purchases = dataset.table("purchases")
    assert isinstance(purchases, dlt.Relation)
    return purchases


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_sql_relation_to_ibis(dataset: dlt.Dataset) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` defined by an SQL query"""
    from ibis import ir

    purchases = dataset.query("SELECT * FROM purchases")
    assert isinstance(purchases, dlt.Relation)

    table = purchases.to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_base_relation_to_ibis(purchases: dlt.Relation) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` defined by an existing table name"""
    from ibis import ir

    table = purchases.to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_transformed_relation_to_ibis_(purchases: dlt.Relation) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` that was transformed by methods"""
    from ibis import ir

    table = purchases.where("id", "gt", 2).select("name").to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


def test_relation_extraction(purchases: dlt.Relation) -> None:
    from dlt.extract.hints import DLT_HINTS_METADATA_KEY

    r_ = dlt.resource(purchases)
    assert r_.name == "purchases"
    results = list(r_)
    assert len(results) == 1
    assert len(results[0]) == 3
    # assert column names are same as relation columns
    arrow_table = results[0]
    assert list(arrow_table.column_names) == purchases.columns
    # assert that DLT_HINTS_METADATA_KEY is there in arrow metadata
    assert DLT_HINTS_METADATA_KEY.encode("utf-8") in arrow_table.schema.metadata


def test_relation_list_extraction(dataset: dlt.Dataset) -> None:
    purchases = dataset.table("purchases")

    # note that list element when passed to the decorator are not separately wrapped
    r_ = dlt.resource([purchases], name="fail")
    assert list(r_)[0] is purchases

    # but in pipeline they will
    pipeline = dlt.pipeline("double_relation", destination="duckdb", dataset_name="_data")
    pipeline.run([purchases])
    table = pipeline.dataset().table("purchases").arrow()
    # assert column names are same as relation columns
    assert list(table.column_names) == purchases.columns
    assert len(table) == 3

    items = dataset.table("items")
    pipeline.run([purchases, items])
    table = pipeline.dataset().table("purchases").arrow()
    assert list(table.column_names) == purchases.columns
    assert len(table) == 6
    table = pipeline.dataset().table("items").arrow()
    assert list(table.column_names) == items.columns
    assert len(table) == 2
