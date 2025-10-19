import sys

import pytest

import dlt

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

    pipeline = dlt.pipeline("_relation_to_ibis", destination="duckdb")
    pipeline.run([purchases])
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
