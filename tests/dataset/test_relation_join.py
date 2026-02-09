from typing import Any

import pytest

import dlt
from dlt.common.schema.typing import TTableReference
from dlt.dataset.relation import _resolve_reference_chain, _to_join_ref
from tests.dataset.conftest import TLoadsFixture


@pytest.mark.parametrize(
    "ref,direction,match",
    [
        (
            TTableReference(
                referenced_table="users", columns=["user_id"], referenced_columns=["id"]
            ),
            "RIGHT",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(table="users__orders", columns=["user_id"], referenced_columns=["id"]),
            "RIGHT",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=[],
                referenced_columns=["id"],
            ),
            "RIGHT",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id"],
                referenced_columns=[],
            ),
            "LEFT",
            "'columns' or 'referenced_columns' are empty",
        ),
    ],
    ids=[
        "missing-table",
        "missing-referenced-table",
        "empty-columns",
        "empty-referenced-columns",
    ],
)
def test_to_join_ref_rejects_malformed(ref: TTableReference, direction: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _to_join_ref(ref, direction)  # type: ignore[arg-type]


def test_resolve_reference_chain_rejects_self_join(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match="Cannot a join table to itself"):
        _resolve_reference_chain(dataset.schema, "users", "users")


@pytest.mark.parametrize(
    "left,right,expected_len,expected_first_direction",
    [
        ("users__orders", "users", 1, "RIGHT"),
        ("users", "users__orders", 1, "LEFT"),
    ],
    ids=["child-to-parent", "parent-to-child"],
)
def test_resolve_reference_chain_direction_and_hops(
    dataset_with_loads: TLoadsFixture,
    left: str,
    right: str,
    expected_len: int,
    expected_first_direction: str,
) -> None:
    dataset, _, _ = dataset_with_loads
    refs = _resolve_reference_chain(dataset.schema, left, right)

    assert len(refs) == expected_len
    assert refs[0]["direction"] == expected_first_direction


@pytest.mark.parametrize(
    "dataset_with_loads,expected_len",
    [
        pytest.param("with_root_key", 1, id="root_key-True"),
        pytest.param("without_root_key", 2, id="root_key-False"),
    ],
    indirect=["dataset_with_loads"],
)
def test_resolve_reference_chain_multi_hop(
    dataset_with_loads: TLoadsFixture,
    expected_len: int,
) -> None:
    dataset, _, _ = dataset_with_loads
    refs = _resolve_reference_chain(dataset.schema, "users__orders__items", "users")

    assert len(refs) == expected_len
    assert refs[0]["direction"] == "RIGHT"


def test_resolve_reference_chain_rejects_unrelated_tables(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match="Unable to resolve reference chain"):
        _resolve_reference_chain(dataset.schema, "products", "users__orders")


@pytest.mark.parametrize(
    "transform",
    [
        lambda r: r.select("order_id"),
        lambda r: r.where("order_id", "gt", 0),
        lambda r: r.order_by("order_id"),
        lambda r: r.limit(1),
        lambda r: r.select("order_id").max(),
    ],
    ids=["select", "where", "order_by", "limit", "agg-max"],
)
def test_origin_table_name_preserved_across_transformations(
    dataset_with_loads: TLoadsFixture,
    transform: Any,
) -> None:
    dataset, _, _ = dataset_with_loads
    base = dataset.table("users__orders")
    transformed = transform(base)

    assert base.origin_table_name == "users__orders"
    assert transformed.origin_table_name == "users__orders"


@pytest.mark.parametrize(
    "case_id,build_rel,other,match",
    [
        (
            "self-not-joinable-select",
            lambda ds: ds.table("users__orders").select("order_id"),
            "users",
            "join-graph relation",
        ),
        (
            "self-not-joinable-where",
            lambda ds: ds.table("users__orders").where("order_id", "gt", 0),
            "users",
            "join-graph relation",
        ),
        (
            "self-not-joinable-order-by",
            lambda ds: ds.table("users__orders").order_by("order_id"),
            "users",
            "join-graph relation",
        ),
        (
            "self-not-joinable-limit",
            lambda ds: ds.table("users__orders").limit(1),
            "users",
            "join-graph relation",
        ),
        (
            "self-not-joinable-agg",
            lambda ds: ds.table("users__orders").select("order_id").max(),
            "users",
            "join-graph relation",
        ),
        (
            "other-not-joinable",
            lambda ds: ds.table("users__orders"),
            lambda ds: ds.table("users").select("id"),
            "Cannot ensure joinability of relation",
        ),
        (
            "invalid-other-type",
            lambda ds: ds.table("users"),
            123,
            "`other` must be a table name or a base table relation",
        ),
        (
            "unknown-table",
            lambda ds: ds.table("users"),
            "table_does_not_exist",
            "not found in dataset schema",
        ),
        (
            "query-relation-not-joinable",
            lambda ds: ds.query("SELECT * FROM users"),
            "users__orders",
            "base or join-graph relation",
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_join_rejection_matrix(
    dataset_with_loads: TLoadsFixture,
    case_id: str,
    build_rel: Any,
    other: Any,
    match: str,
) -> None:
    dataset, _, _ = dataset_with_loads
    rel = build_rel(dataset)
    target = other(dataset) if callable(other) else other

    with pytest.raises(ValueError, match=match):
        rel.join(target)


@pytest.mark.parametrize(
    "case_id,build_rel,other,expected_origin,min_expected_joins",
    [
        (
            "single-hop-by-name",
            lambda ds: ds.table("users__orders"),
            "users",
            "users__orders",
            1,
        ),
        (
            "single-hop-by-relation",
            lambda ds: ds.table("users__orders"),
            lambda ds: ds.table("users"),
            "users__orders",
            1,
        ),
        (
            "chained-two-hops",
            lambda ds: ds.table("users__orders").join("users"),
            "users__orders__items",
            "users__orders",
            2,
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_join_success_matrix(
    dataset_with_loads: TLoadsFixture,
    case_id: str,
    build_rel: Any,
    other: Any,
    expected_origin: str,
    min_expected_joins: int,
) -> None:
    dataset, _, _ = dataset_with_loads
    rel = build_rel(dataset)
    target = other(dataset) if callable(other) else other

    joined = rel.join(target)

    assert isinstance(joined, dlt.Relation)
    assert joined.origin_table_name == expected_origin
    assert joined._is_joinable_graph is True

    joins = joined._sqlglot_expression.args.get("joins", [])
    assert len(joins) >= min_expected_joins


def test_join_rejoin_existing_target_is_idempotent(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    rel = dataset.table("users__orders").join("users")
    joins_before = len(rel._sqlglot_expression.args.get("joins", []))
    aliases_before = rel._joined_table_aliases.copy() if rel._joined_table_aliases else None

    rejoined = rel.join("users")
    joins_after = len(rejoined._sqlglot_expression.args.get("joins", []))
    aliases_after = rejoined._joined_table_aliases

    assert joins_after == joins_before
    assert aliases_after == aliases_before


def _total_rows(load_stats: tuple[dict[str, Any], dict[str, Any]], table_name: str) -> int:
    return sum(stats[table_name] for stats in load_stats)


def test_e2e_join_single_hop_row_count(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, load_stats = dataset_with_loads
    rel = dataset.table("users__orders").join("users")
    df = rel.df()

    assert df is not None
    assert len(df) == _total_rows(load_stats, "users__orders")


@pytest.mark.parametrize(
    "dataset_with_loads",
    [
        pytest.param("with_root_key", id="root_key-True"),
        pytest.param("without_root_key", id="root_key-False"),
    ],
    indirect=True,
)
def test_e2e_join_multi_hop_to_root_row_count(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, load_stats = dataset_with_loads
    rel = dataset.table("users__orders__items").join("users")
    df = rel.df()

    assert df is not None
    assert len(df) == _total_rows(load_stats, "users__orders__items")


def test_e2e_join_chain_row_count(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, load_stats = dataset_with_loads
    rel = dataset.table("users__orders").join("users").join("users__orders__items")
    df = rel.df()

    assert df is not None
    assert len(df) == _total_rows(load_stats, "users__orders__items")


def test_e2e_join_rejects_transformed_relation(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    transformed = dataset.table("users__orders").select("order_id")

    with pytest.raises(ValueError, match="join-graph relation"):
        transformed.join("users")
