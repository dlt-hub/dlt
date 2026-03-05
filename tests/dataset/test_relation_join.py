from dlt.common.runners.pool_runner import T
import tempfile
import pathlib
from dataclasses import dataclass
from typing import Any, Sequence, Union, Callable

import pytest
import sqlglot.expressions as sge

import dlt
from dlt.common.schema.typing import TTableReference
from dlt.dataset.relation import (
    _build_join_condition_from_pairs,
    _resolve_reference_chain,
    _to_join_ref,
    TJoinDirection,
)
from tests.dataset.conftest import TLoadsFixture


@dataclass(frozen=True)
class JoinExpectation:
    kind: str
    pairs: list[tuple[str, str, str, str]]
    target_table: str


def _identifier_name(identifier: Union[sge.Expression, str, None]) -> str:
    if identifier is None:
        raise AssertionError("Expected identifier")
    if isinstance(identifier, sge.Identifier):
        return identifier.name
    if isinstance(identifier, str):
        return identifier
    if hasattr(identifier, "name"):
        return identifier.name
    return str(identifier)


def _flatten_on_pairs(expr: sge.Expression) -> list[tuple[str, str, str, str]]:
    pairs: list[tuple[str, str, str, str]] = []

    def _visit(node: sge.Expression) -> None:
        if isinstance(node, sge.And):
            _visit(node.this)
            _visit(node.expression)
            return
        if not isinstance(node, sge.EQ):
            raise AssertionError(f"Unexpected join condition: {node}")
        left = node.this
        right = node.expression
        if not isinstance(left, sge.Column) or not isinstance(right, sge.Column):
            raise AssertionError(f"Expected column join, got: {node}")
        left_table = _identifier_name(left.args.get("table"))
        left_col = _identifier_name(left.args.get("this"))
        right_table = _identifier_name(right.args.get("table"))
        right_col = _identifier_name(right.args.get("this"))
        pairs.append((left_table, left_col, right_table, right_col))

    _visit(expr)
    return pairs


def _magic_other_table(other: Union[str, dlt.Relation]) -> str:
    if isinstance(other, dlt.Relation):
        if not other.origin_table_name:
            raise AssertionError("Expected relation with origin table")
        return other.origin_table_name
    return other


def _expected_magic_join_plan(
    schema: dlt.Schema,
    origin_table: str,
    other_table: str,
    joined_aliases: dict[str, str],
    next_alias_index: int,
) -> tuple[list[JoinExpectation], dict[str, str], int]:
    refs = _resolve_reference_chain(schema, origin_table, other_table)
    alias_map = joined_aliases.copy()
    base_alias = alias_map[origin_table]
    start_index = next_alias_index
    expectations: list[JoinExpectation] = []

    for ref in refs:
        if ref.target_table in alias_map:
            base_alias = alias_map[ref.target_table]
            continue
        right_alias = f"t{start_index}"
        pairs = [
            (base_alias, left_col, right_alias, right_col) for left_col, right_col in ref.on_pairs
        ]
        expectations.append(
            JoinExpectation(
                kind=ref.direction,
                pairs=pairs,
                target_table=ref.target_table,
            )
        )
        alias_map[ref.target_table] = right_alias
        base_alias = right_alias
        start_index += 1

    return expectations, alias_map, start_index


@pytest.mark.parametrize(
    "ref,direction,match",
    [
        (
            TTableReference(
                referenced_table="users", columns=["user_id"], referenced_columns=["id"]
            ),
            "right",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(table="users__orders", columns=["user_id"], referenced_columns=["id"]),
            "right",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=[],
                referenced_columns=["id"],
            ),
            "right",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id"],
                referenced_columns=[],
            ),
            "left",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id", "tenant_id"],
                referenced_columns=["id"],
            ),
            "right",
            "'columns' or 'referenced_columns' are empty",
        ),
    ],
    ids=[
        "missing-table",
        "missing-referenced-table",
        "empty-columns",
        "empty-referenced-columns",
        "columns-length-mismatch",
    ],
)
def test_to_join_ref_rejects_malformed(
    ref: TTableReference, direction: TJoinDirection, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        _to_join_ref(ref, direction)


def test_build_join_condition_rejects_empty_pairs() -> None:
    with pytest.raises(ValueError, match="Cannot build join condition from empty column pairs"):
        _build_join_condition_from_pairs([], left_alias="a", right_alias="b")


def test_resolve_reference_chain_rejects_self_join(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match="Cannot join a table to itself"):
        _resolve_reference_chain(dataset.schema, "users", "users")


@pytest.mark.parametrize("dataset_with_loads", ["with_root_key"], indirect=True)
def test_join_rejects_cross_dataset(dataset_with_loads: TLoadsFixture) -> None:
    """Test that joining relations from different datasets raises an error."""
    dataset, _, _ = dataset_with_loads

    with tempfile.TemporaryDirectory() as tmp:
        pipeline = dlt.pipeline(
            pipeline_name="other_dataset",
            pipelines_dir=str(pathlib.Path(tmp) / "pipelines_dir"),
            destination=dlt.destinations.duckdb(str(pathlib.Path(tmp) / "other.db")),
            dev_mode=True,
        )

        @dlt.resource
        def other_data():
            yield {"id": 1, "name": "test"}

        pipeline.run([other_data])
        other_dataset = pipeline.dataset()

        # Try to join with a relation from the other dataset
        rel = dataset.table("users")
        other_rel = other_dataset.table("other_data")

        with pytest.raises(ValueError, match="different datasets"):
            rel.join(other_rel)


@pytest.mark.parametrize(
    "dataset_with_loads,left,right,expected_directions",
    [
        pytest.param("with_root_key", "users__orders", "users", ["right"], id="child-to-parent"),
        pytest.param("with_root_key", "users", "users__orders", ["left"], id="parent-to-child"),
        pytest.param(
            "with_root_key",
            "users__orders__items",
            "users",
            ["right"],
            id="items-to-root-root-key",
        ),
        pytest.param(
            "without_root_key",
            "users__orders__items",
            "users",
            ["right", "right"],
            id="items-to-root-parent-key",
        ),
        pytest.param(
            "with_root_key",
            "users",
            "users__orders__items",
            ["left"],
            id="root-to-items-root-key",
        ),
        pytest.param(
            "without_root_key",
            "users",
            "users__orders__items",
            ["left", "left"],
            id="root-to-items-parent-key",
        ),
    ],
    indirect=["dataset_with_loads"],
)
def test_resolve_reference_chain_matrix(
    dataset_with_loads: TLoadsFixture,
    left: str,
    right: str,
    expected_directions: Sequence[TJoinDirection],
) -> None:
    dataset, _, _ = dataset_with_loads
    refs = _resolve_reference_chain(dataset.schema, left, right)

    assert [ref.direction for ref in refs] == list(expected_directions)
    assert len(refs) == len(expected_directions)


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
    transform: Callable[[dlt.Relation], dlt.Relation],
) -> None:
    dataset, _, _ = dataset_with_loads
    base = dataset.table("users__orders")
    transformed = transform(base)

    assert base.origin_table_name == "users__orders"
    assert transformed.origin_table_name == "users__orders"


@pytest.mark.parametrize(
    "build_rel,other,match",
    [
        pytest.param(
            lambda ds: ds.table("users"),
            "users",
            "Cannot join a table to itself",
            id="self-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders"),
            "products",
            "Unable to resolve reference chain",
            id="unrelated-tables",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").select("order_id"),
            "users",
            "not a join-graph relation",
            id="self-not-joinable-select",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").where("order_id", "gt", 0),
            "users",
            "not a join-graph relation",
            id="self-not-joinable-where",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").order_by("order_id"),
            "users",
            "not a join-graph relation",
            id="self-not-joinable-order-by",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").limit(1),
            "users",
            "not a join-graph relation",
            id="self-not-joinable-limit",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").select("order_id").max(),
            "users",
            "not a join-graph relation",
            id="self-not-joinable-agg",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders"),
            lambda ds: ds.table("users").select("id"),
            "not a join-graph relation",
            id="other-not-joinable",
        ),
        pytest.param(
            lambda ds: ds.table("users"),
            123,
            "`other` must be a table name or a base table relation",
            id="invalid-other-type",
        ),
        pytest.param(
            lambda ds: ds.table("users"),
            "table_does_not_exist",
            "not found in dataset schema",
            id="unknown-table",
        ),
        pytest.param(
            lambda ds: ds.query("SELECT * FROM users"),
            "users__orders",
            "no base table",
            id="query-relation-not-joinable",
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_magic_join_rejection_matrix(
    dataset_with_loads: TLoadsFixture,
    build_rel: Callable[[dlt.Dataset], dlt.Relation],
    other: Any,
    match: str,
) -> None:
    dataset, _, _ = dataset_with_loads
    rel = build_rel(dataset)
    target = other(dataset) if callable(other) else other

    with pytest.raises(ValueError, match=match):
        rel.join(target)


@pytest.mark.parametrize(
    "dataset_with_loads,build_rel,other",
    [
        pytest.param(
            "with_root_key",
            lambda ds: ds.table("users__orders"),
            "users",
            id="child-to-parent",
        ),
        pytest.param(
            "with_root_key",
            lambda ds: ds.table("users"),
            "users__orders",
            id="parent-to-child",
        ),
        pytest.param(
            "with_root_key",
            lambda ds: ds.table("users__orders__items"),
            "users",
            id="multi-hop-to-root",
        ),
        pytest.param(
            "without_root_key",
            lambda ds: ds.table("users__orders__items"),
            "users",
            id="multi-hop-to-root-parent-key",
        ),
        pytest.param(
            "with_root_key",
            lambda ds: ds.table("users__orders").join("users"),
            "users__orders__items",
            id="chain-with-existing-join",
        ),
        pytest.param(
            "without_root_key",
            lambda ds: ds.table("users__orders__items").join("users__orders"),
            "users",
            id="reuse-joined-alias",
        ),
        pytest.param(
            "with_root_key",
            lambda ds: ds.table("users__orders__items"),
            lambda ds: ds.table("users__orders").join("users"),
            id="joinable-graph-other",
        ),
    ],
    indirect=["dataset_with_loads"],
)
def test_magic_join_plan_matrix(
    dataset_with_loads: TLoadsFixture,
    build_rel: Callable[[dlt.Dataset], dlt.Relation],
    other: Any,
) -> None:
    dataset, _, _ = dataset_with_loads
    rel = build_rel(dataset)
    target = other(dataset) if callable(other) else other
    existing_joins = rel.sqlglot_expression.args.get("joins") or []

    assert rel.origin_table_name is not None
    other_table = _magic_other_table(target)
    initial_aliases = rel._joined_table_aliases or {rel.origin_table_name: "t0"}
    next_index = (
        rel._next_join_alias_index
        if rel._next_join_alias_index is not None
        else len(initial_aliases)
    )
    expected_joins, expected_aliases, expected_next_index = _expected_magic_join_plan(
        dataset.schema,
        rel.origin_table_name,
        other_table,
        initial_aliases,
        next_index,
    )

    joined = rel.join(target)

    assert joined.origin_table_name == rel.origin_table_name
    assert joined._is_joinable_graph is True

    actual_joins = joined.sqlglot_expression.args.get("joins") or []
    new_joins = actual_joins[len(existing_joins) :]
    assert len(new_joins) == len(expected_joins)

    for actual, expected in zip(new_joins, expected_joins):
        assert actual.args.get("kind", "").lower() == expected.kind
        assert isinstance(actual.this, sge.Table)
        assert _identifier_name(actual.this.this) == expected.target_table
        actual_pairs = _flatten_on_pairs(actual.args["on"])
        assert set(actual_pairs) == set(expected.pairs)

    assert joined._joined_table_aliases == expected_aliases
    assert joined._next_join_alias_index == expected_next_index


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
    # 4 orders, no user has no orders which would create more rows due to child -> parent join
    assert len(df) == 4


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
