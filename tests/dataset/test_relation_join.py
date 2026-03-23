import tempfile
import pathlib
from typing import Any, Sequence, Callable, TypedDict

import pytest
import sqlglot.expressions as sge

import dlt
from dlt.common.schema.typing import TTableReference
from dlt.dataset._join import (
    _build_join_condition_from_pairs,
    _resolve_reference_chain,
    _to_join_ref,
)
from dlt.dataset.relation import TJoinType
from tests.dataset.utils import TLoadsFixture


class _ColumnRef(TypedDict):
    """One side of a join ON equality: a qualified column reference."""

    qualifier: str
    column: str


class JoinExpectation(TypedDict):
    """Expected shape of a single JOIN clause added by ``Relation.join``."""

    target_table: str
    pairs: list[tuple[_ColumnRef, _ColumnRef]]


@pytest.fixture
def join_dataset(request: pytest.FixtureRequest) -> dlt.Dataset:
    dataset_fixture_name, dataset_variant = request.param

    if dataset_fixture_name == "dataset_with_loads":
        loads_fixture_name = f"loads_{dataset_variant}"
        dataset, _, _ = request.getfixturevalue(loads_fixture_name)
        return dataset
    if dataset_fixture_name == "dataset_with_annotated_references":
        return request.getfixturevalue("dataset_with_annotated_references")

    raise ValueError(f"Unknown join dataset fixture: {dataset_fixture_name}")


def _flatten_on_pairs(
    expr: sge.Expression,
) -> list[tuple[_ColumnRef, _ColumnRef]]:
    """Extract ``(left, right)`` column-ref pairs from a JOIN ON expression."""
    pairs: list[tuple[_ColumnRef, _ColumnRef]] = []

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
        pairs.append(
            (
                _ColumnRef(
                    qualifier=left.args["table"].name,
                    column=left.args["this"].name,
                ),
                _ColumnRef(
                    qualifier=right.args["table"].name,
                    column=right.args["this"].name,
                ),
            )
        )

    _visit(expr)
    return pairs


@pytest.mark.parametrize(
    "ref,from_table,match",
    [
        (
            TTableReference(
                referenced_table="users", columns=["user_id"], referenced_columns=["id"]
            ),
            "users__orders",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(table="users__orders", columns=["user_id"], referenced_columns=["id"]),
            "users",
            "missing 'table' or 'referenced_table'",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=[],
                referenced_columns=["id"],
            ),
            "users__orders",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id"],
                referenced_columns=[],
            ),
            "users",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id", "tenant_id"],
                referenced_columns=["id"],
            ),
            "users__orders",
            "'columns' or 'referenced_columns' are empty",
        ),
        (
            TTableReference(
                table="users__orders",
                referenced_table="users",
                columns=["user_id"],
                referenced_columns=["id"],
            ),
            "products",
            "is not connected",
        ),
    ],
    ids=[
        "missing-table",
        "missing-referenced-table",
        "empty-columns",
        "empty-referenced-columns",
        "columns-length-mismatch",
        "from-table-not-connected",
    ],
)
def test_to_join_ref_rejects_malformed(ref: TTableReference, from_table: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _to_join_ref(ref, from_table)


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
    "dataset_with_loads,left,right,expected_targets",
    [
        pytest.param("with_root_key", "users__orders", "users", ["users"], id="child-to-parent"),
        pytest.param(
            "with_root_key", "users", "users__orders", ["users__orders"], id="parent-to-child"
        ),
        pytest.param(
            "with_root_key",
            "users__orders__items",
            "users",
            ["users"],
            id="items-to-root-root-key",
        ),
        pytest.param(
            "without_root_key",
            "users__orders__items",
            "users",
            ["users__orders", "users"],
            id="items-to-root-parent-key",
        ),
        pytest.param(
            "with_root_key",
            "users",
            "users__orders__items",
            ["users__orders__items"],
            id="root-to-items-root-key",
        ),
        pytest.param(
            "without_root_key",
            "users",
            "users__orders__items",
            ["users__orders", "users__orders__items"],
            id="root-to-items-parent-key",
        ),
    ],
    indirect=["dataset_with_loads"],
)
def test_resolve_reference_chain_matrix(
    dataset_with_loads: TLoadsFixture,
    left: str,
    right: str,
    expected_targets: Sequence[str],
) -> None:
    dataset, _, _ = dataset_with_loads
    refs = _resolve_reference_chain(dataset.schema, left, right)

    assert [ref["target_table"] for ref in refs] == list(expected_targets)
    assert len(refs) == len(expected_targets)


def test_resolve_reference_chain_rejects_unrelated_tables(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match="Unable to resolve reference chain"):
        _resolve_reference_chain(dataset.schema, "products", "users__orders")


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
        pytest.param(
            lambda ds: ds.table("users__orders").limit(5).select("order_id"),
            "users",
            "no base table to resolve references",
            id="subquery-hides-base-table",
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


@pytest.mark.parametrize("kind", ["inner", "left", "right", "full"])
def test_join_accepts_kind_parameter(
    dataset_with_loads: TLoadsFixture,
    kind: TJoinType,
) -> None:
    dataset, _, _ = dataset_with_loads

    joined = dataset.table("users__orders").join("users", kind=kind)

    assert isinstance(joined, dlt.Relation)
    joins = joined.sqlglot_expression.args.get("joins") or []
    assert joins
    assert all(join.args.get("kind", "").lower() == kind for join in joins)


def test_join_projection_keeps_left_and_prefixes_explicit_target(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    joined = dataset.table("users__orders").join("users")

    selects = joined.sqlglot_expression.selects
    assert selects
    first = selects[0]
    assert isinstance(first, sge.Column)
    assert isinstance(first.args.get("this"), sge.Star)
    assert first.args["table"].name == "users__orders"

    expected_right_aliases = {
        f"users__{column_name}" for column_name in dataset.schema.tables["users"]["columns"].keys()
    }
    actual_right_aliases = {expr.output_name for expr in selects[1:]}
    assert actual_right_aliases == expected_right_aliases


@pytest.mark.parametrize("dataset_with_loads", ["without_root_key"], indirect=True)
def test_join_projection_excludes_intermediate_tables(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    joined = dataset.table("users__orders__items").join("users")

    appended_names = [expr.output_name for expr in joined.sqlglot_expression.selects[1:]]
    assert appended_names
    assert all(name.startswith("users__") for name in appended_names)
    assert not any(name.startswith("users__orders__") for name in appended_names)


def test_join_projection_alias_prefix_override(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    joined = dataset.table("users__orders").join("users", alias="u")

    expected_right_aliases = {
        f"u__{column_name}" for column_name in dataset.schema.tables["users"]["columns"].keys()
    }
    actual_right_aliases = {expr.output_name for expr in joined.sqlglot_expression.selects[1:]}
    assert actual_right_aliases == expected_right_aliases


def test_join_projection_prefix_allows_distinct_prefixes(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    joined = dataset.table("users__orders").join("users", alias="u")
    joined = joined.join("users__orders__items", alias="i")

    output_names = {expr.output_name for expr in joined.sqlglot_expression.selects}
    users_prefixed = {
        f"u__{column_name}" for column_name in dataset.schema.tables["users"]["columns"].keys()
    }
    items_prefixed = {
        f"i__{column_name}"
        for column_name in dataset.schema.tables["users__orders__items"]["columns"].keys()
    }

    assert users_prefixed.issubset(output_names)
    assert items_prefixed.issubset(output_names)


def test_join_projection_prefix_rejects_colliding_alias(
    dataset_with_loads: TLoadsFixture,
) -> None:
    dataset, _, _ = dataset_with_loads
    joined = dataset.table("users__orders").join("users", alias="shared")

    with pytest.raises(ValueError, match="conflict with existing columns"):
        joined.join("users__orders__items", alias="shared")


def test_join_rejects_empty_alias(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match="must be a non-empty string"):
        dataset.table("users__orders").join("users", alias="")


@pytest.mark.parametrize(
    "join_dataset,build_rel,other,expected_new_joins",
    [
        pytest.param(
            ("dataset_with_loads", "with_root_key"),
            lambda ds: ds.table("users__orders"),
            "users",
            [
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"qualifier": "users__orders", "column": "_dlt_parent_id"},
                            {"qualifier": "users", "column": "_dlt_id"},
                        )
                    ],
                },
            ],
            id="child-to-parent",
        ),
        pytest.param(
            ("dataset_with_loads", "with_root_key"),
            lambda ds: ds.table("users"),
            "users__orders",
            [
                {
                    "target_table": "users__orders",
                    "pairs": [
                        (
                            {"qualifier": "users", "column": "_dlt_id"},
                            {"qualifier": "users__orders", "column": "_dlt_parent_id"},
                        )
                    ],
                },
            ],
            id="parent-to-child",
        ),
        pytest.param(
            ("dataset_with_loads", "with_root_key"),
            lambda ds: ds.table("users__orders__items"),
            "users",
            [
                # root_key=True: single hop via _dlt_root_id
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"qualifier": "users__orders__items", "column": "_dlt_root_id"},
                            {"qualifier": "users", "column": "_dlt_id"},
                        )
                    ],
                },
            ],
            id="multi-hop-to-root",
        ),
        pytest.param(
            ("dataset_with_loads", "without_root_key"),
            lambda ds: ds.table("users__orders__items"),
            "users",
            [
                # root_key=False: must chain through users__orders
                # intermediate table gets generated alias "_dlt_int_t1"
                {
                    "target_table": "users__orders",
                    "pairs": [
                        (
                            {"qualifier": "users__orders__items", "column": "_dlt_parent_id"},
                            {"qualifier": "_dlt_int_t1", "column": "_dlt_id"},
                        )
                    ],
                },
                # final target keeps its own name as qualifier
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"qualifier": "_dlt_int_t1", "column": "_dlt_parent_id"},
                            {"qualifier": "users", "column": "_dlt_id"},
                        )
                    ],
                },
            ],
            id="multi-hop-to-root-parent-key",
        ),
        pytest.param(
            ("dataset_with_loads", "with_root_key"),
            lambda ds: ds.table("users__orders").join("users"),
            "users__orders__items",
            [
                # users already joined; items joins to users__orders (parent)
                {
                    "target_table": "users__orders__items",
                    "pairs": [
                        (
                            {"qualifier": "users__orders", "column": "_dlt_id"},
                            {"qualifier": "users__orders__items", "column": "_dlt_parent_id"},
                        )
                    ],
                },
            ],
            id="chain-with-existing-join",
        ),
        pytest.param(
            ("dataset_with_loads", "without_root_key"),
            lambda ds: ds.table("users__orders__items").join("users__orders"),
            "users",
            [
                # users__orders already joined; attach users via users__orders
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"qualifier": "users__orders", "column": "_dlt_parent_id"},
                            {"qualifier": "users", "column": "_dlt_id"},
                        )
                    ],
                },
            ],
            id="reuse-joined-alias",
        ),
        pytest.param(
            ("dataset_with_loads", "with_root_key"),
            lambda ds: ds.table("users__orders__items"),
            lambda ds: ds.table("users__orders").join("users"),
            [
                # other is a joined relation; target resolves to its base table
                # (users__orders), so the hop is items -> users__orders via parent key
                {
                    "target_table": "users__orders",
                    "pairs": [
                        (
                            {"qualifier": "users__orders__items", "column": "_dlt_parent_id"},
                            {"qualifier": "users__orders", "column": "_dlt_id"},
                        )
                    ],
                },
            ],
            id="joinable-graph-other",
        ),
        pytest.param(
            ("dataset_with_annotated_references", None),
            lambda ds: ds.table("user_sessions"),
            "users",
            [
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"qualifier": "user_sessions", "column": "user_id"},
                            {"qualifier": "users", "column": "id"},
                        )
                    ],
                }
            ],
            id="annotated-single-column-child-to-parent",
        ),
        pytest.param(
            ("dataset_with_annotated_references", None),
            lambda ds: ds.table("users"),
            "user_sessions",
            [
                {
                    "target_table": "user_sessions",
                    "pairs": [
                        (
                            {"qualifier": "users", "column": "id"},
                            {"qualifier": "user_sessions", "column": "user_id"},
                        )
                    ],
                }
            ],
            id="annotated-single-column-parent-to-child",
        ),
        pytest.param(
            ("dataset_with_annotated_references", None),
            lambda ds: ds.table("account_memberships"),
            "accounts",
            [
                {
                    "target_table": "accounts",
                    "pairs": [
                        (
                            {"qualifier": "account_memberships", "column": "account_id"},
                            {"qualifier": "accounts", "column": "account_id"},
                        ),
                        (
                            {"qualifier": "account_memberships", "column": "tenant_id"},
                            {"qualifier": "accounts", "column": "tenant_id"},
                        ),
                    ],
                }
            ],
            id="annotated-multi-column-child-to-parent",
        ),
        pytest.param(
            ("dataset_with_annotated_references", None),
            lambda ds: ds.table("accounts"),
            "account_memberships",
            [
                {
                    "target_table": "account_memberships",
                    "pairs": [
                        (
                            {"qualifier": "accounts", "column": "account_id"},
                            {"qualifier": "account_memberships", "column": "account_id"},
                        ),
                        (
                            {"qualifier": "accounts", "column": "tenant_id"},
                            {"qualifier": "account_memberships", "column": "tenant_id"},
                        ),
                    ],
                }
            ],
            id="annotated-multi-column-parent-to-child",
        ),
    ],
    indirect=["join_dataset"],
)
def test_magic_join_plan_matrix(
    join_dataset: dlt.Dataset,
    build_rel: Callable[[dlt.Dataset], dlt.Relation],
    other: Any,
    expected_new_joins: list[JoinExpectation],
) -> None:
    dataset = join_dataset
    rel = build_rel(dataset)
    target = other(dataset) if callable(other) else other
    existing_joins = rel.sqlglot_expression.args.get("joins") or []

    joined = rel.join(target)

    actual_joins = joined.sqlglot_expression.args.get("joins") or []
    new_joins = actual_joins[len(existing_joins) :]
    assert len(new_joins) == len(expected_new_joins)

    for actual, expected in zip(new_joins, expected_new_joins):
        assert actual.args.get("kind", "").lower() == "inner"
        assert isinstance(actual.this, sge.Table)
        assert actual.this.this.name == expected["target_table"]
        actual_pairs = _flatten_on_pairs(actual.args["on"])
        assert actual_pairs == expected["pairs"]


@pytest.mark.parametrize(
    "left,right,expected_rows,joined_name_column,expected_names",
    [
        pytest.param(
            "user_sessions",
            "users",
            3,
            "users__name",
            ["Alice", "Alice", "Bob"],
            id="annotated-single-column-e2e",
        ),
        pytest.param(
            "account_memberships",
            "accounts",
            3,
            "accounts__name",
            ["Acme", "Globex", "Initech"],
            id="annotated-multi-column-e2e",
        ),
    ],
)
def test_e2e_join_user_references_matrix(
    dataset_with_annotated_references: dlt.Dataset,
    left: str,
    right: str,
    expected_rows: int,
    joined_name_column: str,
    expected_names: list[str],
) -> None:
    df = dataset_with_annotated_references.table(left).join(right).order_by(joined_name_column).df()

    assert df is not None
    assert len(df) == expected_rows
    assert list(df[joined_name_column]) == expected_names


def test_join_rejoin_existing_target_is_idempotent(dataset_with_loads: TLoadsFixture) -> None:
    dataset, _, _ = dataset_with_loads
    rel = dataset.table("users__orders").join("users")
    sql_before = rel.sqlglot_expression.sql()

    rejoined = rel.join("users")
    sql_after = rejoined.sqlglot_expression.sql()

    assert sql_after == sql_before


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


def test_where_then_join_produces_correct_data(dataset_with_loads: TLoadsFixture) -> None:
    """Filtering rows before joining should preserve join correctness."""
    dataset, _, _ = dataset_with_loads

    # join without filter as baseline
    baseline_df = dataset.table("users__orders").join("users").df()
    assert baseline_df is not None

    # filter to a single order, then join
    rel = dataset.table("users__orders").where("order_id", "eq", 101)
    joined = rel.join("users")
    df = joined.df()

    assert df is not None
    assert len(df) == 1
    # verify the joined user column is present and correct
    assert "users__name" in df.columns
    assert df["users__name"].iloc[0] == "Alice"
    assert df["order_id"].iloc[0] == 101


def test_order_by_then_join_produces_correct_data(dataset_with_loads: TLoadsFixture) -> None:
    """order_by before join should preserve join correctness and ordering."""
    dataset, _, _ = dataset_with_loads

    # baseline: unordered join
    baseline_df = dataset.table("users__orders").join("users").df()
    assert baseline_df is not None

    # order then join
    rel = dataset.table("users__orders").order_by("order_id", "asc")
    joined = rel.join("users")
    df = joined.df()

    assert df is not None
    assert len(df) == len(baseline_df)
    # verify user data is attached correctly: each order has a matching user name
    for _, row in df.iterrows():
        assert row["users__name"] in ("Alice", "Bob", "Charlie")


def test_select_then_join_preserves_narrow_projection(dataset_with_loads: TLoadsFixture) -> None:
    """select() narrows the left projection but join columns resolve from the base table."""
    dataset, _, _ = dataset_with_loads
    rel = dataset.table("users__orders").select("order_id")
    joined = rel.join("users")
    df = joined.df()

    assert df is not None
    assert len(df) > 0
    # left side: only the selected column
    assert "order_id" in df.columns
    # join columns like _dlt_parent_id are NOT in the output (not selected)
    assert "_dlt_parent_id" not in df.columns
    # right side columns are present
    assert "users__name" in df.columns


@pytest.mark.parametrize(
    "build_joined",
    [
        pytest.param(
            lambda ds: ds.table("users__orders").join("users"),
            id="plain-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").where("order_id", "gt", 0).join("users"),
            id="where-then-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").order_by("order_id").join("users"),
            id="order-by-then-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").select("order_id").join("users"),
            id="select-then-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").limit(10).join("users"),
            id="limit-then-join",
        ),
        pytest.param(
            lambda ds: ds.table("users__orders").join("users").join("users__orders__items"),
            id="chain-join",
        ),
    ],
)
def test_columns_schema_matches_query_output(
    dataset_with_loads: TLoadsFixture,
    build_joined: Callable[[dlt.Dataset], dlt.Relation],
) -> None:
    """columns_schema must match the actual columns returned by executing the query."""
    dataset, _, _ = dataset_with_loads
    joined = build_joined(dataset)

    # columns_schema triggers compute_columns_schema -> qualify -> star expansion
    schema_cols = set(joined.columns_schema.keys())
    assert schema_cols, "columns_schema must not be empty"

    # execute and compare
    df = joined.df()
    assert df is not None
    df_cols = set(df.columns)

    assert schema_cols == df_cols, (
        "columns_schema keys don't match df columns.\n"
        f"  schema_only: {schema_cols - df_cols}\n"
        f"  df_only:     {df_cols - schema_cols}"
    )
