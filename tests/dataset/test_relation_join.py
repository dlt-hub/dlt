import tempfile
import pathlib
from typing import Any, Sequence, Callable, TypedDict, Optional, Union

import pytest
import sqlglot
import sqlglot.expressions as sge

import dlt
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.schema.typing import TTableReference
from dlt.dataset.exceptions import LineageFailedException
from dlt.dataset._join import (
    _build_join_condition_from_pairs,
    _resolve_reference_chain,
    _to_join_ref,
)
from dlt.dataset.relation import TJoinType
from tests.dataset.utils import TCrossDs3Fixture, TCrossDsFixture, TLoadsFixture


class _ColumnRef(TypedDict):
    """One side of a join ON equality: a table/column reference."""

    table: str
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


def _dataset_with_name_normalizer(dataset: dlt.Dataset, name_normalizer_ref: str) -> dlt.Dataset:
    schema = dataset.schema.clone()
    schema._normalizers_config["allow_identifier_change_on_table_with_data"] = True
    schema._normalizers_config["names"] = name_normalizer_ref
    schema.update_normalizers()
    return dlt.dataset(
        dataset_name=dataset.dataset_name,
        destination=dataset._destination_reference,
        schema=schema,
    )


def _flatten_on_pairs(
    expr: sge.Expression,
    query: Optional[sge.Query] = None,
) -> list[tuple[_ColumnRef, _ColumnRef]]:
    """Extract ``(left, right)`` column-ref pairs from a JOIN ON expression."""
    pairs: list[tuple[_ColumnRef, _ColumnRef]] = []
    qualifier_to_table: dict[str, str] = {}

    if query is not None:
        from_expr = query.args.get("from_") or query.args.get("from")
        if not isinstance(from_expr, sge.From):
            raise AssertionError(f"Expected FROM clause, got: {query}")

        tables = [from_expr.this, *((join.this) for join in query.args.get("joins") or [])]
        for table in tables:
            if not isinstance(table, sge.Table):
                raise AssertionError(f"Expected table expression, got: {table}")

            table_identifier = table.args.get("this")
            if isinstance(table_identifier, sge.Identifier):
                table_name = table_identifier.name
            elif isinstance(table_identifier, str):
                table_name = table_identifier
            else:
                raise AssertionError(f"Expected table identifier, got: {table}")

            alias_expr = table.args.get("alias")
            if isinstance(alias_expr, sge.TableAlias):
                alias_identifier = alias_expr.this
                if isinstance(alias_identifier, sge.Identifier):
                    qualifier = alias_identifier.name
                elif isinstance(alias_identifier, str):
                    qualifier = alias_identifier
                else:
                    qualifier = table_name
            else:
                qualifier = table_name

            qualifier_to_table[qualifier] = table_name

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
        left_qualifier = left.args["table"].name
        right_qualifier = right.args["table"].name
        pairs.append(
            (
                _ColumnRef(
                    table=qualifier_to_table.get(left_qualifier, left_qualifier),
                    column=left.args["this"].name,
                ),
                _ColumnRef(
                    table=qualifier_to_table.get(right_qualifier, right_qualifier),
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


@pytest.mark.parametrize(
    "left,right,match",
    [
        pytest.param("users", "users", "to itself", id="self-join"),
        pytest.param(
            "products", "users__orders", "Unable to resolve reference chain", id="unrelated-tables"
        ),
    ],
)
def test_resolve_reference_chain_rejection_matrix(
    dataset_with_loads: TLoadsFixture,
    left: str,
    right: str,
    match: str,
) -> None:
    dataset, _, _ = dataset_with_loads
    with pytest.raises(ValueError, match=match):
        _resolve_reference_chain(dataset.schema, left, right)


@pytest.mark.parametrize("dataset_with_loads", ["with_root_key"], indirect=True)
def test_join_rejects_different_physical_destination(dataset_with_loads: TLoadsFixture) -> None:
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

        rel = dataset.table("users")
        other_rel = other_dataset.table("other_data")

        with pytest.raises(ValueError, match="different physical destinations"):
            rel.join(other_rel, on="users._dlt_id = other_data._dlt_id")


def test_join_rejects_same_name_on_different_physical_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        shared_dataset_name = "same_name_diff_dest"

        pipeline_a = dlt.pipeline(
            pipeline_name="same_name_diff_dest_a",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(str(tmp_path / "a.duckdb")),
            dataset_name=shared_dataset_name,
        )
        pipeline_a.run([{"id": 1}], table_name="users")

        pipeline_b = dlt.pipeline(
            pipeline_name="same_name_diff_dest_b",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(str(tmp_path / "b.duckdb")),
            dataset_name=shared_dataset_name,
        )
        pipeline_b.run([{"oid": 10}], table_name="orders")

        ds_a = pipeline_a.dataset()
        ds_b = pipeline_b.dataset()
        assert ds_a.dataset_name == ds_b.dataset_name
        assert not ds_a.is_same_physical_destination(ds_b)

        with pytest.raises(ValueError, match="different physical destinations") as exc_info:
            ds_a.table("users").join(ds_b.table("orders"), on="users.id = orders.user_id")

        assert "a.duckdb" in str(exc_info.value)
        assert "b.duckdb" in str(exc_info.value)

        # once `can_read_from` is relaxed (e.g. duckdb ATTACH), the same-name guard must hold
        monkeypatch.setattr(
            DestinationClientConfiguration, "can_read_from", lambda self, other: True
        )
        with pytest.raises(ValueError, match="same name located on two different destinations"):
            ds_a.table("users").join(ds_b.table("orders"), on="users.id = orders.user_id")


@pytest.mark.parametrize(
    "make_destination",
    [
        pytest.param(lambda p: dlt.destinations.filesystem(str(p / "data")), id="filesystem"),
        pytest.param(
            lambda p: dlt.destinations.sqlalchemy(f"sqlite:///{p / 'shop.db'}"), id="sqlite"
        ),
    ],
)
def test_join_rejects_cross_dataset_on_unsupported_destination(
    make_destination: Callable[[pathlib.Path], Any],
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        destination = make_destination(tmp_path)

        pipeline_a = dlt.pipeline(
            pipeline_name="cross_ds_reject_a",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=destination,
            dataset_name="reject_crm",
        )
        pipeline_a.run([{"id": 1, "name": "Alice"}], table_name="users")

        pipeline_b = dlt.pipeline(
            pipeline_name="cross_ds_reject_b",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=destination,
            dataset_name="reject_inv",
        )
        pipeline_b.run([{"order_id": 10, "user_id": 1}], table_name="orders")

        ds_a = pipeline_a.dataset()
        ds_b = pipeline_b.dataset()
        assert ds_a.is_same_physical_destination(ds_b)

        with pytest.raises(ValueError, match="Cross-dataset joins are not supported"):
            ds_a.table("users").join(ds_b.table("orders"), on="users.id = orders.user_id")


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


@pytest.mark.parametrize(
    "build_rel,other,match",
    [
        pytest.param(
            lambda ds: ds.table("users"),
            "users",
            "to itself",
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
            "`other` must be a table name or a `dlt.Relation`, got `int`",
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
    assert first.args["table"].name

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


@pytest.mark.parametrize(
    "build_join",
    [
        pytest.param(lambda ds: ds.table("products").join("categories"), id="magic"),
        pytest.param(
            lambda ds: ds.table("products").join(
                "categories", on="products.category_id = categories.id"
            ),
            id="explicit-on",
        ),
    ],
)
def test_join_does_not_project_incomplete_target_columns(
    dataset_with_incomplete_join_target: dlt.Dataset,
    build_join: Callable[[dlt.Dataset], dlt.Relation],
) -> None:
    relation = build_join(dataset_with_incomplete_join_target)
    assert "categories__phantom_field" not in relation.columns_schema
    rows = relation.fetchall()
    assert rows is not None
    assert len(rows) == 3


@pytest.mark.parametrize(
    "build_rel,expected_session_ids",
    [
        pytest.param(
            lambda ds: ds.table("users").order_by("id").limit(1).join("user_sessions"),
            ["s1", "s2"],
            id="limit-magic",
        ),
        pytest.param(
            lambda ds: ds.table("users")
            .order_by("id")
            .limit(1)
            .join("user_sessions", on="users.id = user_sessions.user_id"),
            ["s1", "s2"],
            id="limit-explicit-on",
        ),
        pytest.param(
            lambda ds: ds.query("SELECT * FROM users ORDER BY id LIMIT 1 OFFSET 1").join(
                "user_sessions", on="users.id = user_sessions.user_id"
            ),
            ["s3"],
            id="limit-explicit-on-offset",
        ),
        pytest.param(
            lambda ds: ds.table("users").where("id", "eq", 1).join("user_sessions", kind="left"),
            ["s1", "s2"],
            id="filter-magic-left",
        ),
        pytest.param(
            lambda ds: ds.table("users").where("id", "eq", 1).join("user_sessions", kind="right"),
            ["s1", "s2", "s3"],
            id="filter-magic-right",
        ),
        pytest.param(
            lambda ds: ds.table("users").where("id", "eq", 1).join("user_sessions", kind="full"),
            ["s1", "s2", "s3"],
            id="filter-magic-full",
        ),
        pytest.param(
            lambda ds: ds.table("users")
            .where("id", "eq", 1)
            .join("user_sessions", on="users.id = user_sessions.user_id", kind="left"),
            ["s1", "s2"],
            id="filter-explicit-on-left",
        ),
        pytest.param(
            lambda ds: ds.table("users")
            .where("id", "eq", 1)
            .join("user_sessions", on="users.id = user_sessions.user_id", kind="right"),
            ["s1", "s2", "s3"],
            id="filter-explicit-on-right",
        ),
        pytest.param(
            lambda ds: ds.table("users")
            .where("id", "eq", 1)
            .join("user_sessions", on="users.id = user_sessions.user_id", kind="full"),
            ["s1", "s2", "s3"],
            id="filter-explicit-on-full",
        ),
    ],
)
def test_lhs_limit_and_filter_apply_before_join(
    dataset_with_annotated_references: dlt.Dataset,
    build_rel: Callable[[dlt.Dataset], dlt.Relation],
    expected_session_ids: list[str],
) -> None:
    """LIMIT and WHERE on the left relation must be applied before joining, not to the joined result."""
    df = build_rel(dataset_with_annotated_references).df()

    assert len(df) == len(expected_session_ids)
    assert sorted(df["user_sessions__session_id"]) == expected_session_ids


def test_windowed_lhs_join_applies_window_before_join(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    numbered = ds.query(
        "SELECT customer_id, name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM customers"
    )
    joined = numbered.join("orders", on="customers.customer_id = orders.customer_id")
    df = joined.order_by("orders__order_id").df()

    assert len(df) == 4
    assert [int(x) for x in df["rn"]] == [1, 1, 2, 3]


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
                            {"table": "users__orders", "column": "_dlt_parent_id"},
                            {"table": "users", "column": "_dlt_id"},
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
                            {"table": "users", "column": "_dlt_id"},
                            {"table": "users__orders", "column": "_dlt_parent_id"},
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
                            {"table": "users__orders__items", "column": "_dlt_root_id"},
                            {"table": "users", "column": "_dlt_id"},
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
                {
                    "target_table": "users__orders",
                    "pairs": [
                        (
                            {"table": "users__orders__items", "column": "_dlt_parent_id"},
                            {"table": "users__orders", "column": "_dlt_id"},
                        )
                    ],
                },
                {
                    "target_table": "users",
                    "pairs": [
                        (
                            {"table": "users__orders", "column": "_dlt_parent_id"},
                            {"table": "users", "column": "_dlt_id"},
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
                            {"table": "users__orders", "column": "_dlt_id"},
                            {"table": "users__orders__items", "column": "_dlt_parent_id"},
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
                            {"table": "users__orders", "column": "_dlt_parent_id"},
                            {"table": "users", "column": "_dlt_id"},
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
                            {"table": "users__orders__items", "column": "_dlt_parent_id"},
                            {"table": "users__orders", "column": "_dlt_id"},
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
                            {"table": "user_sessions", "column": "user_id"},
                            {"table": "users", "column": "id"},
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
                            {"table": "users", "column": "id"},
                            {"table": "user_sessions", "column": "user_id"},
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
                            {"table": "account_memberships", "column": "account_id"},
                            {"table": "accounts", "column": "account_id"},
                        ),
                        (
                            {"table": "account_memberships", "column": "tenant_id"},
                            {"table": "accounts", "column": "tenant_id"},
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
                            {"table": "accounts", "column": "account_id"},
                            {"table": "account_memberships", "column": "account_id"},
                        ),
                        (
                            {"table": "accounts", "column": "tenant_id"},
                            {"table": "account_memberships", "column": "tenant_id"},
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
        actual_pairs = _flatten_on_pairs(actual.args["on"], joined.sqlglot_expression)
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


def test_limit_and_select_then_magic_join(dataset_with_loads: TLoadsFixture) -> None:
    """LIMIT and select() on the left relation apply before the magic join."""
    dataset, _, _ = dataset_with_loads
    limited = dataset.table("users__orders").order_by("order_id").limit(1)

    joined = limited.select("order_id", "_dlt_parent_id").join("users")
    df = joined.df()
    assert len(df) == 1
    assert "order_id" in df.columns
    assert "users__name" in df.columns

    # a projection that drops the join key cannot be joined once LIMIT seals it
    with pytest.raises(LineageFailedException, match="_dlt_parent_id"):
        limited.select("order_id").join("users").df()


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


@pytest.mark.parametrize(
    "name_normalizer_ref",
    (
        "tests.common.cases.normalizers.title_case",
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.snake_no_x",
    ),
)
@pytest.mark.parametrize(
    "left,right",
    [
        ("users__orders", "users"),
        ("users__orders__items", "users"),
    ],
)
def test_join_columns_schema_resolves_with_name_mutating_normalizer(
    dataset_with_loads: TLoadsFixture,
    name_normalizer_ref: str,
    left: str,
    right: str,
) -> None:
    dataset, _, _ = dataset_with_loads
    normalized_dataset = _dataset_with_name_normalizer(dataset, name_normalizer_ref)
    normalized_left = normalized_dataset.schema.naming.normalize_tables_path(left)
    normalized_right = normalized_dataset.schema.naming.normalize_tables_path(right)

    joined = normalized_dataset.table(normalized_left).join(normalized_right)
    schema_cols = set(joined.columns_schema.keys())

    assert schema_cols
    expected_right_aliases = {
        f"{normalized_right}__{column_name}"
        for column_name in normalized_dataset.schema.tables[normalized_right]["columns"].keys()
    }
    assert expected_right_aliases.issubset(schema_cols)


@pytest.mark.parametrize(
    "build_join",
    [
        pytest.param(lambda ds: ds.table("users__orders").join("users"), id="magic"),
        pytest.param(
            lambda ds: ds.table("users__orders").join(
                "users", on="users__orders._dlt_parent_id = users._dlt_id"
            ),
            id="explicit-on",
        ),
    ],
)
def test_join_resolves_physical_dataset_name(
    build_join: Callable[[dlt.Dataset], dlt.Relation],
) -> None:
    """Joins must bind to the physical dataset name when normalization mutates the raw name."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        pipeline = dlt.pipeline(
            pipeline_name="raw_dataset_name",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(str(tmp_path / "raw_name.duckdb")),
            dataset_name="GitHubData",
        )
        pipeline.run(
            [
                {"id": 1, "name": "Alice", "orders": [{"order_id": 101}, {"order_id": 102}]},
                {"id": 2, "name": "Bob", "orders": [{"order_id": 103}]},
            ],
            table_name="users",
        )

        dataset = pipeline.dataset()
        assert dataset.dataset_name == "GitHubData"
        assert dataset.sql_client.dataset_name == "git_hub_data"
        assert len(dataset.table("users").fetchall()) == 2

        df = build_join(dataset).df()
        assert len(df) == 3
        assert sorted(df["users__name"]) == ["Alice", "Alice", "Bob"]


@pytest.mark.parametrize(
    "build_join,expected_names",
    [
        pytest.param(
            lambda ds_crm, ds_mkt: ds_crm.table("orders").join(
                ds_mkt.table("users"), on="orders.user_id = users.id"
            ),
            ["Ann", "Ben"],
            id="base-table-rhs",
        ),
        pytest.param(
            lambda ds_crm, ds_mkt: ds_crm.table("orders").join(
                ds_mkt.table("users").where("segment", "eq", "pro"),
                on="orders.user_id = users.id",
            ),
            ["Ann"],
            id="transformed-rhs",
        ),
    ],
)
def test_cross_dataset_join_resolves_physical_dataset_names(
    build_join: Callable[[dlt.Dataset, dlt.Dataset], dlt.Relation],
    expected_names: list[str],
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        db_path = str(tmp_path / "physical_cross.duckdb")

        pipeline_crm = dlt.pipeline(
            pipeline_name="physical_cross_ds_a",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="CrmData",
        )
        pipeline_crm.run(
            [{"order_id": 1, "user_id": 1}, {"order_id": 2, "user_id": 2}],
            table_name="orders",
        )

        pipeline_mkt = dlt.pipeline(
            pipeline_name="physical_cross_ds_b",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="MktData",
        )
        pipeline_mkt.run(
            [
                {"id": 1, "name": "Ann", "segment": "pro"},
                {"id": 2, "name": "Ben", "segment": "free"},
            ],
            table_name="users",
        )

        ds_crm = pipeline_crm.dataset()
        ds_mkt = pipeline_mkt.dataset()
        assert ds_crm.sql_client.dataset_name != ds_crm.dataset_name
        assert ds_mkt.sql_client.dataset_name != ds_mkt.dataset_name
        assert len(ds_crm.table("orders").fetchall()) == 2
        assert len(ds_mkt.table("users").fetchall()) == 2

        joined = build_join(ds_crm, ds_mkt)
        df = joined.order_by("order_id").df()
        assert list(df["users__name"]) == expected_names

        sql = joined.to_sql()
        assert f'"{ds_crm.sql_client.dataset_name}"."orders"' in sql, sql
        assert f'"{ds_mkt.sql_client.dataset_name}"."users"' in sql, sql


@pytest.mark.parametrize(
    "build_join",
    [
        pytest.param(
            lambda ds: ds.table("customers").join(
                "orders", on="customers.customer_id = orders.customer_id"
            ),
            id="bare-table-name",
        ),
        pytest.param(
            lambda ds: ds.table("customers").join(
                f"{ds.dataset_name}.orders", on="customers.customer_id = orders.customer_id"
            ),
            id="dataset-qualified-string",
        ),
        pytest.param(
            lambda ds: ds.table("customers").join(
                "orders",
                on=sge.EQ(
                    this=sge.Column(
                        table=sge.to_identifier("customers"),
                        this=sge.to_identifier("customer_id"),
                    ),
                    expression=sge.Column(
                        table=sge.to_identifier("orders"),
                        this=sge.to_identifier("customer_id"),
                    ),
                ),
            ),
            id="sqlglot-expression-on",
        ),
    ],
)
def test_explicit_on_joins_local_table(
    dataset_with_relational_tables: dlt.Dataset,
    build_join: Callable[[dlt.Dataset], dlt.Relation],
) -> None:
    ds = dataset_with_relational_tables
    joined = build_join(ds)
    assert not joined._foreign_schemas
    df = joined.df()
    assert len(df) == 4
    assert "orders__amount" in df.columns
    assert list(df["orders__amount"]) == [50.0, 75.0, 200.0, 30.0]


def test_explicit_on_non_eq_predicate(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    joined = ds.table("customers").join(
        "orders",
        on="customers.customer_id = orders.customer_id AND orders.amount > 50",
    )
    df = joined.df()
    assert len(df) == 2
    assert list(df["orders__amount"]) == [75.0, 200.0]


def test_explicit_on_composite_key(
    dataset_with_annotated_references: dlt.Dataset,
) -> None:
    ds = dataset_with_annotated_references
    joined = ds.table("account_memberships").join(
        "accounts",
        on=(
            "account_memberships.account_id = accounts.account_id "
            "AND account_memberships.tenant_id = accounts.tenant_id"
        ),
    )
    df = joined.order_by("accounts__name").df()

    assert len(df) == 3
    assert list(df["accounts__name"]) == ["Acme", "Globex", "Initech"]


def test_explicit_on_left_join_keeps_unmatched_left_rows(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    joined = ds.table("countries").join(
        "customers", kind="left", on="countries.code = customers.country_code"
    )
    df = joined.order_by("code").df()
    assert len(df) == 4
    assert list(df["code"]) == ["DE", "DE", "ES", "FR"]
    assert list(df["customers__name"]) == ["Alice", "Charlie", None, "Bob"]
    es_row = df[df["code"] == "ES"].iloc[0]
    assert es_row["name"] == "Spain"
    customers_cols = [c for c in df.columns if c.startswith("customers__")]
    assert es_row[customers_cols].isna().all()


def test_explicit_on_projection_prefix(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    joined = ds.table("customers").join(
        "orders", on="customers.customer_id = orders.customer_id", alias="o"
    )
    selects = joined.sqlglot_expression.selects
    right_aliases = {expr.output_name for expr in selects if expr.output_name.startswith("o__")}
    assert right_aliases
    expected = {f"o__{col}" for col in ds.schema.tables["orders"]["columns"].keys()}
    assert right_aliases == expected


@pytest.mark.parametrize("alias", [None, "o"], ids=["default-prefix", "custom-alias"])
def test_order_by_join_output_alias_survives_select(
    dataset_with_relational_tables: dlt.Dataset,
    alias: Optional[str],
) -> None:
    """Ordering by a join output alias survives a later projection change."""
    prefix = alias or "orders"
    rel = (
        dataset_with_relational_tables.table("customers")
        .join("orders", on="customers.customer_id = orders.customer_id", alias=alias)
        .order_by(f"{prefix}__order_id", "desc")
        .select("name", f"{prefix}__amount")
    )
    df = rel.df()
    assert list(df.columns) == ["name", f"{prefix}__amount"]
    assert [float(x) for x in df[f"{prefix}__amount"]] == [30.0, 200.0, 75.0, 50.0]


def test_order_by_join_output_binds_to_source_column(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    """tsql cannot resolve a select alias inside an ORDER BY expression (NULLS emulation)."""
    rel = (
        dataset_with_relational_tables.table("customers")
        .join("orders", on="customers.customer_id = orders.customer_id")
        .order_by("orders__order_id")
    )
    sql = rel.to_sql()
    assert 'ORDER BY "orders"."order_id"' in sql
    order_by = sqlglot.transpile(sql, read="duckdb", write="tsql")[0].split("ORDER BY", 1)[1]
    assert "[orders__order_id]" not in order_by
    assert "[orders].[order_id]" in order_by


def test_explicit_on_projection_alias_collision_rejected(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    left = ds.query("SELECT customer_id, 1 AS orders__amount FROM customers")

    with pytest.raises(ValueError, match="conflict with existing columns"):
        left.join("orders", on="customers.customer_id = orders.customer_id")


@pytest.mark.parametrize(
    "build_join,expected_rows,expected_names,expected_amounts",
    [
        pytest.param(
            lambda ds: ds.table("customers")
            .where("country_code", "eq", "DE")
            .join("orders", on="customers.customer_id = orders.customer_id"),
            3,
            ["Alice", "Alice", "Charlie"],
            [50.0, 75.0, 30.0],
            id="filtered-lhs",
        ),
        pytest.param(
            lambda ds: ds.table("customers").join(
                ds.table("orders").where("amount", "gt", 50.0),
                on="customers.customer_id = orders.customer_id",
            ),
            2,
            ["Alice", "Bob"],
            [75.0, 200.0],
            id="filtered-rhs",
        ),
    ],
)
def test_explicit_on_with_filtered_side(
    dataset_with_relational_tables: dlt.Dataset,
    build_join: Callable[[dlt.Dataset], dlt.Relation],
    expected_rows: int,
    expected_names: list[str],
    expected_amounts: list[float],
) -> None:
    ds = dataset_with_relational_tables
    df = build_join(ds).df()
    assert len(df) == expected_rows
    assert list(df["name"]) == expected_names
    assert list(df["orders__amount"]) == expected_amounts


def test_explicit_on_does_not_mutate_transformed_rhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    expensive_orders = ds.table("orders").where("amount", "gt", 50.0)
    rhs_sql_before = expensive_orders.to_sql()
    assert expensive_orders.sqlglot_expression.parent is None

    joined = ds.table("customers").join(
        expensive_orders, on="customers.customer_id = orders.customer_id"
    )

    # the join leaves the RHS relation untouched
    assert expensive_orders.sqlglot_expression.parent is None
    assert expensive_orders.to_sql() == rhs_sql_before

    joined_again = ds.table("customers").join(
        expensive_orders, on="customers.customer_id = orders.customer_id", alias="o2"
    )
    assert list(joined.df()["orders__amount"]) == [75.0, 200.0]
    assert list(joined_again.df()["o2__amount"]) == [75.0, 200.0]


def test_explicit_on_with_projected_lhs_preserves_left_projection(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    narrow_customers = ds.table("customers").select("customer_id", "name")
    joined = narrow_customers.join("orders", on="customers.customer_id = orders.customer_id")
    df = joined.df()
    assert len(df) == 4
    lhs_cols = {c for c in df.columns if not c.startswith("orders__")}
    assert lhs_cols == {"customer_id", "name"}
    assert "country_code" not in df.columns
    assert "orders__amount" in df.columns
    assert list(df["orders__amount"]) == [50.0, 75.0, 200.0, 30.0]


def test_explicit_on_with_projected_rhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    narrow_orders = ds.table("orders").select("order_id", "customer_id")
    joined = ds.table("customers").join(
        narrow_orders, on="customers.customer_id = orders.customer_id"
    )
    df = joined.df()
    assert len(df) == 4
    rhs_cols = {c for c in df.columns if c.startswith("orders__")}
    assert rhs_cols == {"orders__order_id", "orders__customer_id"}
    assert "orders__amount" not in df.columns


def test_explicit_on_with_aggregated_rhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    order_totals = ds.query(
        "SELECT customer_id, SUM(amount) AS total_amount FROM orders GROUP BY customer_id"
    )

    joined = ds.table("customers").join(
        order_totals,
        on="customers.customer_id = orders.customer_id",
        alias="order_totals",
    )
    df = joined.order_by("customer_id").df()

    assert len(df) == 3
    assert list(df["customer_id"]) == [1, 2, 3]
    assert list(df["name"]) == ["Alice", "Bob", "Charlie"]
    assert "order_totals__total_amount" in df.columns
    assert list(df["order_totals__total_amount"]) == [125.0, 200.0, 30.0]
    assert "order_totals__amount" not in df.columns


@pytest.mark.parametrize(
    "lhs_query,expected_ids,expected_totals,expected_names",
    [
        pytest.param(
            "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
            [1, 2, 3],
            [125.0, 200.0, 30.0],
            ["Alice", "Bob", "Charlie"],
            id="group-by",
        ),
        pytest.param(
            "SELECT customer_id, SUM(amount) AS total FROM orders "
            "GROUP BY customer_id HAVING SUM(amount) > 40",
            [1, 2],
            [125.0, 200.0],
            ["Alice", "Bob"],
            id="group-by-having",
        ),
        pytest.param(
            "SELECT customer_id, amount AS total FROM orders ORDER BY total",
            [3, 1, 1, 2],
            [30.0, 50.0, 75.0, 200.0],
            ["Charlie", "Alice", "Alice", "Bob"],
            id="flat-order-by-alias",
        ),
        pytest.param(
            # `customer_id AS cid` aliases a column that also exists in the joined `customers`
            "SELECT customer_id, customer_id AS cid, amount AS total FROM orders ORDER BY total",
            [3, 1, 1, 2],
            [30.0, 50.0, 75.0, 200.0],
            ["Charlie", "Alice", "Alice", "Bob"],
            id="flat-alias-shadows-rhs-column",
        ),
    ],
)
def test_explicit_on_with_query_lhs(
    dataset_with_relational_tables: dlt.Dataset,
    lhs_query: str,
    expected_ids: list[int],
    expected_totals: list[float],
    expected_names: list[str],
) -> None:
    ds = dataset_with_relational_tables
    query_lhs = ds.query(lhs_query)
    joined = query_lhs.join("customers", on="orders.customer_id = customers.customer_id").order_by(
        "customer_id"
    )
    df = joined.df()

    assert list(df["customer_id"]) == expected_ids
    assert [float(x) for x in df["total"]] == expected_totals
    assert list(df["customers__name"]) == expected_names
    assert "amount" not in df.columns


def test_explicit_on_chains_after_wrapped_lhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    """A second join composes onto a left side already embedded as a derived table."""
    ds = dataset_with_relational_tables
    order_totals = ds.query(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    )
    joined = order_totals.join("customers", on="orders.customer_id = customers.customer_id").join(
        "countries", on="customers.country_code = countries.code"
    )
    df = joined.order_by("customer_id").df()

    assert len(df) == 3
    assert [float(x) for x in df["total"]] == [125.0, 200.0, 30.0]
    assert list(df["customers__name"]) == ["Alice", "Bob", "Charlie"]
    assert list(df["countries__name"]) == ["Germany", "France", "Germany"]


def test_explicit_on_with_distinct_lhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    distinct_codes = ds.query("SELECT DISTINCT country_code FROM customers")
    joined = distinct_codes.join("countries", on="customers.country_code = countries.code")

    outer = joined.sqlglot_expression
    assert outer.args.get("distinct") is None
    derived = outer.find(sge.Subquery)
    assert derived is not None and derived.this.args.get("distinct") is not None

    df = joined.order_by("country_code").df()
    assert list(df["country_code"]) == ["DE", "FR"]
    assert list(df["countries__name"]) == ["Germany", "France"]


def test_explicit_on_with_aliased_query_relations(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    customers = ds.query("SELECT * FROM customers AS c")
    orders = ds.query("SELECT * FROM orders AS o")

    joined = customers.join(orders, on="c.customer_id = o.customer_id")
    df = joined.order_by("o__order_id").df()

    assert len(df) == 4
    assert list(df["customer_id"]) == [1, 1, 2, 3]
    assert list(df["name"]) == ["Alice", "Alice", "Bob", "Charlie"]
    assert list(df["o__amount"]) == [50.0, 75.0, 200.0, 30.0]


def test_explicit_on_with_constant_rhs_uses_subquery_fallback_qualifier(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    const = ds.query(
        "SELECT 1 AS customer_id, 'x' AS tag"
    )  # no FROM clause, no qualifier, falls back to subquery
    joined = ds.table("customers").join(const, on="customers.customer_id = subquery.customer_id")

    assert "subquery__tag" in joined.columns_schema
    df = joined.df()
    assert len(df) == 1
    assert list(df["name"]) == ["Alice"]
    assert list(df["subquery__tag"]) == ["x"]


def test_explicit_on_rejects_empty_alias(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    with pytest.raises(ValueError, match="must be a non-empty string"):
        ds.table("customers").join(
            "orders", on="customers.customer_id = orders.customer_id", alias=""
        )


@pytest.mark.parametrize(
    "build_join,expected_error",
    [
        pytest.param(
            lambda ds: ds.table("employees").join(
                "employees", on="employees.manager_id = employees.id", alias="mgr"
            ),
            "already names a source",
            id="direct-base-self-join-rejected",
        ),
        pytest.param(
            lambda ds: ds.query("SELECT * FROM employees AS e1").join(
                "employees", on="e1.manager_id = employees.id", alias="mgr"
            ),
            None,
            id="aliased-query-lhs-base-rhs",
        ),
        pytest.param(
            lambda ds: ds.query("SELECT * FROM employees AS e1").join(
                ds.query("SELECT * FROM employees AS e2"),
                on="e1.manager_id = e2.id",
                alias="mgr",
            ),
            None,
            id="both-aliased-query",
        ),
        pytest.param(
            lambda ds: ds.table("employees").join(
                ds.query("SELECT * FROM employees AS mgr"),
                on="employees.manager_id = mgr.id",
            ),
            None,
            id="base-lhs-aliased-query-rhs",
        ),
    ],
)
def test_self_join_requires_distinct_qualifiers(
    dataset_with_relational_tables: dlt.Dataset,
    build_join: Callable[[dlt.Dataset], dlt.Relation],
    expected_error: Optional[str],
) -> None:
    ds = dataset_with_relational_tables
    if expected_error is not None:
        with pytest.raises(ValueError, match=expected_error):
            build_join(ds)
        return

    df = build_join(ds).df()
    assert sorted(df["name"]) == ["Bob", "Carol"]
    assert sorted(df["mgr__name"]) == ["Alice", "Alice"]


@pytest.mark.parametrize(
    "on,match",
    [
        pytest.param("", "non-empty SQL expression", id="empty"),
        pytest.param("   ", "non-empty SQL expression", id="whitespace"),
        pytest.param("customers.id = (((", "Cannot parse `on`", id="unparsable"),
        pytest.param("SELECT 1", "must be an SQL boolean expression", id="select-string"),
        pytest.param(
            sqlglot.select("1"), "must be an SQL boolean expression", id="select-expression"
        ),
    ],
)
def test_explicit_on_rejects_invalid_on_expression(
    dataset_with_relational_tables: dlt.Dataset,
    on: Union[str, sge.Expression],
    match: str,
) -> None:
    ds = dataset_with_relational_tables
    with pytest.raises(ValueError, match=match):
        ds.table("customers").join("orders", on=on)


@pytest.mark.parametrize(
    "build_join,match",
    [
        pytest.param(
            lambda ds: ds.table("customers").join(
                "orders", kind="outer", on="customers.customer_id = orders.customer_id"
            ),
            "kind=outer",
            id="unknown-kind",
        ),
        pytest.param(
            lambda ds: ds.table("customers").join(
                "unknown_ds.orders", on="customers.customer_id = orders.customer_id"
            ),
            "is not registered",
            id="unknown-dotted-dataset",
        ),
        pytest.param(
            lambda ds: ds.table("customers").join("orders"),
            "Unable to resolve reference chain",
            id="no-on-unresolvable",
        ),
    ],
)
def test_explicit_on_rejection_matrix(
    dataset_with_relational_tables: dlt.Dataset,
    build_join: Callable[[dlt.Dataset], dlt.Relation],
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        build_join(dataset_with_relational_tables)


def test_explicit_on_with_derived_table_lhs(
    dataset_with_relational_tables: dlt.Dataset,
) -> None:
    ds = dataset_with_relational_tables
    derived = ds.query("SELECT * FROM (SELECT * FROM customers) AS sub")
    joined = derived.join("orders", on="sub.customer_id = orders.customer_id")
    df = joined.order_by("orders__order_id").df()

    assert list(df["name"]) == ["Alice", "Alice", "Bob", "Charlie"]
    assert [float(x) for x in df["orders__amount"]] == [50.0, 75.0, 200.0, 30.0]

    # an unaliased derived table exposes no qualifier for `on` to reference
    unaliased = ds.query("SELECT * FROM (SELECT * FROM customers)")
    with pytest.raises(ValueError, match="named source"):
        unaliased.join("orders", on="customers.customer_id = orders.customer_id")


@pytest.mark.parametrize(
    "name_normalizer_ref",
    (
        "tests.common.cases.normalizers.title_case",
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.snake_no_x",
    ),
)
def test_explicit_on_columns_schema_resolves_with_name_mutating_normalizer(
    dataset_with_relational_tables: dlt.Dataset,
    name_normalizer_ref: str,
) -> None:
    normalized_dataset = _dataset_with_name_normalizer(
        dataset_with_relational_tables, name_normalizer_ref
    )
    naming = normalized_dataset.schema.naming
    customers = naming.normalize_tables_path("customers")
    orders = naming.normalize_tables_path("orders")
    customer_id = naming.normalize_identifier("customer_id")

    on_predicate = f'"{customers}"."{customer_id}" = "{orders}"."{customer_id}"'
    joined = normalized_dataset.table(customers).join(orders, on=on_predicate)

    schema_cols = set(joined.columns_schema.keys())
    assert schema_cols
    expected_right_aliases = {
        f"{orders}__{column_name}"
        for column_name in normalized_dataset.schema.tables[orders]["columns"].keys()
    }
    assert expected_right_aliases.issubset(schema_cols)


@pytest.mark.parametrize(
    "on",
    [
        pytest.param("users.id = purchases.user_id", id="string"),
        pytest.param(
            sge.EQ(
                this=sge.Column(table=sge.to_identifier("users"), this=sge.to_identifier("id")),
                expression=sge.Column(
                    table=sge.to_identifier("purchases"), this=sge.to_identifier("user_id")
                ),
            ),
            id="sqlglot-expression",
        ),
    ],
)
def test_cross_dataset_join(
    cross_dataset_duckdb: TCrossDsFixture,
    on: Union[str, sge.Expression],
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb
    users = ds_crm.table("users")

    joined = users.join(ds_inv.table("purchases"), on=on)

    assert ds_inv.dataset_name in joined._foreign_schemas
    assert ds_inv.dataset_name not in users._foreign_schemas
    assert len(joined._foreign_schemas[ds_inv.dataset_name]) >= 1

    df = joined.df()
    assert len(df) == 3
    assert "purchases__sku" in df.columns
    assert "purchases__quantity" in df.columns
    assert sorted(df["purchases__sku"]) == ["G-001", "W-001", "W-001"]


def test_cross_dataset_join_requires_on(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb
    users = ds_crm.table("users")
    purchases = ds_inv.table("purchases")

    with pytest.raises(ValueError, match="`on` is required"):
        users.join(purchases)


_MATCHED = {
    "purchases__purchase_id": [1, 2, 3],
    "purchases__user_id": [1, 1, 2],
    "purchases__sku": ["W-001", "G-001", "W-001"],
    "purchases__quantity": [2, 1, 1],
    "name": ["Alice", "Alice", "Bob"],
}
_MATCHED_PLUS_ORPHAN = {
    "purchases__purchase_id": [1, 2, 3, 4],
    "purchases__user_id": [1, 1, 2, 99],
    "purchases__sku": ["W-001", "G-001", "W-001", "D-001"],
    "purchases__quantity": [2, 1, 1, 5],
    "name": ["Alice", "Alice", "Bob", None],  # orphan's matched user name is NULL
}


@pytest.mark.parametrize(
    "kind,expected",
    [
        pytest.param("inner", _MATCHED, id="inner"),
        pytest.param("left", _MATCHED, id="left"),
        pytest.param("right", _MATCHED_PLUS_ORPHAN, id="right"),
        pytest.param("full", _MATCHED_PLUS_ORPHAN, id="full"),
    ],
)
def test_cross_dataset_join_kind_parameter(
    cross_dataset_duckdb: TCrossDsFixture,
    kind: TJoinType,
    expected: dict[str, list[Any]],
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb
    users = ds_crm.table("users")
    purchases = ds_inv.table("purchases")

    joined = users.join(purchases, on="users.id = purchases.user_id", kind=kind)
    df = joined.df()

    for col, expected_values in expected.items():
        assert list(df[col]) == expected_values, f"column `{col}` mismatch"


def test_cross_dataset_join_to_sql_uses_each_dataset_name(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    joined = ds_crm.table("users").join(
        ds_inv.table("purchases"),
        on="users.id = purchases.user_id",
    )
    sql = joined.to_sql()

    assert f'"{ds_crm.dataset_name}"."users"' in sql
    assert f'"{ds_inv.dataset_name}"."purchases"' in sql
    assert f'"{ds_inv.dataset_name}"."users"' not in sql
    assert f'"{ds_crm.dataset_name}"."purchases"' not in sql


def test_cross_dataset_join_with_transformed_rhs_preserves_foreign_dataset_binding(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb
    users = ds_crm.table("users")
    filtered_purchases = ds_inv.table("purchases").where("quantity", "gt", 1)

    joined = users.join(filtered_purchases, on="users.id = purchases.user_id").order_by("id")
    df = joined.df()

    assert len(df) == 1
    assert list(df["name"]) == ["Alice"]
    assert list(df["purchases__purchase_id"]) == [1]
    assert list(df["purchases__sku"]) == ["W-001"]
    assert list(df["purchases__quantity"]) == [2]


def test_cross_dataset_join_with_aggregated_rhs(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    purchase_totals = ds_inv.query(
        "SELECT user_id, SUM(quantity) AS total_quantity FROM purchases GROUP BY user_id"
    )
    joined = ds_crm.table("users").join(
        purchase_totals,
        on="users.id = purchases.user_id",
        alias="purchase_totals",
    )
    df = joined.order_by("id").df()

    assert len(df) == 2
    assert list(df["id"]) == [1, 2]
    assert list(df["name"]) == ["Alice", "Bob"]
    assert "purchase_totals__total_quantity" in df.columns
    assert [int(x) for x in df["purchase_totals__total_quantity"]] == [3, 1]
    assert "purchase_totals__quantity" not in df.columns


def test_cross_dataset_join_with_cte_qualifies_body_but_not_alias(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    recent_purchases = ds_inv.query(
        "WITH recent AS (SELECT * FROM purchases WHERE quantity > 1) SELECT * FROM recent"
    )
    joined = ds_crm.table("users").join(recent_purchases, on="users.id = recent.user_id")

    table_qualifiers = {
        (node.name, node.db or None) for node in joined.sqlglot_expression.find_all(sge.Table)
    }
    assert ("users", ds_crm.dataset_name) in table_qualifiers
    assert ("purchases", ds_inv.dataset_name) in table_qualifiers
    assert ("recent", None) in table_qualifiers

    df = joined.order_by("id").df()
    assert len(df) == 1
    assert list(df["name"]) == ["Alice"]
    assert list(df["recent__purchase_id"]) == [1]
    assert list(df["recent__sku"]) == ["W-001"]
    assert list(df["recent__quantity"]) == [2]


def test_cross_dataset_join_with_same_table_names_keeps_sources_unambiguous(
    same_named_cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_marketing = same_named_cross_dataset_duckdb
    crm_users = ds_crm.query("SELECT * FROM users AS crm_users")
    marketing_users = ds_marketing.table("users")

    joined = crm_users.join(marketing_users, on="crm_users.id = users.id", alias="marketing")
    df = joined.order_by("id").df()

    assert len(df) == 2
    assert list(df["id"]) == [1, 2]
    assert list(df["name"]) == ["Alice", "Bob"]
    assert list(df["marketing__segment"]) == ["pro", "free"]


def test_cross_dataset_same_named_join_rejects_colliding_target(
    same_named_cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_marketing = same_named_cross_dataset_duckdb
    with pytest.raises(ValueError, match="already names a source"):
        ds_crm.table("users").join(
            ds_marketing.table("users"),
            on="users.id = users.id",
            alias="marketing",
        )


@pytest.mark.parametrize(
    "build_chain,order_column,expected,absent_columns",
    [
        pytest.param(
            lambda ds_crm, ds_inv, ds_billing: ds_inv.table("purchases")
            .join(ds_crm.table("users"), on="purchases.user_id = users.id")
            .join("inventory_items", on="purchases.sku = inventory_items.sku")
            .join("warehouses", on="inventory_items.warehouse_id = warehouses.warehouse_id"),
            "purchase_id",
            {
                "purchase_id": [1, 2, 3],
                "users__name": ["Alice", "Alice", "Bob"],
                "inventory_items__quantity": [50, 30, 50],
                "warehouses__city": ["Berlin", "Paris", "Berlin"],
            },
            [],
            id="local-string-hops-star-schema",
        ),
        pytest.param(
            lambda ds_crm, ds_inv, ds_billing: ds_crm.table("users")
            .join(ds_inv.table("purchases"), on="users.id = purchases.user_id")
            .join(ds_inv.table("inventory_items"), on="purchases.sku = inventory_items.sku"),
            "purchases__purchase_id",
            {
                "purchases__purchase_id": [1, 2, 3],
                "name": ["Alice", "Alice", "Bob"],
                "purchases__sku": ["W-001", "G-001", "W-001"],
                "inventory_items__quantity": [50, 30, 50],
            },
            [],
            id="foreign-relation-hop",
        ),
        pytest.param(
            lambda ds_crm, ds_inv, ds_billing: ds_inv.table("purchases")
            .join(ds_crm.table("users"), on="purchases.user_id = users.id")
            .join(ds_billing.table("subscriptions"), on="users.id = subscriptions.user_id"),
            "purchase_id",
            {
                "users__name": ["Alice", "Alice", "Bob"],
                "subscriptions__plan": ["enterprise", "enterprise", "free"],
            },
            ["u__name"],
            id="three-datasets-default-prefix",
        ),
        pytest.param(
            lambda ds_crm, ds_inv, ds_billing: ds_inv.table("purchases")
            .join(ds_crm.table("users"), on="purchases.user_id = users.id", alias="u")
            .join(ds_billing.table("subscriptions"), on="users.id = subscriptions.user_id"),
            "purchase_id",
            {
                "u__name": ["Alice", "Alice", "Bob"],
                "subscriptions__plan": ["enterprise", "enterprise", "free"],
            },
            ["users__name"],
            id="three-datasets-custom-alias",
        ),
    ],
)
def test_cross_dataset_join_chain_matrix(
    three_way_cross_dataset_duckdb: TCrossDs3Fixture,
    build_chain: Callable[[dlt.Dataset, dlt.Dataset, dlt.Dataset], dlt.Relation],
    order_column: str,
    expected: dict[str, list[Any]],
    absent_columns: list[str],
) -> None:
    """Join chains across datasets; each case adds one composition dimension."""
    ds_crm, ds_inv, ds_billing = three_way_cross_dataset_duckdb
    df = build_chain(ds_crm, ds_inv, ds_billing).order_by(order_column).df()

    assert len(df) == len(next(iter(expected.values())))
    for col, values in expected.items():
        assert list(df[col]) == values, f"column `{col}` mismatch"
    for col in absent_columns:
        assert col not in df.columns


def test_cross_dataset_join_chain_magic_then_cross(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    joined = (
        ds_crm.table("users__orders")
        .join("users")
        .join(ds_inv.table("purchases"), on="users.id = purchases.user_id")
    )
    df = joined.df()

    assert len(df) == 5
    assert "order_id" in df.columns  # base, unprefixed
    assert "users__name" in df.columns
    assert "purchases__sku" in df.columns
    assert sorted(df["users__name"]) == ["Alice", "Alice", "Alice", "Alice", "Bob"]
    assert list(df["users__id"]) == list(df["purchases__user_id"])


def test_cross_dataset_join_chain_magic_then_two_crossings(
    three_way_cross_dataset_duckdb: TCrossDs3Fixture,
) -> None:
    ds_crm, ds_inv, ds_billing = three_way_cross_dataset_duckdb

    joined = (
        ds_crm.table("users__orders")
        .join("users")
        .join(ds_inv.table("purchases"), on="users.id = purchases.user_id")
        .join(ds_billing.table("subscriptions"), on="users.id = subscriptions.user_id")
    )
    df = joined.df()

    assert len(df) == 5
    assert "order_id" in df.columns
    assert "users__name" in df.columns
    assert "purchases__sku" in df.columns
    assert "subscriptions__plan" in df.columns
    assert sorted(df["users__name"]) == ["Alice", "Alice", "Alice", "Alice", "Bob"]
    assert sorted(df["subscriptions__plan"]) == [
        "enterprise",
        "enterprise",
        "enterprise",
        "enterprise",
        "free",
    ]
    assert list(df["users__id"]) == list(df["purchases__user_id"])
    assert list(df["users__id"]) == list(df["subscriptions__user_id"])


def test_cross_dataset_join_via_dotted_string_qualifies_foreign_dataset(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    joined = ds_crm.table("users").join(
        ds_inv.table("purchases"), on="users.id = purchases.user_id"
    )
    assert ds_inv.dataset_name in joined._foreign_schemas

    chained = joined.join(
        f"{ds_inv.dataset_name}.inventory_items",
        on="purchases.sku = inventory_items.sku",
    )
    sql = chained.to_sql()

    assert f'"{ds_inv.dataset_name}"."inventory_items"' in sql, sql
    assert f'"{ds_crm.dataset_name}"."inventory_items"' not in sql, sql

    df = chained.order_by("purchases__purchase_id").df()
    assert list(df["purchases__purchase_id"]) == [1, 2, 3]
    assert list(df["name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["purchases__sku"]) == ["W-001", "G-001", "W-001"]
    assert list(df["inventory_items__quantity"]) == [50, 30, 50]


def test_cross_dataset_join_chain_with_filtered_step(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    alice_purchases = ds_inv.table("purchases").where("user_id", "eq", 1)
    joined = alice_purchases.join(ds_crm.table("users"), on="purchases.user_id = users.id").join(
        "inventory_items", on="purchases.sku = inventory_items.sku"
    )
    df = joined.order_by("purchase_id").df()

    assert len(df) == 2
    assert list(df["purchase_id"]) == [1, 2]
    assert list(df["users__name"]) == ["Alice", "Alice"]
    assert list(df["inventory_items__quantity"]) == [50, 30]


def test_cross_dataset_join_chain_does_not_mutate_sources(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    purchases = ds_inv.table("purchases")
    users = ds_crm.table("users")
    inventory_items = ds_inv.table("inventory_items")

    purchases_sql = purchases.to_sql()
    users_sql = users.to_sql()
    inventory_items_sql = inventory_items.to_sql()

    step1 = purchases.join(users, on="purchases.user_id = users.id")
    step1_sql = step1.to_sql()

    assert purchases.to_sql() == purchases_sql
    assert users.to_sql() == users_sql
    assert inventory_items.to_sql() == inventory_items_sql
    assert step1.to_sql() == step1_sql
    # check if rebuild of the first step is identical
    assert purchases.join(users, on="purchases.user_id = users.id").to_sql() == step1_sql


def test_cross_dataset_join_chain_columns_schema_matches_df(
    three_way_cross_dataset_duckdb: TCrossDs3Fixture,
) -> None:
    ds_crm, ds_inv, ds_billing = three_way_cross_dataset_duckdb

    joined = (
        ds_inv.table("purchases")
        .join(ds_crm.table("users"), on="purchases.user_id = users.id")
        .join(ds_billing.table("subscriptions"), on="users.id = subscriptions.user_id")
    )

    schema_cols = set(joined.columns_schema.keys())
    assert schema_cols, "columns_schema must not be empty"

    df = joined.df()
    df_cols = set(df.columns)

    assert schema_cols == df_cols


def test_cross_dataset_join_chain_filter_on_later_colliding_column(
    cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    ds_crm, ds_inv = cross_dataset_duckdb

    high_value = ds_inv.table("purchases").where("quantity", "gt", 1)
    joined = high_value.join(ds_crm.table("users"), on="purchases.user_id = users.id").join(
        "inventory_items", on="purchases.sku = inventory_items.sku"
    )

    df = joined.order_by("purchase_id").df()
    assert len(df) == 1
    assert list(df["users__name"]) == ["Alice"]
    assert list(df["inventory_items__quantity"]) == [50]


def test_cross_dataset_chain_same_named_tables_disambiguated(
    same_named_cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    """CRM and marketing both expose a `users` table."""
    ds_crm, ds_marketing = same_named_cross_dataset_duckdb

    marketing = ds_marketing.query("SELECT * FROM users AS mkt_users")
    joined = (
        ds_crm.table("users__orders")
        .join("users")
        .join(marketing, on="users.id = mkt_users.id", alias="marketing")
    )
    df = joined.order_by("order_id").df()

    assert len(df) == 3
    assert "users__name" in df.columns
    assert "marketing__segment" in df.columns
    assert list(df["users__name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["marketing__segment"]) == ["pro", "pro", "free"]


@pytest.mark.parametrize(
    "build_local_join,local_table,check_column,expected_values",
    [
        pytest.param(
            lambda ds, rel: rel.join("users", on="orders.user_id = users.id"),
            "users",
            "users__name",
            ["Alice", "Bob"],
            id="bare-table-name",
        ),
        pytest.param(
            lambda ds, rel: rel.join(f"{ds.dataset_name}.users", on="orders.user_id = users.id"),
            "users",
            "users__name",
            ["Alice", "Bob"],
            id="dataset-qualified-string",
        ),
        pytest.param(
            lambda ds, rel: rel.join(
                ds.query("SELECT * FROM users AS u"), on="orders.user_id = u.id"
            ),
            "users",
            "u__name",
            ["Alice", "Bob"],
            id="aliased-local-query",
        ),
        pytest.param(
            lambda ds, rel: rel.join("_dlt_loads", on="orders._dlt_load_id = _dlt_loads.load_id"),
            "_dlt_loads",
            "_dlt_loads__status",
            [0, 0],
            id="dlt-loads-system-table",
        ),
    ],
)
def test_cross_dataset_join_then_local_join_to_same_named_table(
    build_local_join: Callable[[dlt.Dataset, dlt.Relation], dlt.Relation],
    local_table: str,
    check_column: str,
    expected_values: list[Any],
) -> None:
    """A local join target shadowed by a same-named foreign table must bind to the local dataset."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        db_path = str(tmp_path / "shadowed.duckdb")

        pipeline_crm = dlt.pipeline(
            pipeline_name="shadowed_local_target_a",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="crm_data",
        )
        pipeline_crm.run(
            [{"order_id": 1, "user_id": 1}, {"order_id": 2, "user_id": 2}],
            table_name="orders",
        )
        pipeline_crm.run(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            table_name="users",
        )

        pipeline_mkt = dlt.pipeline(
            pipeline_name="shadowed_local_target_b",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="mkt_data",
        )
        pipeline_mkt.run(
            [{"id": 1, "segment": "pro"}, {"id": 2, "segment": "free"}],
            table_name="users",
        )

        ds_crm = pipeline_crm.dataset()
        ds_mkt = pipeline_mkt.dataset()

        foreign_joined = ds_crm.table("orders").join(
            ds_mkt.query("SELECT * FROM users AS mkt_users"),
            on="orders.user_id = mkt_users.id",
            alias="marketing",
        )
        joined = build_local_join(ds_crm, foreign_joined)

        sql = joined.to_sql()
        assert f'"{ds_crm.dataset_name}"."{local_table}"' in sql, sql

        df = joined.order_by("order_id").df()
        assert list(df[check_column]) == expected_values
        assert list(df["marketing__segment"]) == ["pro", "free"]


def test_magic_join_after_cross_dataset_resolves_local_target(
    same_named_cross_dataset_duckdb: TCrossDsFixture,
) -> None:
    """A magic join target shadowed by a same-named foreign table must bind to the local dataset."""
    ds_crm, ds_marketing = same_named_cross_dataset_duckdb
    marketing = ds_marketing.query("SELECT * FROM users AS mkt_users")

    joined = (
        ds_crm.table("users__orders")
        .join(marketing, on="mkt_users.id = 1", alias="marketing", kind="left")
        .join("users")
    )
    df = joined.order_by("order_id").df()

    assert len(df) == 3
    assert list(df["users__name"]) == ["Alice", "Alice", "Bob"]
    assert list(df["marketing__segment"]) == ["pro", "pro", "pro"]


def test_magic_join_after_foreign_base_table_resolves_local_target() -> None:
    """A magic join target shadowed by a same-named foreign base table must bind to the local dataset."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        db_path = str(tmp_path / "shadowed_base.duckdb")

        pipeline_crm = dlt.pipeline(
            pipeline_name="shadowed_base_target_a",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="crm_data",
        )
        pipeline_crm.run(
            [
                {"id": 1, "name": "Alice", "orders": [{"order_id": 101}, {"order_id": 102}]},
                {"id": 2, "name": "Bob", "orders": [{"order_id": 103}]},
            ],
            table_name="users",
        )

        pipeline_mkt = dlt.pipeline(
            pipeline_name="shadowed_base_target_b",
            pipelines_dir=str(tmp_path / "pipelines_dir"),
            destination=dlt.destinations.duckdb(db_path),
            dataset_name="marketing_data",
        )
        # `name` overlaps with the local users table so the shadowing stays silent
        pipeline_mkt.run(
            [
                {"id": 1, "segment": "pro", "name": "MKT-A"},
                {"id": 2, "segment": "free", "name": "MKT-B"},
            ],
            table_name="users",
        )

        ds_crm = pipeline_crm.dataset()
        ds_mkt = pipeline_mkt.dataset()

        joined = (
            ds_crm.table("users__orders")
            .join(ds_mkt.table("users"), on="users.id = 1", alias="marketing", kind="left")
            .join("users")
        )
        df = joined.order_by("order_id").df()

        assert len(df) == 3
        assert list(df["users__name"]) == ["Alice", "Alice", "Bob"]
        assert list(df["marketing__segment"]) == ["pro", "pro", "pro"]
        # the local magic target must enter the query alongside the foreign table
        sql = joined.to_sql()
        assert f'"{ds_crm.dataset_name}"."users"' in sql, sql
