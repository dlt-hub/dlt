from __future__ import annotations

import pathlib
import warnings
from typing import Any, Iterator, Literal

import pytest
from sqlglot import expressions as sge

import dlt
from dlt.common.pendulum import pendulum
from dlt.extract.incremental.transform import ModelIncremental


EVENTS_LOAD_0 = [
    {"id": 1, "created_at": "2026-01-01T00:00:00+00:00", "value": 1.0},
    {"id": 2, "created_at": "2026-01-05T00:00:00+00:00", "value": 2.0},
    {"id": 3, "created_at": "2026-01-10T00:00:00+00:00", "value": 3.0},
]
EVENTS_LOAD_1 = [
    {"id": 4, "created_at": "2026-01-15T00:00:00+00:00", "value": 4.0},
    {"id": 5, "created_at": "2026-01-20T00:00:00+00:00", "value": 5.0},
]

END_VALUE_DT = pendulum.datetime(2999, 1, 1, tz="UTC")
END_VALUE_ID = 10**12


@pytest.fixture(scope="module")
def incremental_pipeline(module_tmp_path: pathlib.Path) -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="relation_incremental",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "incremental.db")),
        dev_mode=True,
    )

    @dlt.resource(name="events", primary_key="id", write_disposition="append")
    def events(batch: int) -> Iterator[Any]:
        if batch == 0:
            yield EVENTS_LOAD_0
        else:
            yield EVENTS_LOAD_1

    pipeline.run(events(batch=0))
    pipeline.run(events(batch=1))
    return pipeline


@pytest.fixture(scope="module")
def incremental_dataset(incremental_pipeline: dlt.Pipeline) -> dlt.Dataset:
    return incremental_pipeline.dataset()


@pytest.fixture(scope="module")
def dataset_with_incomplete_join_target(module_tmp_path: pathlib.Path) -> dlt.Dataset:
    """Two sibling tables joined by an explicit reference, where the join target
    declares an incomplete column hint via `columns=`.

    `phantom_field` is declared on `categories` with no `data_type`, so it never
    materializes at the destination. `Schema.get_table_columns()` filters it out
    via `is_complete_column`; raw `schema.tables[...]["columns"]` does not.
    """
    pipeline = dlt.pipeline(
        pipeline_name="relation_incremental_incomplete",
        pipelines_dir=str(module_tmp_path / "pipelines_dir_incomplete"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "incomplete.db")),
        dev_mode=True,
    )

    @dlt.resource(
        name="categories",
        primary_key="id",
        columns=[{"name": "phantom_field", "nullable": True}],
    )
    def categories() -> Iterator[Any]:
        yield [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

    @dlt.resource(
        name="products",
        primary_key="id",
        columns=[{"name": "category_id", "data_type": "bigint"}],
        references=[{
            "referenced_table": "categories",
            "columns": ["category_id"],
            "referenced_columns": ["id"],
        }],
    )
    def products() -> Iterator[Any]:
        yield [
            {"id": 10, "category_id": 1},
            {"id": 11, "category_id": 2},
            {"id": 12, "category_id": 1},
        ]

    pipeline.run([categories(), products()])
    return pipeline.dataset()


def _where(relation: dlt.Relation) -> sge.Expression:
    where_node = relation.sqlglot_expression.args.get("where")
    assert isinstance(where_node, sge.Where), f"Expected WHERE clause, got {where_node!r}"
    return where_node.this


def _column_name(expr: sge.Expression) -> str:
    assert isinstance(expr, sge.Column), f"Expected Column, got {expr!r}"
    return expr.args["this"].name


def _column_table(expr: sge.Expression) -> str | None:
    assert isinstance(expr, sge.Column), f"Expected Column, got {expr!r}"
    table = expr.args.get("table")
    return table.name if table is not None else None


def _join_target_names(relation: dlt.Relation) -> list[str]:
    joins = relation.sqlglot_expression.args.get("joins") or []
    names: list[str] = []
    for join in joins:
        target = join.this
        assert isinstance(target, sge.Table)
        names.append(target.this.name)
    return names


def test_incremental_emits_where_on_simple_cursor(incremental_dataset: dlt.Dataset) -> None:
    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    relation = incremental_dataset.table("events").incremental(incremental)

    condition = _where(relation)
    assert isinstance(condition, sge.And)
    bound_pair = condition.this
    assert isinstance(bound_pair, sge.And)
    assert isinstance(bound_pair.this, sge.GTE)
    assert _column_name(bound_pair.this.this) == "id"
    # no join is added for a simple cursor path
    assert (relation.sqlglot_expression.args.get("joins") or []) == []


def test_incremental_sets_is_incremental_flag(incremental_dataset: dlt.Dataset) -> None:
    base = incremental_dataset.table("events")
    assert base.is_incremental is False

    incremental = dlt.sources.incremental("id", initial_value=1, end_value=END_VALUE_ID)
    flagged = base.incremental(incremental)
    assert flagged.is_incremental is True

    # flag survives further chaining, context propagates through copies
    chained = flagged.select("id", "value").where("value", "gt", 0)
    assert chained.is_incremental is True

    # a plain where() never sets the flag
    assert base.where("id", "gt", 1).is_incremental is False


def test_incremental_kwarg_on_table_equivalent_to_method(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)

    via_kwarg = incremental_dataset.table(
        "events", incremental=incremental
    ).sqlglot_expression.sql()
    via_method = (
        incremental_dataset.table("events").incremental(incremental).sqlglot_expression.sql()
    )

    assert via_kwarg == via_method


def test_incremental_returns_new_relation(incremental_dataset: dlt.Dataset) -> None:
    base = incremental_dataset.table("events")
    sql_before = base.sqlglot_expression.sql()

    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    filtered = base.incremental(incremental)

    assert filtered is not base
    assert base.sqlglot_expression.sql() == sql_before
    assert filtered.sqlglot_expression.sql() != sql_before


@pytest.mark.parametrize(
    "last_value_func,range_start,range_end,expected_start_cls,expected_end_cls",
    [
        pytest.param("max", "closed", "open", sge.GTE, sge.LT, id="max-closed-open-default"),
        pytest.param("max", "open", "closed", sge.GT, sge.LTE, id="max-open-closed"),
        pytest.param("min", "closed", "open", sge.LTE, sge.GT, id="min-closed-open"),
        pytest.param("min", "open", "closed", sge.LT, sge.GTE, id="min-open-closed"),
    ],
)
def test_incremental_operators_matrix(
    incremental_dataset: dlt.Dataset,
    last_value_func: Literal["min", "max"],
    range_start: Literal["open", "closed"],
    range_end: Literal["open", "closed"],
    expected_start_cls: type,
    expected_end_cls: type,
) -> None:
    incremental = dlt.sources.incremental(
        "id",
        initial_value=2,
        end_value=4,
        last_value_func=last_value_func,
        range_start=range_start,
        range_end=range_end,
    )
    relation = incremental_dataset.table("events").incremental(incremental)

    condition = _where(relation)
    assert isinstance(condition, sge.And)
    bound_pair = condition.this
    assert isinstance(bound_pair, sge.And)
    start_op = bound_pair.this
    end_op = bound_pair.expression
    assert isinstance(start_op, expected_start_cls)
    assert isinstance(end_op, expected_end_cls)
    assert isinstance(start_op, sge.Binary) and isinstance(end_op, sge.Binary)
    assert _column_name(start_op.this) == "id"
    assert _column_name(end_op.this) == "id"


def test_incremental_datetime_cursor_renders_as_sql_literal(
    incremental_dataset: dlt.Dataset,
) -> None:
    ts = pendulum.datetime(2026, 1, 5, tz="UTC")
    incremental = dlt.sources.incremental("created_at", initial_value=ts, end_value=END_VALUE_DT)
    # `created_at` is nullable, below silence "raise" warning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        relation = incremental_dataset.table("events").incremental(incremental)

    sql = relation.sqlglot_expression.sql(dialect=incremental_dataset.destination_dialect)
    assert "2026-01-05" in sql
    assert "DateTime(" not in sql
    assert "datetime.datetime" not in sql


def test_incremental_dotted_cursor_auto_joins_target(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    # _dlt_loads.inserted_at is `nullable=False` in the system schema, so the
    # default "raise" policy stays silent here — no warnings.catch_warnings needed
    relation = incremental_dataset.table("events").incremental(incremental)

    # exactly one JOIN added, targeting _dlt_loads
    assert _join_target_names(relation) == ["_dlt_loads"]

    # bound pair is wrapped with AND IS NOT NULL by the default "raise" policy
    condition = _where(relation)
    assert isinstance(condition, sge.And)
    bound_pair = condition.this
    assert isinstance(bound_pair, sge.And)
    start_op = bound_pair.this
    assert isinstance(start_op, sge.Binary)
    # WHERE column is qualified to the joined table
    assert _column_name(start_op.this) == "inserted_at"
    assert _column_table(start_op.this) == "_dlt_loads"


def test_incremental_dotted_cursor_does_not_pollute_projection(
    incremental_dataset: dlt.Dataset,
) -> None:
    # end-only: valid unbound mode, last_value is None -> single LT condition,
    # enough to trigger the auto-join without needing a start bound.
    incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at", end_value=END_VALUE_DT
    )
    relation = incremental_dataset.table("events").incremental(incremental)

    # no column from _dlt_loads appears in the SELECT list — the auto-join
    # is filter-only (project=False path).
    selects = relation.sqlglot_expression.selects
    output_names = [expr.output_name for expr in selects]
    assert not any(name.startswith("_dlt_loads__") for name in output_names)


def test_incremental_dotted_cursor_runtime_columns_base_only(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    relation = incremental_dataset.table("events").incremental(incremental)

    expected_columns = set(incremental_dataset.table("events").columns)
    assert set(relation.columns) == expected_columns
    assert not any(c.startswith("_dlt_loads__") for c in relation.columns)

    row = relation.fetchone()
    assert row is not None
    assert len(row) == len(relation.columns)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Bug: `Relation.incremental` dotted-cursor branch reads target columns via raw"
        " `schema.tables[name]['columns']`, accepting incomplete column hints (no"
        " `data_type`) as cursors. The resulting WHERE references a column that"
        " doesn't exist on the destination — materialization fails at lineage."
        " Fix: source target columns via `Schema.get_table_columns(table_name)` and"
        " reject `.incremental()` on cursors that aren't materialized."
    ),
)
def test_incremental_dotted_cursor_rejects_incomplete_target_column(
    dataset_with_incomplete_join_target: dlt.Dataset,
) -> None:
    """An incomplete (declared but unmaterialized) cursor column must not produce
    a relation that emits SQL referencing a non-existent column. Materializing
    the relation against duckdb is the source of truth.
    """
    incremental = dlt.sources.incremental(
        "categories.phantom_field",
        initial_value=0,
        end_value=10**12,
        on_cursor_value_missing="exclude",
    )
    relation = dataset_with_incomplete_join_target.table("products").incremental(incremental)
    # Hard failure today: lineage rejects "Unknown column: phantom_field" because
    # the SQLGlot schema filters incomplete columns but the WHERE built by
    # `.incremental()` does not — they disagree, lineage raises.
    relation.fetchall()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Bug: `_apply_join_projection` reads `schema.tables[target]['columns']` raw"
        " and aliases every column into the SELECT — including incomplete columns"
        " (no `data_type`) that don't exist on the destination. Fix: source columns"
        " via `Schema.get_table_columns(target)` so incomplete hints are filtered"
        " out of the projection."
    ),
)
def test_join_does_not_project_incomplete_target_columns(
    dataset_with_incomplete_join_target: dlt.Dataset,
) -> None:
    """`relation.join(other)` must not emit projection aliases for columns that
    are declared as hints but were never materialized. Materializing the join is
    the source of truth: today it raises `LineageFailedException` because the
    projected `categories__phantom_field` has no underlying column.
    """
    relation = dataset_with_incomplete_join_target.table("products").join("categories")
    rows = relation.fetchall()
    assert rows is not None
    # 3 products inner-joined to 2 categories on category_id → 3 rows
    assert len(rows) == 3


def test_incremental_dotted_cursor_reuses_existing_join(
    incremental_dataset: dlt.Dataset,
) -> None:
    """An explicit .join() before .incremental() on the same target should
    not be duplicated — the WHERE latches onto the existing qualifier.
    """
    pre_joined = incremental_dataset.table("events").join("_dlt_loads")
    existing_targets = _join_target_names(pre_joined)
    assert existing_targets.count("_dlt_loads") == 1

    incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at", end_value=END_VALUE_DT
    )
    relation = pre_joined.incremental(incremental)

    assert _join_target_names(relation).count("_dlt_loads") == 1


def test_incremental_aggregate_on_simple_cursor(incremental_dataset: dlt.Dataset) -> None:
    """`_incremental_aggregate_relation` returns the MAX cursor over the filter."""
    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    relation = incremental_dataset.table("events").incremental(incremental)
    # max id across EVENTS_LOAD_0 + EVENTS_LOAD_1 with id >= 2 is 5
    assert relation._incremental_aggregate_relation().fetchscalar() == 5


def test_incremental_aggregate_on_dotted_cursor(incremental_dataset: dlt.Dataset) -> None:
    incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    relation = incremental_dataset.table("events").incremental(incremental)
    # exact value depends on load timing, but a MAX of inserted_at should be non-null
    agg_value = relation._incremental_aggregate_relation().fetchscalar()
    assert agg_value is not None


def test_incremental_aggregate_returns_none_when_not_incremental(
    incremental_dataset: dlt.Dataset,
) -> None:
    not_incremental = incremental_dataset.table("events")
    assert not_incremental._incremental_aggregate_relation() is None


def test_incremental_aggregate_honors_min(incremental_dataset: dlt.Dataset) -> None:
    """`last_value_func=min` flips the aggregate to SQL `MIN`."""
    # for min: closed start -> `<=`, closed end -> `>=`. Window [0, 5] contains ids 1-5.
    incremental = dlt.sources.incremental(
        "id",
        initial_value=5,
        end_value=0,
        last_value_func="min",
        range_end="closed",
    )
    relation = incremental_dataset.table("events").incremental(incremental)
    assert relation._incremental_aggregate_relation().fetchscalar() == 1


def test_incremental_aggregate_on_query_with_group_by(incremental_dataset: dlt.Dataset) -> None:
    incremental = dlt.sources.incremental(
        "day",
        initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    sql = (
        "SELECT CAST(date_trunc('day', created_at) AS TIMESTAMP WITH TIME ZONE) AS day,"
        " COUNT(*) AS total FROM events GROUP BY day"
    )
    relation = incremental_dataset(sql).incremental(incremental)
    assert relation._incremental_aggregate_relation().fetchscalar() == pendulum.datetime(
        2026, 1, 20, tz="UTC"
    )


def test_incremental_aggregate_on_query_relation_bare_cursor(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    relation = incremental_dataset("SELECT id, value FROM events WHERE value > 0").incremental(
        incremental
    )
    assert relation._incremental_aggregate_relation().fetchscalar() == 5


def test_incremental_aggregate_preserves_distinct(incremental_dataset: dlt.Dataset) -> None:
    incremental = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    relation = incremental_dataset("SELECT DISTINCT id FROM events").incremental(incremental)
    assert relation._incremental_aggregate_relation().fetchscalar() == 5


def test_incremental_aggregate_branches_on_cursor_qualifier(
    incremental_dataset: dlt.Dataset,
) -> None:
    bare = dlt.sources.incremental("id", initial_value=0, end_value=END_VALUE_ID)
    bare_rel = incremental_dataset.table("events").incremental(bare)
    bare_agg = bare_rel._incremental_aggregate_relation().sqlglot_expression
    bare_inner_subq = bare_agg.args["from_"].this
    assert isinstance(bare_inner_subq, sge.Subquery)
    bare_inner_select = bare_inner_subq.this
    bare_inner_from = bare_inner_select.args["from_"].this
    assert isinstance(
        bare_inner_from, sge.Subquery
    ), "Bare cursor: base query must be wrapped as a subquery"

    dotted = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    dotted_rel = incremental_dataset.table("events").incremental(dotted)
    dotted_agg = dotted_rel._incremental_aggregate_relation().sqlglot_expression
    dotted_inner_subq = dotted_agg.args["from_"].this
    assert isinstance(dotted_inner_subq, sge.Subquery)
    dotted_inner_select = dotted_inner_subq.this
    dotted_inner_from = dotted_inner_select.args["from_"].this
    assert isinstance(
        dotted_inner_from, sge.Table
    ), "Qualified cursor: inline-projection path must keep the base table in FROM"
    assert dotted_inner_select.args.get(
        "joins"
    ), "Qualified cursor: JOIN must be preserved so the qualifier still resolves"


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(lambda r: r.limit(2), id="limit-only"),
        pytest.param(lambda r: r.order_by("id", "desc"), id="order-by-only"),
        pytest.param(lambda r: r.order_by("id").limit(2), id="order-by-limit"),
    ],
)
def test_incremental_aggregate_rejects_limit_or_order_by_in_stateful_mode(
    incremental_pipeline: dlt.Pipeline, shape: Any
) -> None:
    # In stateful mode (no end_value), LIMIT/ORDER BY would advance state past
    # only the returned rows. Rejected so callers can't silently skip rows.
    # Empty yield -> no rows pass the pipe step -> state never advances, so a
    # fixed resource name is safe to reuse across params.
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_reject")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=0, range_start="open"
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = shape(dataset.table("events").incremental(cursor))
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    with pytest.raises(ValueError, match="LIMIT and ORDER BY aren't supported"):
        captured._incremental_aggregate_relation()


def test_incremental_inside_resource_captures_bound_sql(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_simple_cursor")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental("id", initial_value=2),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    condition = _where(captured)
    assert isinstance(condition, sge.And)
    start_op = condition.this
    assert isinstance(start_op, sge.GTE)
    assert _column_name(start_op.this) == "id"


def test_incremental_custom_last_value_func_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    """Only `min` and `max` can be pushed down to SQL; custom callables can't."""
    incremental = dlt.sources.incremental("id", initial_value=1, last_value_func=lambda xs: max(xs))
    with pytest.raises(ValueError, match="last_value_func"):
        incremental_dataset.table("events").incremental(incremental)


def test_incremental_unknown_dotted_target_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental("not_a_table.ts", initial_value=1)
    with pytest.raises(ValueError, match="not found in dataset schema"):
        incremental_dataset.table("events").incremental(incremental)


def test_incremental_dotted_cursor_on_query_relation_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    """Dotted cursors need a base-table relation to resolve the join chain."""
    query_relation = incremental_dataset.query("SELECT * FROM events")
    incremental = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    with pytest.raises(ValueError, match="no base table"):
        query_relation.incremental(incremental)


def test_incremental_chained_call_raises(incremental_dataset: dlt.Dataset) -> None:
    incremental_a = dlt.sources.incremental("id", initial_value=1, end_value=END_VALUE_ID)
    incremental_b = dlt.sources.incremental("value", initial_value=0.0, end_value=10.0)

    relation = incremental_dataset.table("events").incremental(incremental_a)
    with pytest.raises(ValueError, match="already been applied"):
        relation.incremental(incremental_b)


@pytest.mark.parametrize(
    "build_relation",
    [
        pytest.param(
            lambda ds, load_ids, incremental: ds.table(
                "events", load_ids=load_ids, incremental=incremental
            ),
            id="kwargs",
        ),
        pytest.param(
            lambda ds, load_ids, incremental: ds.table("events")
            .from_loads(load_ids)
            .incremental(incremental),
            id="chained",
        ),
    ],
)
def test_incremental_dotted_cursor_after_from_loads_raises(
    incremental_pipeline: dlt.Pipeline, build_relation: Any
) -> None:
    """`.from_loads()` wraps FROM in a subquery, so a subsequent dotted-cursor
    `.incremental()` cannot resolve the join. Both the kwargs combo on
    `dataset.table()` and the chained form must fail with a clear, user-facing
    message rather than the internal `_discover_join_params` error.
    """
    dataset = incremental_pipeline.dataset()
    load_ids = dataset.load_ids()
    assert load_ids, "fixture must produce at least one load"

    incremental = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    with pytest.raises(ValueError, match="dotted cursor cannot be applied"):
        build_relation(dataset, load_ids, incremental)


@pytest.mark.parametrize(
    "cursor_path",
    [
        pytest.param("$.items[*].name", id="jsonpath-wildcard"),
        pytest.param("$.name", id="jsonpath-root"),
        pytest.param("items[0]", id="array-index"),
    ],
)
def test_incremental_rejects_jsonpath_cursor(
    incremental_dataset: dlt.Dataset, cursor_path: str
) -> None:
    incremental = dlt.sources.incremental(cursor_path, initial_value=1)
    with pytest.raises(ValueError, match="JSONPath|plain column"):
        incremental_dataset.table("events").incremental(incremental)


@pytest.mark.parametrize(
    "cursor_path,match",
    [
        pytest.param("", "non-empty string", id="empty"),
        pytest.param("col.", "not a plain column identifier", id="trailing-dot"),
        pytest.param(".col", "not a plain column identifier", id="leading-dot"),
        pytest.param('"col with.dot"', "not a plain column identifier", id="quoted-with-dot"),
        pytest.param("$.name", "JSONPath expression", id="jsonpath-root"),
        pytest.param("items[0]", "JSONPath expression", id="array-index"),
    ],
)
def test_parse_incremental_cursor_path_rejects_malformed(cursor_path: str, match: str) -> None:
    from dlt.dataset._incremental import _parse_incremental_cursor_path

    with pytest.raises(ValueError, match=match):
        _parse_incremental_cursor_path(cursor_path)


def test_incremental_rejects_quoted_cursor_with_inner_dot(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental('"col with.dot"', initial_value=1)
    with pytest.raises(ValueError, match="not a plain column identifier"):
        incremental_dataset.table("events").incremental(incremental)


@pytest.mark.parametrize(
    "bounds_kwargs,bind_via_resource",
    [
        pytest.param({"initial_value": 2}, True, id="start-only"),
        pytest.param({"end_value": END_VALUE_ID}, False, id="end-only"),
        pytest.param({"initial_value": 2, "end_value": END_VALUE_ID}, False, id="start-and-end"),
    ],
)
@pytest.mark.parametrize(
    "policy,expected_root_cls",
    [
        pytest.param("include", sge.Or, id="include-or-is-null"),
        pytest.param("exclude", sge.And, id="exclude-and-is-not-null"),
    ],
)
def test_incremental_on_cursor_value_missing(
    incremental_pipeline: dlt.Pipeline,
    bounds_kwargs: dict[str, Any],
    bind_via_resource: bool,
    policy: Literal["include", "exclude"],
    expected_root_cls: type,
) -> None:
    dataset = incremental_pipeline.dataset()

    if bind_via_resource:
        bounds_id = "_".join(sorted(bounds_kwargs))
        resource_name = f"probe_null_guard_{policy}_{bounds_id}"
        captured: dlt.Relation | None = None

        @dlt.resource(name=resource_name)
        def probe(
            cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
                "id", on_cursor_value_missing=policy, **bounds_kwargs
            ),
        ) -> Iterator[Any]:
            nonlocal captured
            captured = dataset.table("events").incremental(cursor)
            yield from []

        incremental_pipeline.extract(probe())
        assert captured is not None
        relation = captured
    else:
        incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
            "id", on_cursor_value_missing=policy, **bounds_kwargs
        )
        relation = dataset.table("events").incremental(incremental)

    condition = _where(relation)
    assert isinstance(condition, expected_root_cls), (
        f"Expected `{expected_root_cls.__name__}` root for policy={policy} "
        f"bounds={bounds_kwargs}, got {type(condition).__name__}: "
        f"{condition.sql()}"
    )
    # right-hand side of the wrapper is the null-guard on the cursor column:
    # `Is(col, Null)` for include, `Not(Is(col, Null))` for exclude
    null_guard = condition.expression
    if isinstance(null_guard, sge.Not):
        null_guard = null_guard.this
    assert isinstance(null_guard, sge.Is)
    assert isinstance(null_guard.expression, sge.Null)
    assert _column_name(null_guard.this) == "id"


def test_incremental_raise_emits_is_not_null_pushdown(
    incremental_dataset: dlt.Dataset,
) -> None:
    # We can't raise on NULL cursor values, so `"raise"` (the default)
    # falls back to `... AND col IS NOT NULL`, same shape as `"exclude"`
    incremental = dlt.sources.incremental(
        "id",
        initial_value=2,
        end_value=END_VALUE_ID,
        on_cursor_value_missing="raise",
    )
    relation = incremental_dataset.table("events").incremental(incremental)

    condition = _where(relation)
    assert isinstance(condition, sge.And), (
        "raise pushdown must wrap with `AND IS NOT NULL`, got "
        f"{type(condition).__name__}: {condition.sql()}"
    )
    null_guard = condition.expression
    assert isinstance(null_guard, sge.Not)
    inner = null_guard.this
    assert isinstance(inner, sge.Is)
    assert isinstance(inner.expression, sge.Null)
    assert _column_name(inner.this) == "id"


def test_incremental_raise_warns_on_nullable_cursor(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
        on_cursor_value_missing="raise",
    )
    with pytest.warns(UserWarning, match="Can't raise on NULL cursor"):
        incremental_dataset.table("events").incremental(incremental)


def test_incremental_raise_no_warn_on_non_nullable_cursor(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
        on_cursor_value_missing="raise",
    )
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", UserWarning)
        incremental_dataset.table("events").incremental(incremental)
    pushdown_warnings = [w for w in captured if "Can't raise on NULL cursor" in str(w.message)]
    assert pushdown_warnings == [], (
        "unexpected pushdown warning on a non-nullable cursor: "
        f"{[str(w.message) for w in pushdown_warnings]}"
    )


def test_incremental_no_bounds_include_emits_no_where(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_no_bounds_include")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", on_cursor_value_missing="include"
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    relation = captured

    assert relation.sqlglot_expression.args.get("where") is None
    assert relation.is_incremental is True
    # the aggregate over the unfiltered base should still observe the full max id (5)
    assert relation._incremental_aggregate_relation().fetchscalar() == 5


@pytest.mark.parametrize("policy", ["exclude", "raise"])
def test_incremental_no_bounds_exclude_or_raise_emits_only_is_not_null(
    incremental_pipeline: dlt.Pipeline, policy: Literal["exclude", "raise"]
) -> None:
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name=f"probe_no_bounds_{policy}")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", on_cursor_value_missing=policy
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    relation = captured

    condition = _where(relation)
    assert isinstance(condition, sge.Not), (
        f"expected bare `IS NOT NULL` for no-bounds policy={policy!r}, "
        f"got {type(condition).__name__}: {condition.sql()}"
    )
    inner = condition.this
    assert isinstance(inner, sge.Is)
    assert isinstance(inner.expression, sge.Null)
    assert _column_name(inner.this) == "id"
    assert relation.is_incremental is True


@pytest.mark.parametrize("policy", ["include", "exclude"])
def test_incremental_no_warn_when_policy_explicit(
    incremental_dataset: dlt.Dataset, policy: Literal["include", "exclude"]
) -> None:
    incremental: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
        on_cursor_value_missing=policy,
    )
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", UserWarning)
        incremental_dataset.table("events").incremental(incremental)
    assert (
        captured == []
    ), f"unexpected warning for policy={policy!r}: {[str(w.message) for w in captured]}"


def _model_transformer(
    *,
    cursor_path: str = "id",
    start_value: Any = 0,
    end_value: Any = None,
    last_value_func: Any = max,
    range_start: Literal["open", "closed"] = "open",
    range_end: Literal["open", "closed"] = "open",
) -> ModelIncremental:
    return ModelIncremental(
        resource_name="test",
        cursor_path=cursor_path,
        initial_value=start_value,
        start_value=start_value,
        end_value=end_value,
        last_value_func=last_value_func,
        primary_key=None,
        unique_hashes=set(),
        range_start=range_start,
        range_end=range_end,
    )


def _capture_stateful_relation(
    pipeline: dlt.Pipeline,
    *,
    resource_name: str,
    initial_value: int,
    range_start: Literal["open", "closed"] = "open",
) -> dlt.Relation:
    """Build an `.incremental()`-applied Relation against a bound stateful cursor.

    Stateful incrementals need an active pipeline to resolve
    `get_state()`, so we wrap the build in a no-op resource and `extract()` it
    just to bind.
    """
    dataset = pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name=resource_name)
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=initial_value, range_start=range_start
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from []

    pipeline.extract(probe())
    assert captured is not None
    return captured


def test_get_transform_dispatches_modelincremental_for_relation(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental: dlt.sources.incremental[int] = dlt.sources.incremental(
        "id", initial_value=0, end_value=END_VALUE_ID
    )
    incremental._cached_state = {
        "unique_hashes": [],
        "initial_value": 0,
        "last_value": 0,
        "start_value": 0,
    }
    relation = incremental_dataset.table("events")
    incremental_transform = incremental._get_transform(relation)
    assert isinstance(incremental_transform, ModelIncremental)
    assert incremental_transform.cursor_path == "id"


def test_model_incremental_advances_last_value_for_open_range(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    relation = _capture_stateful_relation(
        incremental_pipeline, resource_name="probe_advance", initial_value=2
    )
    transformer = _model_transformer(start_value=2)
    out, start_out_of_range, end_out_of_range = transformer(relation)

    assert out is relation
    assert (start_out_of_range, end_out_of_range) == (False, False)
    assert transformer.last_value == 5


def test_model_incremental_no_advance_in_scheduler_mode(
    incremental_dataset: dlt.Dataset,
) -> None:
    incremental = dlt.sources.incremental("id", initial_value=0, end_value=END_VALUE_ID)
    relation = incremental_dataset.table("events").incremental(incremental)

    transformer = _model_transformer(start_value=0, end_value=END_VALUE_ID, range_start="closed")
    transformer(relation)

    assert transformer.last_value == 0


def test_model_incremental_rejects_closed_range_stateful(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    relation = _capture_stateful_relation(
        incremental_pipeline,
        resource_name="probe_reject_closed",
        initial_value=0,
        range_start="closed",
    )

    transformer = _model_transformer(start_value=0, range_start="closed")
    with pytest.raises(ValueError, match="range_start='open'"):
        transformer(relation)


def test_model_incremental_auto_applies_on_bare_relation(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    dataset = incremental_pipeline.dataset()
    yielded: dlt.Relation | None = None

    @dlt.resource(name="probe_auto_apply")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=2, range_start="open"
        ),
    ) -> Iterator[Any]:
        nonlocal yielded
        yielded = dataset.table("events")
        yield yielded

    resource = probe()
    incremental_pipeline.extract(resource)

    assert yielded is not None
    assert yielded.is_incremental is False

    # max(id) over the events table is 5, so `last_value` becomes 5.
    assert resource.state["incremental"]["id"]["last_value"] == 5


def test_model_incremental_does_not_clobber_last_value_on_empty_filter(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    # initial_value above all data (max is 5) so the WHERE excludes everything.
    relation = _capture_stateful_relation(
        incremental_pipeline, resource_name="probe_empty_filter", initial_value=10**9
    )

    transformer = _model_transformer(start_value=10**9)
    transformer(relation)

    assert transformer.last_value == 10**9
