from __future__ import annotations

import pathlib
import warnings
from typing import Any, Iterator, List, Literal

import pytest
from sqlglot import expressions as sge

import dlt
from dlt.common.pendulum import pendulum


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


def _where(rel: dlt.Relation) -> sge.Expression:
    where_node = rel.sqlglot_expression.args.get("where")
    assert isinstance(where_node, sge.Where), f"Expected WHERE clause, got {where_node!r}"
    return where_node.this


def _column_name(expr: sge.Expression) -> str:
    assert isinstance(expr, sge.Column), f"Expected Column, got {expr!r}"
    return expr.args["this"].name


def _column_table(expr: sge.Expression) -> str | None:
    assert isinstance(expr, sge.Column), f"Expected Column, got {expr!r}"
    table = expr.args.get("table")
    return table.name if table is not None else None


def _join_target_names(rel: dlt.Relation) -> list[str]:
    joins = rel.sqlglot_expression.args.get("joins") or []
    names: list[str] = []
    for join in joins:
        target = join.this
        assert isinstance(target, sge.Table)
        names.append(target.this.name)
    return names


def test_incremental_emits_where_on_simple_cursor(incremental_dataset: dlt.Dataset) -> None:
    inc = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    rel = incremental_dataset.table("events").incremental(inc)

    condition = _where(rel)
    assert isinstance(condition, sge.And)
    bound_pair = condition.this
    assert isinstance(bound_pair, sge.And)
    assert isinstance(bound_pair.this, sge.GTE)
    assert _column_name(bound_pair.this.this) == "id"
    # no join is added for a simple cursor path
    assert (rel.sqlglot_expression.args.get("joins") or []) == []


def test_incremental_sets_is_incremental_flag(incremental_dataset: dlt.Dataset) -> None:
    base = incremental_dataset.table("events")
    assert base.is_incremental is False

    inc = dlt.sources.incremental("id", initial_value=1, end_value=END_VALUE_ID)
    flagged = base.incremental(inc)
    assert flagged.is_incremental is True

    # flag survives further chaining — meta must propagate through copies
    chained = flagged.select("id", "value").where("value", "gt", 0)
    assert chained.is_incremental is True

    # a plain where() never sets the flag
    assert base.where("id", "gt", 1).is_incremental is False


def test_incremental_kwarg_on_table_equivalent_to_method(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)

    via_kwarg = incremental_dataset.table("events", incremental=inc).sqlglot_expression.sql()
    via_method = incremental_dataset.table("events").incremental(inc).sqlglot_expression.sql()

    assert via_kwarg == via_method


def test_incremental_returns_new_relation(incremental_dataset: dlt.Dataset) -> None:
    base = incremental_dataset.table("events")
    sql_before = base.sqlglot_expression.sql()

    inc = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    filtered = base.incremental(inc)

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
    inc = dlt.sources.incremental(
        "id",
        initial_value=2,
        end_value=4,
        last_value_func=last_value_func,
        range_start=range_start,
        range_end=range_end,
    )
    rel = incremental_dataset.table("events").incremental(inc)

    condition = _where(rel)
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
    inc = dlt.sources.incremental("created_at", initial_value=ts, end_value=END_VALUE_DT)
    # `created_at` is nullable, below silence "raise" warning
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        rel = incremental_dataset.table("events").incremental(inc)

    sql = rel.sqlglot_expression.sql(dialect=incremental_dataset.destination_dialect)
    assert "2026-01-05" in sql
    assert "DateTime(" not in sql
    assert "datetime.datetime" not in sql


def test_incremental_dotted_cursor_auto_joins_target(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    # _dlt_loads.inserted_at is `nullable=False` in the system schema, so the
    # default "raise" policy stays silent here — no warnings.catch_warnings needed
    rel = incremental_dataset.table("events").incremental(inc)

    # exactly one JOIN added, targeting _dlt_loads
    assert _join_target_names(rel) == ["_dlt_loads"]

    # bound pair is wrapped with AND IS NOT NULL by the default "raise" policy
    condition = _where(rel)
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
    inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at", end_value=END_VALUE_DT
    )
    rel = incremental_dataset.table("events").incremental(inc)

    # no column from _dlt_loads appears in the SELECT list — the auto-join
    # is filter-only (project=False path).
    selects = rel.sqlglot_expression.selects
    output_names = [expr.output_name for expr in selects]
    assert not any(name.startswith("_dlt_loads__") for name in output_names)


def test_incremental_dotted_cursor_runtime_columns_base_only(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    rel = incremental_dataset.table("events").incremental(inc)

    expected_columns = set(incremental_dataset.table("events").columns)
    assert set(rel.columns) == expected_columns
    assert not any(c.startswith("_dlt_loads__") for c in rel.columns)

    row = rel.fetchone()
    assert row is not None
    assert len(row) == len(rel.columns)


def test_incremental_dotted_cursor_reuses_existing_join(
    incremental_dataset: dlt.Dataset,
) -> None:
    """An explicit .join() before .incremental() on the same target should
    not be duplicated — the WHERE latches onto the existing qualifier.
    """
    pre_joined = incremental_dataset.table("events").join("_dlt_loads")
    existing_targets = _join_target_names(pre_joined)
    assert existing_targets.count("_dlt_loads") == 1

    inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at", end_value=END_VALUE_DT
    )
    rel = pre_joined.incremental(inc)

    assert _join_target_names(rel).count("_dlt_loads") == 1


def test_incremental_aggregate_on_simple_cursor(incremental_dataset: dlt.Dataset) -> None:
    """`_incremental_aggregate_relation` returns the MAX cursor over the filter."""
    inc = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    rel = incremental_dataset.table("events").incremental(inc)
    # max id across EVENTS_LOAD_0 + EVENTS_LOAD_1 with id >= 2 is 5
    assert rel._incremental_aggregate_relation().fetchscalar() == 5


def test_incremental_aggregate_on_dotted_cursor(incremental_dataset: dlt.Dataset) -> None:
    inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    rel = incremental_dataset.table("events").incremental(inc)
    # exact value depends on load timing, but a MAX of inserted_at should be non-null
    agg_value = rel._incremental_aggregate_relation().fetchscalar()
    assert agg_value is not None


def test_incremental_aggregate_returns_none_when_not_incremental(
    incremental_dataset: dlt.Dataset,
) -> None:
    not_incremental = incremental_dataset.table("events")
    assert not_incremental._incremental_aggregate_relation() is None


def test_incremental_aggregate_honors_min(incremental_dataset: dlt.Dataset) -> None:
    """`last_value_func=min` flips the aggregate to SQL `MIN`."""
    # for min: closed start -> `<=`, closed end -> `>=`. Window [0, 5] contains ids 1-5.
    inc = dlt.sources.incremental(
        "id",
        initial_value=5,
        end_value=0,
        last_value_func="min",
        range_end="closed",
    )
    rel = incremental_dataset.table("events").incremental(inc)
    assert rel._incremental_aggregate_relation().fetchscalar() == 1


def test_incremental_aggregate_matches_user_visible_rows_under_limit(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental("id", initial_value=2, end_value=END_VALUE_ID)
    rel = incremental_dataset.table("events").incremental(inc).order_by("id", "desc").limit(2)

    id_index = rel.columns.index("id")
    visible_ids = sorted(row[id_index] for row in rel.fetchall())
    # filter id >= 2 -> {2,3,4,5}; ORDER BY id DESC LIMIT 2 -> {4, 5}
    assert visible_ids == [4, 5]
    assert rel._incremental_aggregate_relation().fetchscalar() == 5


def test_incremental_aggregate_matches_user_visible_rows_under_limit_min(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental(
        "id",
        initial_value=10,
        end_value=2,
        last_value_func="min",
    )
    rel = incremental_dataset.table("events").incremental(inc).order_by("id", "desc").limit(2)

    id_index = rel.columns.index("id")
    visible_ids = sorted(row[id_index] for row in rel.fetchall())
    # for min: closed start -> id <= 10, closed end -> id >= 2 -> {2,3,4,5}
    # ORDER BY id DESC LIMIT 2 -> {4, 5}
    assert visible_ids == [4, 5]
    assert rel._incremental_aggregate_relation().fetchscalar() == 4


def test_incremental_inside_resource_captures_bound_sql(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    dataset = incremental_pipeline.dataset()
    captured: List[dlt.Relation] = []

    @dlt.resource(name="probe_simple_cursor")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental("id", initial_value=2),
    ) -> Iterator[Any]:
        captured.append(dataset.table("events").incremental(cursor))
        yield from []

    incremental_pipeline.extract(probe())
    condition = _where(captured[0])
    assert isinstance(condition, sge.And)
    start_op = condition.this
    assert isinstance(start_op, sge.GTE)
    assert _column_name(start_op.this) == "id"


def test_incremental_custom_last_value_func_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    """Only `min` and `max` can be pushed down to SQL; custom callables can't."""
    inc = dlt.sources.incremental("id", initial_value=1, last_value_func=lambda xs: max(xs))
    with pytest.raises(ValueError, match="last_value_func"):
        incremental_dataset.table("events").incremental(inc)


def test_incremental_unknown_dotted_target_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental("not_a_table.ts", initial_value=1)
    with pytest.raises(ValueError, match="not found in dataset schema"):
        incremental_dataset.table("events").incremental(inc)


def test_incremental_dotted_cursor_on_query_relation_raises(
    incremental_dataset: dlt.Dataset,
) -> None:
    """Dotted cursors need a base-table relation to resolve the join chain."""
    q_rel = incremental_dataset.query("SELECT * FROM events")
    inc = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    with pytest.raises(ValueError, match="no base table"):
        q_rel.incremental(inc)


def test_incremental_chained_call_raises(incremental_dataset: dlt.Dataset) -> None:
    inc1 = dlt.sources.incremental("id", initial_value=1, end_value=END_VALUE_ID)
    inc2 = dlt.sources.incremental("value", initial_value=0.0, end_value=10.0)

    rel = incremental_dataset.table("events").incremental(inc1)
    with pytest.raises(ValueError, match="already been applied"):
        rel.incremental(inc2)


@pytest.mark.parametrize(
    "build_rel",
    [
        pytest.param(
            lambda ds, load_ids, inc: ds.table("events", load_ids=load_ids, incremental=inc),
            id="kwargs",
        ),
        pytest.param(
            lambda ds, load_ids, inc: ds.table("events").from_loads(load_ids).incremental(inc),
            id="chained",
        ),
    ],
)
def test_incremental_dotted_cursor_after_from_loads_raises(
    incremental_pipeline: dlt.Pipeline, build_rel: Any
) -> None:
    """`.from_loads()` wraps FROM in a subquery, so a subsequent dotted-cursor
    `.incremental()` cannot resolve the join. Both the kwargs combo on
    `dataset.table()` and the chained form must fail with a clear, user-facing
    message rather than the internal `_discover_join_params` error.
    """
    dataset = incremental_pipeline.dataset()
    load_ids = dataset.load_ids()
    assert load_ids, "fixture must produce at least one load"

    inc = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    with pytest.raises(ValueError, match="dotted cursor cannot be applied"):
        build_rel(dataset, load_ids, inc)


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
    inc = dlt.sources.incremental(cursor_path, initial_value=1)
    with pytest.raises(ValueError, match="JSONPath|plain column"):
        incremental_dataset.table("events").incremental(inc)


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
    inc = dlt.sources.incremental('"col with.dot"', initial_value=1)
    with pytest.raises(ValueError, match="not a plain column identifier"):
        incremental_dataset.table("events").incremental(inc)


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
        captured: List[dlt.Relation] = []

        @dlt.resource(name=resource_name)
        def probe(
            cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
                "id", on_cursor_value_missing=policy, **bounds_kwargs
            ),
        ) -> Iterator[Any]:
            captured.append(dataset.table("events").incremental(cursor))
            yield from []

        incremental_pipeline.extract(probe())
        rel = captured[0]
    else:
        inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
            "id", on_cursor_value_missing=policy, **bounds_kwargs
        )
        rel = dataset.table("events").incremental(inc)

    condition = _where(rel)
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
    inc = dlt.sources.incremental(
        "id",
        initial_value=2,
        end_value=END_VALUE_ID,
        on_cursor_value_missing="raise",
    )
    rel = incremental_dataset.table("events").incremental(inc)

    condition = _where(rel)
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
    inc = dlt.sources.incremental(
        "created_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
        on_cursor_value_missing="raise",
    )
    with pytest.warns(UserWarning, match="Can't raise on NULL cursor"):
        incremental_dataset.table("events").incremental(inc)


def test_incremental_raise_no_warn_on_non_nullable_cursor(
    incremental_dataset: dlt.Dataset,
) -> None:
    inc = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
        on_cursor_value_missing="raise",
    )
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", UserWarning)
        incremental_dataset.table("events").incremental(inc)
    pushdown_warnings = [w for w in captured if "Can't raise on NULL cursor" in str(w.message)]
    assert pushdown_warnings == [], (
        "unexpected pushdown warning on a non-nullable cursor: "
        f"{[str(w.message) for w in pushdown_warnings]}"
    )


def test_incremental_no_warn_when_policy_explicit(
    incremental_dataset: dlt.Dataset,
) -> None:
    """The warning is gated on `on_cursor_value_missing == 'raise'` — explicit
    `"include"`/`"exclude"` on the same nullable cursor must stay silent."""
    for policy in ("include", "exclude"):
        inc: dlt.sources.incremental[Any] = dlt.sources.incremental(
            "created_at",
            initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
            end_value=END_VALUE_DT,
            on_cursor_value_missing=policy,
        )
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", UserWarning)
            incremental_dataset.table("events").incremental(inc)

        pushdown_warnings = [w for w in captured if "Can't raise on NULL cursor" in str(w.message)]
        assert pushdown_warnings == [], (
            f"unexpected pushdown warning for policy={policy!r}: "
            f"{[str(w.message) for w in pushdown_warnings]}"
        )
