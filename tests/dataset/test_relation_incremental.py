from __future__ import annotations

import pathlib
import warnings
from typing import Any, Iterator, Literal, Optional

import pytest
from sqlglot import expressions as sge

import dlt
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.sqlglot import to_sqlglot_type
from dlt.common.pendulum import pendulum
from dlt.dataset._incremental import (
    _build_incremental_aggregate,
    _build_incremental_condition,
    _parse_incremental_cursor_path,
    _RelationIncrementalContext,
)


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


def test_incremental_dotted_cursor_rejects_incomplete_target_column(
    dataset_with_incomplete_join_target: dlt.Dataset,
) -> None:
    """An incomplete (declared but unmaterialized) cursor column must be rejected
    up front by `.incremental()` rather than producing a relation whose SQL
    references a column that doesn't exist at the destination.
    """
    incremental = dlt.sources.incremental(
        "categories.phantom_field",
        initial_value=0,
        end_value=10**12,
        on_cursor_value_missing="exclude",
    )
    with pytest.raises(ValueError, match="not a materialized column on table `categories`"):
        dataset_with_incomplete_join_target.table("products").incremental(incremental)


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


def test_incremental_aggregate_rejects_limit_in_stateful_mode(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    # In stateful mode (no end_value), LIMIT would advance state past only the
    # returned rows. Rejected so callers can't silently skip rows. ORDER BY
    # alone does not change the row set, so it's allowed.
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_reject")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=0, range_start="open"
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor).limit(2)
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    with pytest.raises(ValueError, match="LIMIT isn't supported"):
        captured._incremental_aggregate_relation()


def _caps(
    *,
    dialect: Optional[str] = None,
    timestamp_precision: int = 6,
    supports_tz_aware_datetime: bool = True,
    supports_tz_aware_datetime_in_cast: Optional[bool] = None,
) -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.sqlglot_dialect = dialect  # type: ignore[assignment]
    caps.timestamp_precision = timestamp_precision
    caps.supports_tz_aware_datetime = supports_tz_aware_datetime
    caps.supports_tz_aware_datetime_in_cast = supports_tz_aware_datetime_in_cast
    return caps


_TS_EPOCH = pendulum.datetime(2026, 1, 1, tz="UTC")
_TS_END = pendulum.datetime(2026, 1, 5, tz="UTC")


def _ts_sqlglot_type(timezone: Optional[bool] = True) -> sge.DataType:
    return to_sqlglot_type(dlt_type="timestamp", precision=6, timezone=timezone, nullable=True)


@pytest.mark.parametrize(
    ("caps_kwargs", "dialect", "tz_aware_cursor", "must_contain", "must_not_contain"),
    [
        pytest.param(
            None,
            "duckdb",
            True,
            [
                "CAST('2026-01-01 00:00:00.000000+00:00' AS TIMESTAMPTZ)",
                "CAST('2026-01-05 00:00:00.000000+00:00' AS TIMESTAMPTZ)",
            ],
            [],
            id="no-caps-generic-tz-aware-cast",
        ),
        pytest.param(
            {"dialect": "duckdb", "timestamp_precision": 6, "supports_tz_aware_datetime": True},
            "duckdb",
            True,
            ["CAST('2026-01-01 00:00:00.000000+00:00' AS TIMESTAMPTZ)"],
            [],
            id="duckdb-keeps-tz-aware-cast",
        ),
        pytest.param(
            {"dialect": "sqlite", "timestamp_precision": 0, "supports_tz_aware_datetime": False},
            "sqlite",
            True,
            ["'2026-01-01 00:00:00'", "'2026-01-05 00:00:00'"],
            ["CAST("],
            id="sqlite-drops-cast-naive-form",
        ),
        pytest.param(
            {"dialect": "dremio", "timestamp_precision": 6, "supports_tz_aware_datetime": False},
            "dremio",
            True,
            ["TIMESTAMP"],
            ["TIMESTAMPTZ", "+00:00"],
            id="dremio-athena-naive-cast",
        ),
        pytest.param(
            {
                "dialect": "clickhouse",
                "timestamp_precision": 6,
                "supports_tz_aware_datetime": True,
                "supports_tz_aware_datetime_in_cast": False,
            },
            "clickhouse",
            True,
            [],
            ["+00:00"],
            id="clickhouse-tz-cast-unsupported-naive-cast",
        ),
        pytest.param(
            {"dialect": "bigquery", "timestamp_precision": 0},
            "bigquery",
            True,
            ["'2026-01-01 00:00:00+00:00'"],
            [".000000"],
            id="bigquery-precision-zero-trims-fractional",
        ),
        pytest.param(
            {"dialect": "duckdb", "supports_tz_aware_datetime": True},
            "duckdb",
            False,
            ["'2026-01-01 00:00:00.000000'"],
            ["+00:00"],
            id="naive-cursor-naive-form-regardless-of-caps",
        ),
    ],
)
def test_incremental_timestamp_emission(
    caps_kwargs: Optional[dict[str, Any]],
    dialect: str,
    tz_aware_cursor: bool,
    must_contain: list[str],
    must_not_contain: list[str],
) -> None:
    if tz_aware_cursor:
        incr = dlt.sources.incremental[pendulum.DateTime](
            "created_at", initial_value=_TS_EPOCH, end_value=_TS_END
        )
        sqlglot_type = _ts_sqlglot_type()
    else:
        incr = dlt.sources.incremental[pendulum.DateTime](
            "created_at",
            initial_value=pendulum.naive(2026, 1, 1),
            end_value=pendulum.naive(2026, 1, 5),
        )
        sqlglot_type = _ts_sqlglot_type(timezone=False)

    caps = _caps(**caps_kwargs) if caps_kwargs is not None else None
    column_ref = sge.Column(this=sge.to_identifier("created_at", quoted=True))
    cond = _build_incremental_condition(
        incr, column_ref, sqlglot_type, destination_capabilities=caps
    )
    assert cond is not None
    sql = cond.sql(dialect=dialect)
    for expected in must_contain:
        assert expected in sql, f"expected {expected!r} in {sql!r}"
    for unexpected in must_not_contain:
        assert unexpected not in sql, f"unexpected {unexpected!r} in {sql!r}"


@pytest.mark.parametrize(
    ("dlt_type", "initial_value", "expected_cast"),
    [
        pytest.param("date", pendulum.date(2026, 1, 1), "CAST('2026-01-01' AS DATE)", id="date"),
        pytest.param("text", "abc", "CAST('abc' AS TEXT)", id="text"),
        pytest.param("double", 1.5, "CAST(1.5 AS DOUBLE)", id="double"),
    ],
)
def test_incremental_condition_typed_literal_for_non_timestamp_types(
    dlt_type: str, initial_value: Any, expected_cast: str
) -> None:
    incr = dlt.sources.incremental(
        "created_at", initial_value=initial_value, on_cursor_value_missing="exclude"
    )
    column_ref = sge.Column(this=sge.to_identifier("created_at", quoted=True))
    sqlglot_type = to_sqlglot_type(dlt_type=dlt_type, nullable=True)  # type: ignore[arg-type]
    cond = _build_incremental_condition(incr, column_ref, sqlglot_type)
    assert cond is not None
    assert expected_cast in cond.sql(dialect="duckdb")


def test_incremental_condition_untyped_literals_when_sqlglot_type_unknown() -> None:
    incr: dlt.sources.incremental[int] = dlt.sources.incremental(
        "created_at", initial_value=10, end_value=50, on_cursor_value_missing="exclude"
    )
    column_ref = sge.Column(this=sge.to_identifier("created_at", quoted=True))
    cond = _build_incremental_condition(incr, column_ref, sqlglot_type=None)
    assert cond is not None
    sql = cond.sql(dialect="duckdb")
    assert "CAST(" not in sql
    assert '"created_at" >= 10' in sql
    assert '"created_at" < 50' in sql


def _build_agg_sql(*, caps: Optional[DestinationCapabilitiesContext], dialect: str) -> str:
    incr = dlt.sources.incremental[int]("id", initial_value=0, range_start="open")
    ctx = _RelationIncrementalContext(
        incremental=incr,
        cursor_column=sge.Column(this=sge.to_identifier("id", quoted=True)),
    )
    base = sge.Select(expressions=[sge.Column(this=sge.to_identifier("id", quoted=True))]).from_(
        sge.Table(this=sge.to_identifier("t", quoted=True))
    )
    return _build_incremental_aggregate(base, ctx, destination_capabilities=caps).sql(
        dialect=dialect
    )


def test_incremental_aggregate_uses_plain_max_when_caps_lack_null_safe_wrapper() -> None:
    # standard-SQL destinations get a plain MAX(...): empty input -> NULL, the
    # caller's `is not None` guard preserves state.
    sql = _build_agg_sql(caps=_caps(dialect="duckdb"), dialect="duckdb")
    assert 'MAX("__dlt_inc_cursor")' in sql
    assert "OrNull" not in sql

    sql = _build_agg_sql(caps=None, dialect="duckdb")
    assert 'MAX("__dlt_inc_cursor")' in sql


def test_incremental_aggregate_applies_null_safe_wrapper_when_caps_provide_one() -> None:
    from dlt.destinations.impl.clickhouse.factory import _clickhouse_null_safe_aggregate

    caps = _caps(dialect="clickhouse")
    caps.null_safe_aggregate = _clickhouse_null_safe_aggregate
    sql = _build_agg_sql(caps=caps, dialect="clickhouse")
    assert 'maxOrNull("__dlt_inc_cursor")' in sql
    assert "MAX(" not in sql


def test_incremental_aggregate_allows_order_by_in_stateful_mode(
    incremental_pipeline: dlt.Pipeline,
) -> None:
    # ORDER BY alone doesn't change which rows are returned and MAX/MIN are
    # order-independent, so the aggregate still reflects every row.
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_allow_order")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=0, range_start="open"
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor).order_by("id", "desc")
        yield from []

    incremental_pipeline.extract(probe())
    assert captured is not None
    assert captured._incremental_aggregate_relation().fetchscalar() == 5


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


def test_incremental_lag_on_unbound_is_no_op(incremental_dataset: dlt.Dataset) -> None:
    no_lag = dlt.sources.incremental("id", initial_value=10, end_value=END_VALUE_ID)
    with_lag = dlt.sources.incremental("id", initial_value=10, end_value=END_VALUE_ID, lag=5)
    no_lag_sql = incremental_dataset.table("events").incremental(no_lag).sqlglot_expression.sql()
    with_lag_sql = (
        incremental_dataset.table("events").incremental(with_lag).sqlglot_expression.sql()
    )
    assert no_lag_sql == with_lag_sql


def test_incremental_lag_applied_after_bind(incremental_pipeline: dlt.Pipeline) -> None:
    dataset = incremental_pipeline.dataset()
    captured: dlt.Relation | None = None

    @dlt.resource(name="probe_lag_after_extract")
    def probe(
        cursor: dlt.sources.incremental[int] = dlt.sources.incremental(
            "id", initial_value=0, lag=2
        ),
    ) -> Iterator[Any]:
        nonlocal captured
        captured = dataset.table("events").incremental(cursor)
        yield from [{"id": i} for i in range(1, 6)]

    # first extract: cursor starts unbound, lag is a no-op, state["last_value"] -> 5
    incremental_pipeline.extract(probe())
    # second extract: bind() reads last_value=5 and applies lag -> start_value=3
    incremental_pipeline.extract(probe())

    assert captured is not None
    condition = _where(captured)
    assert isinstance(condition, sge.And)
    start_op = condition.this
    assert isinstance(start_op, sge.GTE)
    assert _column_name(start_op.this) == "id"
    assert "CAST(3 AS BIGINT)" in start_op.expression.sql(dialect="duckdb")


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
    "build_relation,expected_rows",
    [
        pytest.param(
            lambda ds, load_ids, incremental: ds.table(
                "events", load_ids=load_ids[:1], incremental=incremental
            ),
            len(EVENTS_LOAD_0),
            id="kwargs",
        ),
        pytest.param(
            lambda ds, load_ids, incremental: ds.table("events")
            .from_loads(load_ids[:1])
            .incremental(incremental),
            len(EVENTS_LOAD_0),
            id="chained",
        ),
        pytest.param(
            lambda ds, load_ids, incremental: ds.table("events")
            .select("id", "value")
            .incremental(incremental),
            len(EVENTS_LOAD_0) + len(EVENTS_LOAD_1),
            id="after-select",
        ),
    ],
)
def test_incremental_dotted_cursor_on_derived_relation(
    incremental_pipeline: dlt.Pipeline, build_relation: Any, expected_rows: int
) -> None:
    """Root-table derivations keep the base table in FROM, so a dotted cursor still applies."""
    dataset = incremental_pipeline.dataset()
    load_ids = dataset.load_ids()
    assert load_ids, "fixture must produce at least one load"

    incremental = dlt.sources.incremental(
        "_dlt_loads.inserted_at",
        initial_value=pendulum.datetime(2026, 1, 1, tz="UTC"),
        end_value=END_VALUE_DT,
    )
    relation = build_relation(dataset, load_ids, incremental)
    assert len(relation.fetchall()) == expected_rows


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
    with pytest.warns(UserWarning, match="Can't raise on NULL cursor") as records:
        incremental_dataset.table("events").incremental(incremental)
    assert records[0].filename == __file__


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
