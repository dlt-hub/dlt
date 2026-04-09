from datetime import date  # noqa: I251
from typing import Any, Callable, Optional

import pytest

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import TSortOrder  # noqa: F401  -- avoids unused-import lint downstream

from dlt.extract.exceptions import IncrementalUnboundError
from dlt.extract.incremental import Incremental


def _filter_sql(incr: Incremental[Any], apply_lag: bool = True) -> Optional[str]:
    expr = incr.to_sqlglot_filter(apply_lag=apply_lag)
    return expr.sql(dialect="duckdb") if expr is not None else None


def _bind_state(
    incr: Incremental[Any],
    *,
    initial_value: Any = None,
    last_value: Any = None,
    start_value: Any = None,
) -> None:
    """Inject cached state and instance start_value as `bind()` would have."""
    incr._cached_state = {
        "initial_value": initial_value,
        "last_value": last_value,
        "start_value": start_value,
        "unique_hashes": [],
    }
    incr.start_value = start_value


@pytest.mark.parametrize(
    (
        "last_value_func",
        "range_start",
        "range_end",
        "lower",
        "upper",
        "on_cursor_value_missing",
        "expected_sql",
    ),
    [
        pytest.param(
            max,
            "closed",
            "open",
            10,
            50,
            "raise",
            '"created_at" >= CAST(10 AS BIGINT) AND "created_at" < CAST(50 AS BIGINT)',
            id="max-closed-open-both",
        ),
        pytest.param(
            max,
            "open",
            "closed",
            10,
            50,
            "raise",
            '"created_at" > CAST(10 AS BIGINT) AND "created_at" <= CAST(50 AS BIGINT)',
            id="max-open-closed-both",
        ),
        pytest.param(
            min,
            "closed",
            "open",
            50,
            10,
            "raise",
            '"created_at" <= CAST(50 AS BIGINT) AND "created_at" > CAST(10 AS BIGINT)',
            id="min-closed-open-both",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            None,
            100,
            "raise",
            '"created_at" < CAST(100 AS BIGINT)',
            id="max-upper-only",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            10,
            None,
            "raise",
            '"created_at" >= CAST(10 AS BIGINT)',
            id="max-lower-only",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            10,
            50,
            "include",
            '"created_at" >= CAST(10 AS BIGINT) AND "created_at" < CAST(50 AS BIGINT)'
            ' OR "created_at" IS NULL',
            id="max-both-include-null",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            10,
            50,
            "exclude",
            '"created_at" >= CAST(10 AS BIGINT) AND "created_at" < CAST(50 AS BIGINT)'
            ' AND NOT "created_at" IS NULL',
            id="max-both-exclude-null",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            None,
            None,
            "exclude",
            'NOT "created_at" IS NULL',
            id="no-bounds-exclude-null",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            None,
            None,
            "include",
            None,
            id="no-bounds-include-null",
        ),
        pytest.param(
            max,
            "closed",
            "open",
            None,
            None,
            "raise",
            None,
            id="no-bounds-raise",
        ),
    ],
)
def test_to_sqlglot_filter_operators(
    last_value_func: Callable[[Any], Any],
    range_start: str,
    range_end: str,
    lower: Optional[int],
    upper: Optional[int],
    on_cursor_value_missing: str,
    expected_sql: Optional[str],
) -> None:
    incr = dlt.sources.incremental[int](
        "created_at",
        last_value_func=last_value_func,
        range_start=range_start,  # type: ignore[arg-type]
        range_end=range_end,  # type: ignore[arg-type]
        on_cursor_value_missing=on_cursor_value_missing,  # type: ignore[arg-type]
    )
    _bind_state(incr, initial_value=lower, last_value=upper, start_value=lower)
    assert _filter_sql(incr) == expected_sql


def test_to_sqlglot_filter_apply_lag_false_uses_state() -> None:
    """`apply_lag=False` reads `state["start_value"]`, not the lag-adjusted instance attr."""
    incr = dlt.sources.incremental[int]("created_at", initial_value=10)
    incr._cached_state = {
        "initial_value": 10,
        "last_value": 50,
        "start_value": 20,  # raw, persisted
        "unique_hashes": [],
    }
    incr.start_value = 15  # what bind() would have set with lag

    # apply_lag=True picks the lag-adjusted instance attribute
    assert (
        _filter_sql(incr, apply_lag=True)
        == '"created_at" >= CAST(15 AS BIGINT) AND "created_at" < CAST(50 AS BIGINT)'
    )
    # apply_lag=False picks the raw value from state
    assert (
        _filter_sql(incr, apply_lag=False)
        == '"created_at" >= CAST(20 AS BIGINT) AND "created_at" < CAST(50 AS BIGINT)'
    )


def test_to_sqlglot_filter_apply_lag_false_unbound_raises() -> None:
    """`apply_lag=False` requires a bound incremental; `apply_lag=True` does not."""
    incr = dlt.sources.incremental[int]("created_at", initial_value=10)
    # not bound — _cached_state is None
    with pytest.raises(IncrementalUnboundError):
        incr.to_sqlglot_filter(apply_lag=False)
    # apply_lag=True falls back to instance start_value (= initial_value)
    assert _filter_sql(incr, apply_lag=True) == '"created_at" >= CAST(10 AS BIGINT)'


@pytest.mark.parametrize(
    ("py_type", "value", "expected_cast"),
    [
        pytest.param(
            pendulum.DateTime,
            pendulum.parse("2024-01-01T00:00:00Z"),
            "CAST('2024-01-01 00:00:00+00:00' AS TIMESTAMP)",
            id="datetime-tz-aware",
        ),
        pytest.param(
            date,
            date(2024, 1, 1),
            "CAST('2024-01-01' AS DATE)",
            id="date",
        ),
        pytest.param(
            str,
            "abc",
            "CAST('abc' AS TEXT)",
            id="text",
        ),
        pytest.param(
            float,
            1.5,
            "CAST(1.5 AS DOUBLE)",
            id="double",
        ),
    ],
)
def test_to_sqlglot_filter_typed_literals(py_type: type, value: Any, expected_cast: str) -> None:
    incr = dlt.sources.incremental[py_type]("created_at", initial_value=value)  # type: ignore[valid-type]
    _bind_state(incr, initial_value=value, last_value=None, start_value=value)
    sql = _filter_sql(incr)
    assert sql == f'"created_at" >= {expected_cast}'


def test_to_sqlglot_filter_returns_none_for_jsonpath_cursor() -> None:
    incr = dlt.sources.incremental("$.foo.bar", initial_value=10)
    _bind_state(incr, initial_value=10, last_value=50, start_value=10)
    assert incr.to_sqlglot_filter() is None


def test_to_sqlglot_filter_returns_none_for_custom_last_value_func() -> None:
    incr = dlt.sources.incremental[int](
        "created_at", initial_value=10, last_value_func=lambda xs: xs[-1]
    )
    _bind_state(incr, initial_value=10, last_value=50, start_value=10)
    assert incr.to_sqlglot_filter() is None


def test_to_sqlglot_filter_untyped_literal_when_type_unknown() -> None:
    """Cursor with no Generic param and no initial_value -> Any -> untyped literal (no CAST)."""
    incr: Incremental[Any] = dlt.sources.incremental("created_at")
    _bind_state(incr, initial_value=None, last_value=50, start_value=10)
    assert _filter_sql(incr) == '"created_at" >= 10 AND "created_at" < 50'
