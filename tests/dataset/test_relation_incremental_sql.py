from copy import copy
from datetime import date  # noqa: I251
from typing import Any, Optional

import pytest
import sqlglot.expressions as sge

import dlt
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.sqlglot import to_sqlglot_type
from dlt.common.pendulum import pendulum
from dlt.dataset._incremental import _build_incremental_condition
from dlt.extract.incremental import Incremental


def _caps(
    *,
    dialect: Optional[str] = None,
    timestamp_precision: int = 6,
    supports_tz_aware_datetime: bool = True,
    supports_tz_aware_datetime_in_cast: Optional[bool] = None,
) -> DestinationCapabilitiesContext:
    """Build a minimal caps object for the fields `resolve_timestamp_cast` reads."""
    caps = DestinationCapabilitiesContext()
    caps.sqlglot_dialect = dialect  # type: ignore[assignment]
    caps.timestamp_precision = timestamp_precision
    caps.supports_tz_aware_datetime = supports_tz_aware_datetime
    caps.supports_tz_aware_datetime_in_cast = supports_tz_aware_datetime_in_cast
    return caps


def _bind_state(
    incr: Incremental[Any],
    *,
    initial_value: Any = None,
    last_value: Any = None,
    start_value: Any = None,
) -> Incremental[Any]:
    """Inject cached state as `bind()` would have done."""
    incr._cached_state = {
        "initial_value": initial_value,
        "last_value": last_value,
        "start_value": start_value,
        "unique_hashes": [],
    }
    incr.start_value = start_value
    return incr


def _emit_sql(
    incr: Incremental[Any],
    *,
    column_name: str = "created_at",
    sqlglot_type: Optional[sge.DataType] = None,
    caps: Optional[DestinationCapabilitiesContext] = None,
    dialect: str = "duckdb",
) -> Optional[str]:
    column_ref = sge.Column(this=sge.to_identifier(column_name, quoted=True))
    cond = _build_incremental_condition(
        incr, column_ref, sqlglot_type, destination_capabilities=caps
    )
    return cond.sql(dialect=dialect) if cond is not None else None


@pytest.mark.parametrize(
    ("incremental_kwargs", "bind_state", "expected"),
    [
        pytest.param(
            {"initial_value": 10},
            None,
            '"id" >= CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="unbound-initial-value-only-does-not-raise",
        ),
        pytest.param(
            {"initial_value": 10, "end_value": 100},
            None,
            '"id" >= CAST(10 AS BIGINT) AND "id" < CAST(100 AS BIGINT) AND NOT "id" IS NULL',
            id="unbound-with-end-value-emits-both-bounds",
        ),
        pytest.param(
            {"initial_value": 0},
            {"initial_value": 0, "last_value": 5, "start_value": 5},
            '"id" >= CAST(5 AS BIGINT) AND NOT "id" IS NULL',
            id="bound-no-end-value-omits-upper-bound",
        ),
        pytest.param(
            {"initial_value": 0, "lag": 2},
            # state advanced to last=5, bind() applied lag -> start_value = 3
            {"initial_value": 0, "last_value": 5, "start_value": 3},
            '"id" >= CAST(3 AS BIGINT) AND NOT "id" IS NULL',
            id="bound-with-lag-uses-lag-adjusted-start",
        ),
    ],
)
def test_bounds_resolution(
    incremental_kwargs: dict[str, Any],
    bind_state: Optional[dict[str, Any]],
    expected: str,
) -> None:
    incr = dlt.sources.incremental[int]("id", **incremental_kwargs)
    if bind_state is not None:
        _bind_state(incr, **bind_state)
    sql = _emit_sql(incr, column_name="id", sqlglot_type=to_sqlglot_type("bigint"))
    assert sql == expected


_TS_EPOCH = pendulum.datetime(2026, 1, 1, tz="UTC")
_TS_END = pendulum.datetime(2026, 1, 5, tz="UTC")


def _ts_incremental() -> Incremental[Any]:
    return dlt.sources.incremental[pendulum.DateTime](
        "created_at", initial_value=_TS_EPOCH, end_value=_TS_END
    )


def _ts_sqlglot_type(timezone: Optional[bool] = True) -> sge.DataType:
    # match what `_sqlglot_type_for_column` would produce for a tz-aware timestamp
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
            # naive (no +00:00) + truncated to precision=0
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
def test_timestamp_emission(
    caps_kwargs: Optional[dict[str, Any]],
    dialect: str,
    tz_aware_cursor: bool,
    must_contain: list[str],
    must_not_contain: list[str],
) -> None:
    if tz_aware_cursor:
        incr = _ts_incremental()
        sqlglot_type = _ts_sqlglot_type()
    else:
        incr = dlt.sources.incremental[pendulum.DateTime](
            "created_at",
            initial_value=pendulum.naive(2026, 1, 1),
            end_value=pendulum.naive(2026, 1, 5),
        )
        sqlglot_type = _ts_sqlglot_type(timezone=False)

    caps = _caps(**caps_kwargs) if caps_kwargs is not None else None
    sql = _emit_sql(incr, sqlglot_type=sqlglot_type, caps=caps, dialect=dialect)

    assert sql is not None
    for expected in must_contain:
        assert expected in sql, f"expected {expected!r} in {sql!r}"
    for unexpected in must_not_contain:
        assert unexpected not in sql, f"unexpected {unexpected!r} in {sql!r}"


@pytest.mark.parametrize(
    ("last_value_func", "range_start", "range_end", "expected"),
    [
        pytest.param(
            max,
            "closed",
            "open",
            '"id" >= CAST(2 AS BIGINT) AND "id" < CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="max-closed-open",
        ),
        pytest.param(
            max,
            "open",
            "closed",
            '"id" > CAST(2 AS BIGINT) AND "id" <= CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="max-open-closed",
        ),
        pytest.param(
            min,
            "closed",
            "open",
            '"id" <= CAST(2 AS BIGINT) AND "id" > CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="min-closed-open",
        ),
        pytest.param(
            min,
            "open",
            "closed",
            '"id" < CAST(2 AS BIGINT) AND "id" >= CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="min-open-closed",
        ),
    ],
)
def test_operator_matrix(
    last_value_func: Any, range_start: str, range_end: str, expected: str
) -> None:
    incr = dlt.sources.incremental[int](
        "id",
        initial_value=2,
        end_value=10,
        last_value_func=last_value_func,
        range_start=range_start,  # type: ignore[arg-type]
        range_end=range_end,  # type: ignore[arg-type]
    )
    sql = _emit_sql(incr, column_name="id", sqlglot_type=to_sqlglot_type("bigint"))
    assert sql == expected


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        pytest.param(
            "include",
            '"id" >= CAST(2 AS BIGINT) AND "id" < CAST(10 AS BIGINT) OR "id" IS NULL',
            id="include-or-is-null",
        ),
        pytest.param(
            "exclude",
            '"id" >= CAST(2 AS BIGINT) AND "id" < CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="exclude-and-is-not-null",
        ),
        pytest.param(
            "raise",
            '"id" >= CAST(2 AS BIGINT) AND "id" < CAST(10 AS BIGINT) AND NOT "id" IS NULL',
            id="raise-falls-back-to-is-not-null",
        ),
    ],
)
def test_on_cursor_value_missing_matrix(policy: str, expected: str) -> None:
    incr = dlt.sources.incremental[int](
        "id", initial_value=2, end_value=10, on_cursor_value_missing=policy  # type: ignore[arg-type]
    )
    sql = _emit_sql(incr, column_name="id", sqlglot_type=to_sqlglot_type("bigint"))
    assert sql == expected


def test_rejects_custom_last_value_func() -> None:
    incr = dlt.sources.incremental[int](
        "id", initial_value=2, end_value=10, last_value_func=lambda xs: xs[-1]
    )
    column_ref = sge.Column(this=sge.to_identifier("id", quoted=True))
    with pytest.raises(ValueError, match="cannot be pushed down to SQL"):
        _build_incremental_condition(incr, column_ref, sqlglot_type=None)
