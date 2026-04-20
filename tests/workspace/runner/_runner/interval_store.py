"""Interval completion store protocol and DuckDB implementation."""

from typing import List, Protocol

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TTimeInterval as TInterval

from dlt._workspace.deployment.interval import sort_and_coalesce


class TIntervalStore(Protocol):
    """Interval completion tracking for jobs.

    Implementations store half-open `[start, end)` intervals per `job_ref`
    and answer coverage queries. Intervals are `Tuple[datetime, datetime]`.
    """

    def mark_interval_completed(self, job_ref: str, interval: TInterval) -> None:
        """Record `interval` as completed for `job_ref`."""
        ...

    def is_interval_completed(self, job_ref: str, interval: TInterval) -> bool:
        """Check if `interval` is fully covered by completed intervals."""
        ...

    def get_completed_intervals(self, job_ref: str, overall: TInterval) -> List[TInterval]:
        """Return completed intervals within `overall`, sorted and coalesced."""
        ...

    def invalidate_interval(self, job_ref: str, interval: TInterval) -> None:
        """Remove all completed data overlapping `interval`."""
        ...

    def close(self) -> None:
        """Release any resources held by the store."""
        ...


class DuckDBIntervalStore:
    """Tracks completed job intervals using in-memory DuckDB."""

    def __init__(self) -> None:
        import duckdb

        self._conn = duckdb.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE completed_intervals ("
            "  job_ref VARCHAR,"
            "  start_ts TIMESTAMPTZ,"
            "  end_ts TIMESTAMPTZ"
            ")"
        )

    def mark_interval_completed(self, job_ref: str, interval: TInterval) -> None:
        """Record an interval as completed."""
        self._conn.execute(
            "INSERT INTO completed_intervals VALUES (?, ?, ?)",
            [job_ref, interval[0], interval[1]],
        )

    def is_interval_completed(self, job_ref: str, interval: TInterval) -> bool:
        """Check if [start, end) is fully covered by completed intervals."""
        start, end = interval
        rows = self._conn.execute(
            "SELECT start_ts, end_ts FROM completed_intervals "
            "WHERE job_ref = ? AND end_ts > ? AND start_ts < ? "
            "ORDER BY start_ts",
            [job_ref, start, end],
        ).fetchall()
        if not rows:
            return False
        if rows[0][0] > start:
            return False
        merged_end = rows[0][1]
        for row in rows[1:]:
            if row[0] > merged_end:
                return False
            merged_end = max(merged_end, row[1])
        return bool(merged_end >= end)

    def get_completed_intervals(self, job_ref: str, overall: TInterval) -> List[TInterval]:
        """Return completed intervals within overall, sorted and coalesced."""
        rows = self._conn.execute(
            "SELECT start_ts, end_ts FROM completed_intervals "
            "WHERE job_ref = ? AND end_ts > ? AND start_ts < ? "
            "ORDER BY start_ts",
            [job_ref, overall[0], overall[1]],
        ).fetchall()
        raw: List[TInterval] = [
            (ensure_pendulum_datetime_utc(r[0]), ensure_pendulum_datetime_utc(r[1])) for r in rows
        ]
        return sort_and_coalesce(raw)

    def invalidate_interval(self, job_ref: str, interval: TInterval) -> None:
        """Remove all completed data overlapping interval."""
        self._conn.execute(
            "DELETE FROM completed_intervals WHERE job_ref = ? AND end_ts > ? AND start_ts < ?",
            [job_ref, interval[0], interval[1]],
        )

    def close(self) -> None:
        self._conn.close()
