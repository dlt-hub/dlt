"""Per-job freshness state store: tracks `prev_completed_run` for non-interval jobs."""

from datetime import datetime  # noqa: I251
from typing import Optional, Protocol

from dlt.common.time import ensure_pendulum_datetime_utc


class TJobFreshnessStore(Protocol):
    """Tracks the most recent successful work-window timestamp per job.

    Used by non-interval jobs as the source of the `refresh` signal: a job
    whose `prev_completed_run` is `None` is dispatched with `refresh=True`.
    State is mutable independently of run history — `--refresh` and policy
    propagation reset entries here without rewriting `runs_store`.
    """

    def get_prev_completed_run(self, job_ref: str) -> Optional[datetime]:
        """Return the most recent completed-run timestamp for `job_ref`, or `None`."""
        ...

    def set_prev_completed_run(self, job_ref: str, value: Optional[datetime]) -> None:
        """Set or clear the timestamp for `job_ref`. Passing `None` clears it."""
        ...

    def clear_prev_completed_run(self, job_ref: str) -> None:
        """Clear the timestamp for `job_ref`. Equivalent to `set_prev_completed_run(ref, None)`."""
        ...

    def close(self) -> None:
        """Release any resources held by the store."""
        ...


class DuckDBJobFreshnessStore:
    """Tracks `prev_completed_run` per job using in-memory DuckDB."""

    def __init__(self) -> None:
        import duckdb

        self._conn = duckdb.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE job_freshness ("
            "  job_ref VARCHAR PRIMARY KEY,"
            "  prev_completed_run TIMESTAMPTZ"
            ")"
        )

    def get_prev_completed_run(self, job_ref: str) -> Optional[datetime]:
        """Return the timestamp for `job_ref`, or `None` if absent or cleared."""
        rows = self._conn.execute(
            "SELECT prev_completed_run FROM job_freshness WHERE job_ref = ?",
            [job_ref],
        ).fetchall()
        if not rows or rows[0][0] is None:
            return None
        return ensure_pendulum_datetime_utc(rows[0][0])

    def set_prev_completed_run(self, job_ref: str, value: Optional[datetime]) -> None:
        """Upsert the timestamp for `job_ref`. `None` clears it."""
        self._conn.execute(
            "INSERT INTO job_freshness (job_ref, prev_completed_run) VALUES (?, ?)"
            " ON CONFLICT (job_ref) DO UPDATE SET prev_completed_run = EXCLUDED.prev_completed_run",
            [job_ref, value],
        )

    def clear_prev_completed_run(self, job_ref: str) -> None:
        """Clear the timestamp for `job_ref`."""
        self.set_prev_completed_run(job_ref, None)

    def close(self) -> None:
        self._conn.close()
