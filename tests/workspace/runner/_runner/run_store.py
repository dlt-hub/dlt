"""Job run types, protocol, and DuckDB-backed store."""

from datetime import datetime  # noqa: I251
from typing import Literal, Optional, Protocol

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import NotRequired, TypedDict


TJobRunStatus = Literal["running", "completed", "failed"]


class TJobRun(TypedDict):
    """A single job execution record."""

    run_id: str
    job_ref: str
    trigger: str
    scheduled_at: datetime
    started_at: NotRequired[datetime]
    finished_at: NotRequired[datetime]
    status: TJobRunStatus
    interval_start: NotRequired[datetime]
    interval_end: NotRequired[datetime]


class TJobRunsStore(Protocol):
    """Protocol for tracking job executions."""

    def create_run(self, run: TJobRun) -> None:
        """Record a new job run."""
        ...

    def update_run(
        self,
        run_id: str,
        status: TJobRunStatus,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        """Update run status and timestamps."""
        ...

    def get_run(self, run_id: str) -> Optional[TJobRun]:
        """Return a run by its ID, or `None`."""
        ...

    def get_last_run(self, job_ref: str) -> Optional[TJobRun]:
        """Return the most recent run for a job, or `None`."""
        ...

    def close(self) -> None:
        """Release any resources held by the store."""
        ...


class DuckDBJobRunsStore:
    """Tracks job executions using in-memory DuckDB."""

    def __init__(self) -> None:
        import duckdb

        self._conn = duckdb.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE job_runs ("
            "  run_id VARCHAR PRIMARY KEY,"
            "  job_ref VARCHAR,"
            "  trigger VARCHAR,"
            "  scheduled_at TIMESTAMPTZ,"
            "  started_at TIMESTAMPTZ,"
            "  finished_at TIMESTAMPTZ,"
            "  status VARCHAR,"
            "  interval_start TIMESTAMPTZ,"
            "  interval_end TIMESTAMPTZ"
            ")"
        )

    def create_run(self, run: TJobRun) -> None:
        """Record a new job run."""
        self._conn.execute(
            "INSERT INTO job_runs"
            " (run_id, job_ref, trigger, scheduled_at, started_at, status,"
            "  interval_start, interval_end)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run["run_id"],
                run["job_ref"],
                run["trigger"],
                run["scheduled_at"],
                run.get("started_at"),
                run["status"],
                run.get("interval_start"),
                run.get("interval_end"),
            ],
        )

    def update_run(
        self,
        run_id: str,
        status: TJobRunStatus,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        """Update run status and timestamps."""
        if started_at and finished_at:
            self._conn.execute(
                "UPDATE job_runs SET status = ?, started_at = ?, finished_at = ? WHERE run_id = ?",
                [status, started_at, finished_at, run_id],
            )
        elif finished_at:
            self._conn.execute(
                "UPDATE job_runs SET status = ?, finished_at = ? WHERE run_id = ?",
                [status, finished_at, run_id],
            )
        elif started_at:
            self._conn.execute(
                "UPDATE job_runs SET status = ?, started_at = ? WHERE run_id = ?",
                [status, started_at, run_id],
            )
        else:
            self._conn.execute(
                "UPDATE job_runs SET status = ? WHERE run_id = ?",
                [status, run_id],
            )

    def get_run(self, run_id: str) -> Optional[TJobRun]:
        """Return a run by its ID, or None."""
        rows = self._conn.execute(self._SELECT + " WHERE run_id = ?", [run_id]).fetchall()
        return self._row_to_run(rows[0]) if rows else None

    def get_last_run(self, job_ref: str) -> Optional[TJobRun]:
        """Return the most recent run for a job, or None."""
        rows = self._conn.execute(
            self._SELECT + " WHERE job_ref = ? ORDER BY scheduled_at DESC LIMIT 1",
            [job_ref],
        ).fetchall()
        return self._row_to_run(rows[0]) if rows else None

    _SELECT = (
        "SELECT run_id, job_ref, trigger, scheduled_at, started_at,"
        " finished_at, status, interval_start, interval_end FROM job_runs"
    )

    @staticmethod
    def _row_to_run(r: tuple) -> TJobRun:  # type: ignore[type-arg]
        run: TJobRun = {
            "run_id": r[0],
            "job_ref": r[1],
            "trigger": r[2],
            "scheduled_at": ensure_pendulum_datetime_utc(r[3]),
            "status": r[6],
        }
        if r[4] is not None:
            run["started_at"] = ensure_pendulum_datetime_utc(r[4])
        if r[5] is not None:
            run["finished_at"] = ensure_pendulum_datetime_utc(r[5])
        if r[7] is not None:
            run["interval_start"] = ensure_pendulum_datetime_utc(r[7])
        if r[8] is not None:
            run["interval_end"] = ensure_pendulum_datetime_utc(r[8])
        return run

    def close(self) -> None:
        self._conn.close()
