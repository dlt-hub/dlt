"""Batch-only deployment."""

from tests.workspace.cases.runtime_workspace.batch_jobs import (
    backfill,
    daily_ingest,
    transform,
)

__all__ = ["backfill", "daily_ingest", "transform"]
