"""Batch jobs for the test workspace."""

import dlt
from dlt._workspace.deployment.decorators import job


@job(timeout="24h")
def backfill():
    """Backfill historical data."""
    return "backfill_done"


@job(trigger="0 8 * * *", timeout="4h")
def daily_ingest():
    return "ingested"


@job(trigger=[backfill.success, daily_ingest.success])
def transform():
    """Transform ingested data."""
    return "transformed"


@job(starred=True, tags=["ops"])
def maintenance(cleanup_days=dlt.config.value):
    """Run maintenance tasks."""
    pass
