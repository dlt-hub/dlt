"""Batch jobs for launcher tests."""

from dlt.hub.run import job


@job
def backfill():
    """Backfill historical data."""
    return "backfill_done"


@job(trigger=[backfill.success])
def transform():
    """Transform after backfill."""
    return "transformed"
