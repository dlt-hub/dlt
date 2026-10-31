"""Batch jobs for launcher tests."""

import dlt
from dlt.hub.run import job


@job
def backfill():
    """Backfill historical data."""
    return "backfill_done"


@job
def auto_refresh_probe():
    """Reports refresh mode of a pipeline created in the job."""
    p = dlt.pipeline("auto_refresh_probe")
    return f"refresh={p.refresh}"


@job(trigger=[backfill.success])
def transform():
    """Transform after backfill."""
    return "transformed"
