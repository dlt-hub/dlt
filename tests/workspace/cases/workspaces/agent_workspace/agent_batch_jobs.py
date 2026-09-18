"""Plain jobs the agent watcher observes."""

from typing import Any, Dict

from dlt.hub.run import job, result


@job(trigger="0 8 * * *", expose={"tags": ["ingest"]})
def daily_ingest() -> Dict[str, Any]:
    """Ingest yesterday."""
    return result({"rows": 10}, type="etl_summary")


@job
def transform() -> str:
    return "transformed"
