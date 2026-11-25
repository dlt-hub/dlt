"""Jobs that the example plugin rejects."""

from dlt.hub.run import job


@job
def rejected_job() -> str:
    return "rejected"


__all__ = ["rejected_job"]
