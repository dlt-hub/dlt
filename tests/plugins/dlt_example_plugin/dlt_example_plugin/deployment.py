"""Jobs that the example plugin runs on its provider."""

from dlt.hub.run import job


@job
def example_job() -> str:
    return "done"


__all__ = ["example_job"]
