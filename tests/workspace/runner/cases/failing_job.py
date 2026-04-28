"""A job that always fails."""

from dlt._workspace.deployment.decorators import job


@job
def fail():
    raise RuntimeError("intentional failure")
