"""Deployment manifest exposing a function-based @job, a @pipeline_run job, and a plain module."""

import dlt
from dlt._workspace.deployment.decorators import job, pipeline_run

import plain  # type: ignore[import-not-found]


@job(trigger="schedule:0 2 * * *")
def batch_one() -> None:
    pass


p = dlt.pipeline("fruitshop", destination="duckdb")


@pipeline_run(p, trigger="schedule:0 3 * * *")
def load_fruitshop() -> None:
    pass


__all__ = ["batch_one", "load_fruitshop", "plain"]
