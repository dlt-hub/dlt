"""Deployment that includes a non-job item in __all__."""

from tests.workspace.cases.runtime_workspace.batch_jobs import backfill
from tests.workspace.cases.runtime_workspace.plain_module import helper

__all__ = ["backfill", "helper"]
