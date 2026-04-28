"""Deployment with a plain local Python module."""

from tests.workspace.cases.runtime_workspace.batch_jobs import backfill
import tests.workspace.cases.runtime_workspace.etl_script as etl_script

__all__ = ["backfill", "etl_script"]
