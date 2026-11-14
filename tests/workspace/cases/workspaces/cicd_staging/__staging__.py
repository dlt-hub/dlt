"""Staging module: proxies `__deployment__` with copies of its jobs. CI deploys it to another
remote workspace with `stg.config.toml` renamed to `prod.config.toml`."""

import copy
from typing import Any

import __deployment__  # type: ignore[import-not-found]
from notebooks import migration_notebook  # type: ignore[import-not-found]  # noqa: F401


def _patch_job_for_staging(job_original: Any, module_name: str) -> Any:
    """A copy of the job that belongs to this module and runs on demand only."""
    job = copy.deepcopy(job_original)
    job.section = module_name
    job._f.__module__ = module_name
    job.trigger = []
    return job


def __getattr__(name: str) -> Any:
    job = getattr(__deployment__, name)
    if getattr(job, "job_type", None) == "batch":
        job = _patch_job_for_staging(job, __name__)
    return job


__all__ = [*__deployment__.__all__, "migration_notebook"]
