"""Deployment that exposes jobs lazily via module-level __getattr__ (PEP 562).

The names in ``__all__`` are not present in the module ``__dict__``; they are
resolved on access through ``__getattr__``. This mirrors modules that build
their public surface lazily.
"""

from typing import Any

from tests.workspace.cases.runtime_workspace import batch_jobs

__all__ = ["backfill", "daily_ingest"]  # noqa: F822 -- resolved via __getattr__

_LAZY = {"backfill": batch_jobs.backfill, "daily_ingest": batch_jobs.daily_ingest}


def __getattr__(name: str) -> Any:
    try:
        return _LAZY[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
