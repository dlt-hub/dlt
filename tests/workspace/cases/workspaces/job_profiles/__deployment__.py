"""Deployment that differs per environment."""

import dlt

from jobs import (  # type: ignore[import-not-found]
    admin,
    api,
    ci_only,
    declared,
    editor,
    freshness,
    nightly,
    reader,
    reporting,
    triage,
    writer,
)

__all__ = ["nightly", "api", "freshness", "editor", "triage", "writer"]

# jobs that declare profiles of `prod` cannot be deployed to `ci`
if dlt.current.workspace().environment == "ci":
    __all__.append("ci_only")
else:
    __all__ += ["declared", "reporting", "admin", "reader"]
