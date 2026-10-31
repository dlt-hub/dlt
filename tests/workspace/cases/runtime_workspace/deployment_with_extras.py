"""Deployment that has __all__ alongside third-party imports, helpers, and constants."""

import os

# NOTE: pendulum stays here as a stand-in third-party import: the detector must not mistake it for
# a local module, which `test_use_all_false_with_all_present` asserts by name
import pendulum

from tests.workspace.cases.runtime_workspace.batch_jobs import backfill

__all__ = ["backfill"]

SOME_CONST = "hello"


def _private_helper() -> None:
    pass


def helper() -> str:
    return "ok"
