"""Marimo notebook with resource requirements and the access it needs."""

import marimo

app = marimo.App(width="medium")

__require__ = {"dependency_groups": ["heavy-ml"], "machine": "gpu-a100"}
__access__ = {"data": ["read", "write"]}
