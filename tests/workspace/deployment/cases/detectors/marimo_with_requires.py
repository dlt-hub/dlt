"""Marimo notebook with resource requirements."""

import marimo

app = marimo.App(width="medium")

__require__ = {"dependency_groups": ["heavy-ml"], "machine": "gpu-a100"}
