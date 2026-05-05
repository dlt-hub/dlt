"""Marimo notebook with custom expose."""

import marimo

app = marimo.App(width="medium")

__expose__ = {"interface": "rest_api"}
