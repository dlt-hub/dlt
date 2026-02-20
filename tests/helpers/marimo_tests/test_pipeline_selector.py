import asyncio

import marimo

from dlt.helpers.marimo import _pipeline_selector, pipeline_selector, render


def test_cell_pipeline_locations():
    _, defs = _pipeline_selector.pipeline_locations.run()

    assert "pipelines_locations" in defs
    assert isinstance(defs["pipelines_locations"], dict)


def test_cell_pipeline_selector():
    _, defs = _pipeline_selector.pipeline_selector.run()

    assert "pipeline_selector" in defs
    assert isinstance(defs["pipeline_selector"], marimo.ui.dropdown)


def test_cell_outputs():
    _, defs = _pipeline_selector.outputs.run()

    assert "pipeline_name" in defs
    assert isinstance(defs["pipeline_name"], str)
    assert "pipeline_path" in defs
    assert isinstance(defs["pipeline_path"], str)


def test_app_variables():
    expected_variables = [
        "pipeline_selector",
        "pipelines_locations",
        "pipeline_name",
        "pipeline_path",
    ]

    _, defs = _pipeline_selector.app.run()

    assert all(v in defs for v in expected_variables)
