import tempfile
from pathlib import Path
import os

import pytest
import dlt

from dlt._workspace.cli import utils as cli_utils
from dlt.pipeline.trace import TRACE_FILE_NAME
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    NEVER_RAN_PIPELINE,
    NO_DESTINATION_PIPELINE,
)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipelines(pipeline: dlt.Pipeline):
    """Test getting local pipelines"""
    pipelines_dir, pipelines = cli_utils.list_local_pipelines(pipeline.pipelines_dir)
    assert pipelines_dir == pipeline.pipelines_dir
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == pipeline.pipeline_name


def test_get_local_pipelines_with_temp_dir(temp_pipelines_dir):
    """Test getting local pipelines with temporary directory"""
    pipelines_dir, pipelines = cli_utils.list_local_pipelines(temp_pipelines_dir)

    assert pipelines_dir == temp_pipelines_dir
    assert len(pipelines) == 3  # success_pipeline_1, success_pipeline_2, _dlt_internal

    # Should be sorted by timestamp (descending)
    pipeline_names = [p["name"] for p in pipelines]
    assert "success_pipeline_2" in pipeline_names
    assert "success_pipeline_1" in pipeline_names
    assert "_dlt_internal" in pipeline_names

    # Check timestamps are present
    for pipeline in pipelines:
        assert "timestamp" in pipeline
        assert isinstance(pipeline["timestamp"], (int, float))


def test_get_local_pipelines_empty_dir():
    """Test getting local pipelines from empty directory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipelines_dir, pipelines = cli_utils.list_local_pipelines(temp_dir)

        assert pipelines_dir == temp_dir
        assert pipelines == []


def test_get_local_pipelines_nonexistent_dir():
    """Test getting local pipelines from nonexistent directory"""
    nonexistent_dir = "/nonexistent/directory"
    pipelines_dir, pipelines = cli_utils.list_local_pipelines(nonexistent_dir)

    assert pipelines_dir == nonexistent_dir
    assert pipelines == []


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipeline_last_run(pipeline: dlt.Pipeline):
    """Test getting the last run of a pipeline"""
    if pipeline.pipeline_name in [NEVER_RAN_PIPELINE, NO_DESTINATION_PIPELINE]:
        assert (
            cli_utils.get_pipeline_trace_mtime(pipeline.pipelines_dir, pipeline.pipeline_name) == 0
        )
    else:
        assert (
            cli_utils.get_pipeline_trace_mtime(pipeline.pipelines_dir, pipeline.pipeline_name)
            > 1000000
        )


def test_integration_get_local_pipelines_with_sorting(temp_pipelines_dir):
    """Test integration scenario with multiple pipelines sorted by timestamp"""
    pipelines_dir, pipelines = cli_utils.list_local_pipelines(
        temp_pipelines_dir, sort_by_trace=True
    )

    assert pipelines_dir == temp_pipelines_dir
    assert len(pipelines) == 3

    # Should be sorted by timestamp (descending - most recent first)
    timestamps = [p["timestamp"] for p in pipelines]
    assert timestamps == sorted(timestamps, reverse=True)

    # Verify the most recent pipeline is first
    most_recent = pipelines[0]
    assert most_recent["name"] == "success_pipeline_2"
    assert most_recent["timestamp"] == 2000000
