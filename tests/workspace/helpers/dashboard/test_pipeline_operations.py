import pytest
import marimo as mo
import dlt
from typing import Set

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    _get_steps_data_and_status,
    get_pipeline,
    pipeline_details,
    build_exception_section,
    get_local_data_path,
    get_source_and_resource_state_for_table,
    remote_state_details,
    TPipelineRunStatus,
    TVisualPipelineStep,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    EXTRACT_EXCEPTION_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
    NO_DESTINATION_PIPELINE,
    PIPELINES_WITH_EXCEPTIONS,
    PIPELINES_WITH_LOAD,
    SUCCESS_PIPELINE_FILESYSTEM,
    SUCCESS_PIPELINE_DUCKDB,
)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_build_exception_section(pipeline: dlt.Pipeline):
    if pipeline.pipeline_name in PIPELINES_WITH_EXCEPTIONS:
        assert "Show full stacktrace" in build_exception_section(pipeline)[0].text
    else:
        assert not build_exception_section(pipeline)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_local_data_path(pipeline: dlt.Pipeline):
    if pipeline.pipeline_name in [LOAD_EXCEPTION_PIPELINE, NO_DESTINATION_PIPELINE]:
        # custom destination does not support local data path
        assert get_local_data_path(pipeline) is None
    else:
        assert get_local_data_path(pipeline)


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_source_and_resource_state_for_table(pipeline: dlt.Pipeline):
    """Test getting source and resource state for a table"""
    table = pipeline.default_schema.tables["purchases"]
    resource_name, source_state, resource_state = get_source_and_resource_state_for_table(
        table, pipeline, pipeline.default_schema_name
    )
    assert resource_name
    assert source_state == {}
    assert resource_state.get("incremental").get("id") is not None

    # check it can be rendered with marimo
    assert mo.json(resource_state).text
    assert mo.json(source_state).text


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipeline(pipeline: dlt.Pipeline):
    """Test getting a real pipeline by name"""
    pipeline = get_pipeline(pipeline.pipeline_name, pipeline.pipelines_dir)

    assert pipeline.pipeline_name == pipeline.pipeline_name
    assert pipeline.dataset_name == pipeline.dataset_name


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_pipeline_details(pipeline, temp_pipelines_dir):
    """Test getting pipeline details from a real pipeline"""
    config = DashboardConfiguration()
    result = pipeline_details(config, pipeline, temp_pipelines_dir)

    assert isinstance(result, list)
    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert len(result) == 9
    elif pipeline.pipeline_name in [LOAD_EXCEPTION_PIPELINE, "normalize_exception_pipeline"]:
        # custom destination does not support remote data info
        assert len(result) == 8
    else:
        # no remote data info
        assert len(result) == 7

    # Convert to dict for easier testing
    details_dict = {item["name"]: item["value"] for item in result}

    assert details_dict["pipeline_name"] == pipeline.pipeline_name
    if pipeline.pipeline_name == NO_DESTINATION_PIPELINE:
        assert details_dict["destination"] == "No destination set"
    elif pipeline.pipeline_name == SUCCESS_PIPELINE_FILESYSTEM:
        assert details_dict["destination"] == "filesystem (dlt.destinations.filesystem)"
    elif pipeline.pipeline_name == LOAD_EXCEPTION_PIPELINE:
        assert details_dict["destination"] == "dummy (dlt.destinations.dummy)"
    else:
        assert details_dict["destination"] == "duckdb (dlt.destinations.duckdb)"
    assert details_dict["dataset_name"] == pipeline.dataset_name


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_remote_state_details(pipeline: dlt.Pipeline):
    remote_state = remote_state_details(pipeline)
    # check it can be rendered as table with marimo
    assert mo.ui.table(remote_state).text is not None


@pytest.mark.parametrize(
    "pipeline, expected_steps, expected_status",
    [
        (SUCCESS_PIPELINE_DUCKDB, {"extract", "normalize", "load"}, "succeeded"),
        (SUCCESS_PIPELINE_FILESYSTEM, {"extract", "normalize", "load"}, "succeeded"),
        (EXTRACT_EXCEPTION_PIPELINE, {"extract"}, "failed"),
        (LOAD_EXCEPTION_PIPELINE, {"extract", "normalize", "load"}, "failed"),
    ],
    indirect=["pipeline"],
)
def test_get_steps_data_and_status(
    pipeline: dlt.Pipeline,
    expected_steps: Set[TVisualPipelineStep],
    expected_status: TPipelineRunStatus,
) -> None:
    """Test getting steps data and the pipeline execution status from trace"""
    trace = pipeline.last_trace

    steps_data, status = _get_steps_data_and_status(trace.steps)
    assert len(steps_data) == len(expected_steps)
    assert status == expected_status

    assert all(step.duration_ms > 0 for step in steps_data)
    if expected_status == "succeeded":
        assert all(step.failed is False for step in steps_data)
    else:
        assert any(step.failed is True for step in steps_data)

    assert set([step.step for step in steps_data]) == expected_steps
