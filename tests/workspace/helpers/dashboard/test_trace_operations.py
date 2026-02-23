import pytest
import dlt
import marimo as mo

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.trace import (
    trace_overview,
    trace_execution_context,
    trace_steps_overview,
    trace_resolved_config_values,
    trace_step_details,
    build_trace_section,
)
from dlt._workspace.helpers.dashboard.utils.pipeline import sanitize_trace_for_display
from dlt._workspace.helpers.dashboard.utils.ui import dlt_table
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    NEVER_RAN_PIPELINE,
    NO_DESTINATION_PIPELINE,
    SYNC_EXCEPTION_PIPELINE,
    PIPELINES_WITH_LOAD,
)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_trace(pipeline: dlt.Pipeline):
    """Test trace overview with real trace data"""
    config = DashboardConfiguration()
    trace = pipeline.last_trace

    if pipeline.pipeline_name in [NEVER_RAN_PIPELINE, NO_DESTINATION_PIPELINE]:
        assert trace is None
        return

    # overview
    overview_result = trace_overview(config, trace)
    # check it can be rendered as table with marimo
    assert dlt_table(overview_result).text is not None

    assert {item["name"] for item in overview_result} == {
        "pipeline_name",
        "started_at",
        "finished_at",
        "transaction_id",
        "duration",
    }
    values_dict = {item["name"]: item["value"] for item in overview_result}
    assert values_dict["pipeline_name"] == pipeline.pipeline_name

    # execution context
    context_result = trace_execution_context(config, trace)
    # check it can be rendered as table with marimo
    assert dlt_table(context_result).text is not None

    assert len(context_result) == 7
    assert {item["name"] for item in context_result} == {
        "cpu",
        "os",
        "library",
        "run_context",
        "python",
        "ci_run",
        "exec_info",
    }
    # TODO: inspect values

    # steps overview
    steps_result = trace_steps_overview(config, trace)
    # check it can be rendered as table with marimo
    assert dlt_table(steps_result).text is not None

    if pipeline.pipeline_name == EXTRACT_EXCEPTION_PIPELINE:
        assert len(steps_result) == 1
        assert steps_result[0]["step"] == "extract"
    elif pipeline.pipeline_name == NORMALIZE_EXCEPTION_PIPELINE:
        assert len(steps_result) == 2
        assert steps_result[0]["step"] == "extract"
        assert steps_result[1]["step"] == "normalize"
    elif pipeline.pipeline_name == SYNC_EXCEPTION_PIPELINE:
        assert len(steps_result) == 0
    else:
        assert len(steps_result) == 3
        assert steps_result[0]["step"] == "extract"
        assert steps_result[1]["step"] == "normalize"
        assert steps_result[2]["step"] == "load"

    # TODO: inspect values of trace steps overview

    for item in steps_result:
        trace_step_details(config, trace, item["step"])
        # TODO: inspect trace step details

    # resolved config values (TODO: add at least one config value)
    config_result = trace_resolved_config_values(config, trace)
    # check it can be rendered as table with marimo
    assert dlt_table(config_result).text is not None


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_sanitize_trace_for_display(pipeline: dlt.Pipeline):
    """Test sanitizing trace for display"""
    trace = pipeline.last_trace
    sanitized = sanitize_trace_for_display(trace)
    assert sanitized is not None
    assert isinstance(sanitized, dict)
    # check it can be rendered with marimo
    assert mo.json(sanitized).text is not None


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_build_trace_section(pipeline: dlt.Pipeline):
    """Test building the full trace section with real pipeline data"""
    config = DashboardConfiguration()
    trace = pipeline.last_trace
    assert trace is not None

    # build the steps table that build_trace_section expects
    steps_data = trace_steps_overview(config, trace)
    trace_steps_table = dlt_table(steps_data, selection="multi", freeze_column="step")

    result = build_trace_section(config, pipeline, trace_steps_table)
    assert isinstance(result, list)
    assert len(result) > 0
    # should render without error
    assert mo.vstack(result).text is not None


def test_build_trace_section_no_trace(never_ran_pipline: dlt.Pipeline):
    """Test build_trace_section when pipeline has no trace"""
    config = DashboardConfiguration()
    result = build_trace_section(config, never_ran_pipline, None)
    assert isinstance(result, list)
    assert len(result) >= 1
    # should contain the no-trace warning
    rendered = mo.vstack(result).text
    assert "No local trace" in rendered
