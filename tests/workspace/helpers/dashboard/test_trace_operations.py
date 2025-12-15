import pytest
import dlt
import marimo as mo

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    trace_overview,
    trace_execution_context,
    trace_steps_overview,
    trace_resolved_config_values,
    trace_step_details,
    sanitize_trace_for_display,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    NEVER_RAN_PIPELINE,
    NO_DESTINATION_PIPELINE,
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
    result = trace_overview(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    assert {item["name"] for item in result} == {
        "pipeline_name",
        "started_at",
        "finished_at",
        "transaction_id",
        "duration",
    }
    values_dict = {item["name"]: item["value"] for item in result}
    assert values_dict["pipeline_name"] == pipeline.pipeline_name

    # execution context
    result = trace_execution_context(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    assert len(result) == 7
    assert {item["name"] for item in result} == {
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
    result = trace_steps_overview(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    if pipeline.pipeline_name == EXTRACT_EXCEPTION_PIPELINE:
        assert len(result) == 1
        assert result[0]["step"] == "extract"
    elif pipeline.pipeline_name == NORMALIZE_EXCEPTION_PIPELINE:
        assert len(result) == 2
        assert result[0]["step"] == "extract"
        assert result[1]["step"] == "normalize"
    else:
        assert len(result) == 3
        assert result[0]["step"] == "extract"
        assert result[1]["step"] == "normalize"
        assert result[2]["step"] == "load"

    # TODO: inspect values of trace steps overview

    for item in result:
        result = trace_step_details(config, trace, item["step"])
        # TODO: inspect trace step details

    # resolved config values (TODO: add at least one config value)
    result = trace_resolved_config_values(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_sanitize_trace_for_display(pipeline: dlt.Pipeline):
    """Test sanitizing trace for display"""
    trace = pipeline.last_trace
    sanitized = sanitize_trace_for_display(trace)
    assert sanitized is not None
    assert isinstance(sanitized, dict)
    # check it can be rendered with marimo
    assert mo.json(sanitized).text is not None
