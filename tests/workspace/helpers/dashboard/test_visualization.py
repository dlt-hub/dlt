import pytest
import dlt
import re
from typing import Set

from dlt._workspace.helpers.dashboard.utils import (
    build_pipeline_execution_visualization,
    TPipelineRunStatus,
    TVisualPipelineStep,
    _get_migrations_count,
    _get_steps_data_and_status,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    SUCCESS_PIPELINE_DUCKDB,
    SUCCESS_PIPELINE_FILESYSTEM,
    EXTRACT_EXCEPTION_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
)


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
def test_build_pipeline_execution_visualization(
    pipeline: dlt.Pipeline,
    expected_steps: Set[TVisualPipelineStep],
    expected_status: TPipelineRunStatus,
) -> None:
    """Test overall pipeline execution visualization logic"""

    trace = pipeline.last_trace

    html = build_pipeline_execution_visualization(trace)
    html_str = str(html.text)

    # Check for CSS class structure
    assert 'class="pipeline-execution-container"' in html_str
    assert 'class="pipeline-execution-layout"' in html_str
    assert 'class="pipeline-execution-timeline"' in html_str
    assert 'class="pipeline-execution-badges"' in html_str

    assert f"Last execution ID: <strong>{trace.transaction_id[:8]}</strong>" in html_str
    total_time_match = re.search(
        r"<div>Total time: <strong>([\d.]+)(ms|s)?</strong></div>", html_str
    )
    assert total_time_match is not None

    # Check for status badge using CSS classes (not inline styles)
    status_badge_class = (
        "status-badge-green" if expected_status == "succeeded" else "status-badge-red"
    )
    assert (
        f'<div class="status-badge {status_badge_class}"><strong>{expected_status}</strong></div>'
        in html_str
    )

    # Check for migration badge using CSS classes (not inline styles)
    migrations_count = _get_migrations_count(trace.last_load_info) if trace.last_load_info else 0
    migration_badge = (
        f'<div class="status-badge status-badge-yellow"><strong>{migrations_count} dataset'
        " migration(s)</strong></div>"
    )
    if migrations_count != 0:
        assert migration_badge in html_str
    else:
        assert migration_badge not in html_str

    steps_data, _ = _get_steps_data_and_status(trace.steps)
    for step_data in steps_data:
        duration_pattern = re.search(rf"{step_data.step.capitalize()}\s+([\d.]+)(ms|s)?", html_str)
        assert duration_pattern is not None

    if "extract" in expected_steps:
        assert "var(--dlt-color-lime)" in html_str
    if "normalize" in expected_steps:
        assert "var(--dlt-color-aqua)" in html_str
    if "load" in expected_steps:
        assert "var(--dlt-color-pink)" in html_str
