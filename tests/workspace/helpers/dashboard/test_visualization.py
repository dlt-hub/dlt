import pytest
import dlt
import re
from typing import Set
from xml.etree.ElementTree import fromstring

from dlt._workspace.helpers.dashboard.utils.visualization import (
    pipeline_execution_visualization,
    pipeline_execution_html,
    get_migrations_count,
    get_steps_data_and_status,
    badge_html,
    migration_badge,
    status_badge,
    collect_load_packages_from_trace,
    PipelineStepData,
)
from dlt._workspace.helpers.dashboard.const import TPipelineRunStatus, TVisualPipelineStep
from tests.workspace.helpers.dashboard.example_pipelines import (
    SUCCESS_PIPELINE_DUCKDB,
    SUCCESS_PIPELINE_FILESYSTEM,
    EXTRACT_EXCEPTION_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
    PIPELINES_WITH_LOAD,
)


def assert_valid_html(html: str) -> None:
    """Assert that HTML has properly matched and closed tags using the XML parser."""
    fromstring(f"<root>{html}</root>")


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
def test_pipeline_execution_visualization(
    pipeline: dlt.Pipeline,
    expected_steps: Set[TVisualPipelineStep],
    expected_status: TPipelineRunStatus,
) -> None:
    """Test overall pipeline execution visualization logic"""

    trace = pipeline.last_trace

    html = pipeline_execution_visualization(trace)
    html_str = str(html.text)
    assert_valid_html(html_str)

    # Check for CSS class structure
    assert 'class="pipeline-execution-container"' in html_str
    assert 'class="pipeline-execution-layout"' in html_str
    assert 'class="pipeline-execution-timeline"' in html_str
    assert 'class="pipeline-execution-badges"' in html_str

    assert f"Last execution ID: <strong>{trace.transaction_id[:8]}</strong>" in html_str
    total_time_match = re.search(r"Total time: <strong>([\d.]+)(ms|s)?</strong>", html_str)
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
    migrations_count = get_migrations_count(trace.last_load_info) if trace.last_load_info else 0
    migration_badge_html = (
        f'<div class="status-badge status-badge-yellow"><strong>{migrations_count} dataset'
        " migration(s)</strong></div>"
    )
    if migrations_count != 0:
        assert migration_badge_html in html_str
    else:
        assert migration_badge_html not in html_str

    steps_data, _ = get_steps_data_and_status(trace.steps)
    for step_data in steps_data:
        duration_pattern = re.search(
            rf"{step_data.step.capitalize()}\s+(?:<strong>)?([\d.]+)(ms|s)?", html_str
        )
        assert duration_pattern is not None

    if "extract" in expected_steps:
        assert "var(--dlt-color-lime)" in html_str
    if "normalize" in expected_steps:
        assert "var(--dlt-color-aqua)" in html_str
    if "load" in expected_steps:
        assert "var(--dlt-color-pink)" in html_str


def test_badge_html():
    result = badge_html("test", "green")
    assert "test" in result
    assert "status-badge-green" in result
    assert "<small>" in result
    assert_valid_html(result)

    result = badge_html("error", "red", "strong")
    assert "error" in result
    assert "status-badge-red" in result
    assert "<strong>" in result
    assert_valid_html(result)


def test_migration_badge():
    assert migration_badge(0) == ""

    result = migration_badge(1)
    assert "1 dataset migration(s)" in result
    assert "status-badge-yellow" in result
    assert_valid_html(result)

    result = migration_badge(3)
    assert "3 dataset migration(s)" in result
    assert_valid_html(result)


def test_status_badge():
    result = status_badge("succeeded")
    assert "succeeded" in result
    assert "status-badge-green" in result
    assert_valid_html(result)

    result = status_badge("failed")
    assert "failed" in result
    assert "status-badge-red" in result
    assert_valid_html(result)


def test_pipeline_execution_html_valid_xml():
    """Verify generated HTML is well-formed (all tags properly opened and closed)."""
    steps = [
        PipelineStepData(step="extract", duration_ms=100, failed=False),
        PipelineStepData(step="normalize", duration_ms=200, failed=False),
        PipelineStepData(step="load", duration_ms=300, failed=False),
    ]
    html = pipeline_execution_html("abc12345", "succeeded", steps, migrations_count=2)
    assert_valid_html(html.text)

    # single step, failed, no migrations
    html = pipeline_execution_html("def67890", "failed", [PipelineStepData("extract", 50, True)])
    assert_valid_html(html.text)

    # zero duration steps
    html = pipeline_execution_html("00000000", "succeeded", [PipelineStepData("load", 0, False)])
    assert_valid_html(html.text)


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_collect_load_packages_from_trace(pipeline: dlt.Pipeline):
    """Test collecting load packages from trace steps"""
    trace = pipeline.last_trace
    assert trace is not None

    packages = collect_load_packages_from_trace(trace)
    assert isinstance(packages, list)
    assert len(packages) >= 1
    # each package should have a load_id
    assert all(hasattr(p, "load_id") for p in packages)
