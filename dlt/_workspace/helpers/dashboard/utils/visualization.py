"""Pipeline execution timeline visualization and load package status badges."""

from typing import Dict, List, Optional, Tuple, NamedTuple, Union, cast, get_args

from dlt.common.pendulum import datetime

import marimo as mo

from dlt.common.pendulum import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.storages import LoadPackageInfo
from dlt.common.storages.load_package import PackageStorage

from dlt._workspace.helpers.dashboard.const import (
    LOAD_PACKAGE_STATUS_COLORS,
    PENDING_LOAD_STATUSES,
    PIPELINE_RUN_STEP_COLORS,
    TPipelineRunStatus,
    TVisualPipelineStep,
    VISUAL_PIPELINE_STEPS,
)
from dlt._workspace.helpers.dashboard.utils.formatters import format_duration
from dlt._workspace.helpers.dashboard.utils.ui import dlt_table
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace


class PipelineStepData(NamedTuple):
    step: TVisualPipelineStep
    duration_ms: float
    failed: bool


def badge_html(text: str, color: str, size: str = "small") -> str:
    """Build a colored badge HTML element."""
    return f'<div class="status-badge status-badge-{color}"><{size}>{text}</{size}></div>'


def migration_badge(count: int) -> str:
    """Build migration badge HTML using CSS classes."""
    if count == 0:
        return ""
    return badge_html(f"{count} dataset migration(s)", "yellow", "strong")


def status_badge(status: TPipelineRunStatus) -> str:
    """Build status badge HTML using CSS classes."""
    return badge_html(status, "green" if status == "succeeded" else "red", "strong")


def pipeline_execution_html(
    transaction_id: str,
    status: TPipelineRunStatus,
    steps_data: List[PipelineStepData],
    migrations_count: int = 0,
    finished_at: Optional[datetime] = None,
) -> mo.Html:
    """Build an HTML visualization for a pipeline execution using CSS classes."""
    total_ms = sum(step.duration_ms for step in steps_data)
    if total_ms == 0:
        total_ms = 1.0  # avoid division by zero when all steps complete instantly
    last = len(steps_data) - 1

    # Build the general info of the execution
    relative_time = ""
    if finished_at:
        time_ago = pendulum.instance(finished_at).diff_for_humans()
        relative_time = f"<div><small>Executed: <strong>{time_ago}</strong></small></div>"

    general_info = f"""
    <div><small>Last execution ID: <strong>{transaction_id[:8]}</strong></small></div>
    <div><small>Total time: <strong>{format_duration(total_ms)}</strong></small></div>
    {relative_time}
    """

    # Build the pipeline execution timeline bar and labels
    segments, labels = [], []
    for i, step in enumerate(steps_data):
        percentage = step.duration_ms / total_ms * 100
        color = PIPELINE_RUN_STEP_COLORS.get(step.step)
        radius = (
            "6px"
            if i == 0 and i == last
            else "6px 0 0 6px" if i == 0 else "0 6px 6px 0" if i == last else "0"
        )
        segments.append(
            '<div class="pipeline-execution-timeline-segment" '
            f'style="width:{percentage}%;background-color:{color};border-radius:{radius};"></div>'
        )
        labels.append(
            f'<span><span style="color:{color};">●</span> '
            f"{step.step.capitalize()} <strong>{format_duration(step.duration_ms)}</strong> </span>"
        )

    html = f"""
    <div class="pipeline-execution-container">
        <!-- Main 3-column flex container -->
        <div class="pipeline-execution-layout">

            <!-- LEFT COLUMN: Run ID, Total time -->
            <div class="pipeline-execution-info paragraph">
                {general_info}
            </div>

            <!-- CENTER COLUMN: Timeline bar and legend -->
            <div class="pipeline-execution-timeline">
                <div class="pipeline-execution-timeline-bar">
                    {''.join(segments)}
                </div>
                <div class="pipeline-execution-labels">
                    {''.join(labels)}
                </div>
            </div>

            <!-- RIGHT COLUMN: Status badges -->
            <div class="pipeline-execution-badges">
                {migration_badge(migrations_count)}
                {status_badge(status)}
            </div>
        </div>
    </div>
    """
    return mo.Html(html)


def get_steps_data_and_status(
    trace_steps: List[PipelineStepTrace],
) -> Tuple[List[PipelineStepData], TPipelineRunStatus]:
    """Get trace steps data and the status of the corresponding pipeline execution."""
    steps_data: List[PipelineStepData] = []
    any_step_failed: bool = False

    for step in trace_steps:
        if step.step_exception is not None:
            any_step_failed = True

        if step.step not in get_args(TVisualPipelineStep) or not step.finished_at:
            continue

        duration_ms = (step.finished_at - step.started_at).total_seconds() * 1000
        steps_data.append(
            PipelineStepData(
                step=cast(TVisualPipelineStep, step.step),
                duration_ms=duration_ms,
                failed=step.step_exception is not None,
            )
        )
    status: TPipelineRunStatus = "failed" if any_step_failed else "succeeded"
    return steps_data, status


def get_migrations_count(last_load_info: LoadInfo) -> int:
    """Count the number of unique migrations (schema versions) from load packages."""
    migrations_count: int = 0
    seen_schema_hashes = set()
    for package in last_load_info.load_packages:
        if len(package.schema_update) > 0:
            if package.schema_hash not in seen_schema_hashes:
                migrations_count += 1
                seen_schema_hashes.add(package.schema_hash)
    return migrations_count


def pipeline_execution_visualization(trace: PipelineTrace) -> Optional[mo.Html]:
    """Create a visual timeline of pipeline run showing extract, normalize and load steps."""
    steps_data, status = get_steps_data_and_status(trace.steps)
    migrations_count = get_migrations_count(trace.last_load_info) if trace.last_load_info else 0

    return pipeline_execution_html(
        trace.transaction_id,
        status,
        steps_data,
        migrations_count,
        trace.finished_at,
    )


def collect_load_packages_from_trace(
    trace: PipelineTrace,
) -> List[LoadPackageInfo]:
    """Collect all unique load packages from all steps."""
    packages_by_load_id: Dict[str, LoadPackageInfo] = {}

    for step in trace.steps:
        if step.step in VISUAL_PIPELINE_STEPS and step.step_info and step.step_info.load_packages:
            for package in step.step_info.load_packages:
                packages_by_load_id[package.load_id] = package

    return list(packages_by_load_id.values())


def load_package_status_labels(trace: PipelineTrace) -> mo.ui.table:
    """Build a table of load package status badges for each package in the trace.

    For each package, determines its visual status badge based on whether the
    package is partially loaded, pending, or in a final state.
    """
    packages = collect_load_packages_from_trace(trace)
    result: List[Dict[str, Union[str, mo.Html]]] = []

    for package in packages:
        is_partial = PackageStorage.is_package_partially_loaded(package)
        badge_color_key = "red" if is_partial else LOAD_PACKAGE_STATUS_COLORS.get(package.state)
        if is_partial:
            badge_text = f"partially {package.state}"
        elif package.state in PENDING_LOAD_STATUSES:
            badge_text = PENDING_LOAD_STATUSES.get(package.state)
        elif package.state == "new":
            badge_text = "discarded"
        else:
            badge_text = package.state

        status_html = badge_html(badge_text, badge_color_key, "strong")
        result.append(
            {
                "load_id": package.load_id,
                "status": mo.Html(status_html),
            }
        )

    return dlt_table(
        result,
        pagination=True,
        show_download=False,
        freeze_column=None,
    )
