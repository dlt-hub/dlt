"""Trace introspection and rendering helpers."""

from typing import Any, Dict, List

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.formatters import (
    dict_to_table_items,
    humanize_datetime_values,
)
from dlt._workspace.helpers.dashboard.utils import ui
from dlt.pipeline.trace import PipelineTrace


def trace_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, Any]]:
    """Get the overview of a trace as name/value table items."""
    return dict_to_table_items(
        humanize_datetime_values(
            c,
            {
                "transaction_id": trace.transaction_id,
                "pipeline_name": trace.pipeline_name,
                "started_at": trace.started_at,
                "finished_at": trace.finished_at,
            },
        )
    )


def trace_execution_context(
    c: DashboardConfiguration, trace: PipelineTrace
) -> List[Dict[str, Any]]:
    """Get the execution context of a trace as name/value table items."""
    return dict_to_table_items(dict(trace.execution_context) or {})


def trace_steps_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, Any]]:
    """Get the steps overview of a trace."""
    result = []
    for step_obj in trace.steps:
        if step_obj.step == "run":
            continue
        # NOTE: use typing and not asdict here
        step = step_obj.asdict()
        step = humanize_datetime_values(c, step)
        step_dict = {
            k: step[k] for k in ["step", "started_at", "finished_at", "duration"] if k in step
        }
        step_dict["result"] = "failed" if step.get("step_exception") else "completed"
        result.append(step_dict)
    return result


def trace_resolved_config_values(
    c: DashboardConfiguration, trace: PipelineTrace
) -> List[Dict[str, Any]]:
    """Get the resolved config values of a trace."""
    return [v.asdict() for v in trace.resolved_config_values]  # type: ignore[misc]


def trace_step_details(c: DashboardConfiguration, trace: PipelineTrace, step_id: str) -> List[Any]:
    """Get the details of a step including table and job metrics."""
    _result = []
    for step_obj in trace.steps:
        if step_obj.step == step_id:
            step = step_obj.asdict()
            info_section = step.get(f"{step_id}_info", {})
            if "table_metrics" in info_section:
                _result.append(
                    ui.title_and_subtitle(
                        f"{step_id} table metrics",
                        title_level=4,
                    )
                )
                table_metrics = info_section.get("table_metrics", [])
                table_metrics = [humanize_datetime_values(c, t) for t in table_metrics]
                _result.append(ui.dlt_table(table_metrics, freeze_column="table_name"))

            if "job_metrics" in info_section:
                _result.append(
                    ui.title_and_subtitle(
                        f"{step_id} job metrics",
                        title_level=4,
                    )
                )
                job_metrics = info_section.get("job_metrics", [])
                job_metrics = [humanize_datetime_values(c, j) for j in job_metrics]
                _result.append(ui.dlt_table(job_metrics, freeze_column="table_name"))

    return _result
