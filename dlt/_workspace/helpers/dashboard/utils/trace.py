"""Trace introspection and rendering helpers."""

from typing import Any, Dict, List, cast

from dlt._workspace.helpers.dashboard.types import TNameValueItem

import marimo as mo

import dlt

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.formatters import (
    dict_to_table_items,
    humanize_datetime_values,
)
from dlt._workspace.helpers.dashboard.utils import pipeline as pipeline_utils
from dlt._workspace.helpers.dashboard.utils import ui
from dlt.pipeline.trace import PipelineTrace


def trace_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[TNameValueItem]:
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
) -> List[TNameValueItem]:
    """Get the execution context of a trace as name/value table items."""
    return dict_to_table_items(dict(trace.execution_context) or {})


def trace_steps_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, str]]:
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


def trace_step_details(
    c: DashboardConfiguration, trace: PipelineTrace, step_id: str
) -> List[mo.Html]:
    """Get the details of a step including table and job metrics."""
    _metrics_sections = [
        ("table_metrics", strings.trace_table_metrics_title),
        ("job_metrics", strings.trace_job_metrics_title),
    ]

    _result = []
    for step_obj in trace.steps:
        if step_obj.step == step_id:
            info_section = step_obj.asdict().get(f"{step_id}_info", {})
            for key, title_template in _metrics_sections:
                if key in info_section:
                    _result.append(
                        ui.title_and_subtitle(title_template.format(step_id), title_level=4)
                    )
                    metrics = [humanize_datetime_values(c, m) for m in info_section[key]]
                    _result.append(ui.dlt_table(metrics, freeze_column="table_name"))

    return _result


def build_trace_section(
    config: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    trace_steps_table: mo.ui.table,
) -> List[mo.Html]:
    """Build the trace section content: overview, execution context, steps, config, raw trace."""
    result: List[mo.Html] = []

    if _exception_section := pipeline_utils.exception_section(pipeline):
        result.extend(_exception_section)

    if not (dlt_trace := pipeline.last_trace):
        result.append(
            mo.callout(
                mo.md(strings.trace_no_trace_text),
                kind="warn",
            )
        )
        return result

    result.append(ui.title_and_subtitle(strings.trace_overview_title, title_level=3))
    result.append(ui.dlt_table(trace_overview(config, dlt_trace)))

    result.append(
        ui.title_and_subtitle(
            strings.trace_execution_context_title,
            strings.trace_execution_context_subtitle,
            title_level=3,
        )
    )
    result.append(ui.dlt_table(trace_execution_context(config, dlt_trace)))

    result.append(
        ui.title_and_subtitle(
            strings.trace_steps_overview_title,
            strings.trace_steps_overview_subtitle,
            title_level=3,
        )
    )
    result.append(trace_steps_table)

    for item in cast(List[Dict[str, str]], trace_steps_table.value):
        step_id = item["step"]
        result.append(
            ui.title_and_subtitle(
                strings.trace_step_details_title.format(step_id.capitalize()),
                title_level=3,
            )
        )
        result += trace_step_details(config, dlt_trace, step_id)

    result.append(
        ui.title_and_subtitle(
            strings.trace_resolved_config_title,
            strings.trace_resolved_config_subtitle,
            title_level=3,
        )
    )
    result.append(ui.dlt_table(trace_resolved_config_values(config, dlt_trace)))

    result.append(ui.title_and_subtitle(strings.trace_raw_trace_title, title_level=3))
    result.append(
        mo.accordion(
            {
                strings.trace_show_raw_trace_text: mo.json(
                    pipeline_utils.sanitize_trace_for_display(dlt_trace)
                )
            }
        )
    )

    return result
