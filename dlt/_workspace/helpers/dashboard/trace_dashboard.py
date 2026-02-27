# /// script
# [tool.marimo.display]
# theme = "light"
# ///
# flake8: noqa: F841
# mypy: disable-error-code=no-untyped-def
"""
Standalone trace viewer - matches dlt_dashboard style, no dlt dependency.
Usage: marimo run trace_dashboard.py -- --trace-url <URL>
"""

import marimo

__generated_with = "0.13.9"
app = marimo.App(
    width="medium", app_title="dlt Trace Viewer", css_file="dlt_dashboard_styles.css"
)

with app.setup:
    from typing import Any, Dict, List, cast
    from datetime import datetime
    from urllib.request import urlopen
    import json
    import traceback

    import marimo as mo

    TRACE_SECTION_NAME = "trace_section"
    TRACE_TITLE = "Last Pipeline Run Trace"
    TRACE_SUBTITLE = (
        "An overview of the last load trace from the most recent successful run of the selected"
        " pipeline, if available."
    )
    TRACE_OVERVIEW_TITLE = "Trace Overview"
    TRACE_EXECUTION_CONTEXT_TITLE = "Execution Context"
    TRACE_EXECUTION_CONTEXT_SUBTITLE = (
        "Machine and execution information about the environment in which the trace was created."
    )
    TRACE_STEPS_OVERVIEW_TITLE = "Steps Overview"
    TRACE_STEPS_OVERVIEW_SUBTITLE = (
        "Select a step to view details about the step and its metrics."
    )
    TRACE_STEP_DETAILS_TITLE = "{} Details"
    TRACE_RESOLVED_CONFIG_TITLE = "Resolved Config Values"
    TRACE_RESOLVED_CONFIG_SUBTITLE = (
        "These are the configuration values that were resolved during the pipeline run. This"
        " includes injected configuration values."
    )
    TRACE_RAW_TRACE_TITLE = "Raw Trace"
    TRACE_NO_TRACE_TEXT = "No trace found for this pipeline."

    def section_marker(section_name: str, has_content: bool = False) -> mo.Html:
        content_class = "has-content" if has_content else ""
        return mo.Html(
            f'<div class="section-marker {content_class}" data-section="{section_name}" hidden"></div>'
        )

    def build_title_and_subtitle(
        title: str, subtitle: str = None, title_level: int = 2
    ) -> Any:
        _result = []
        if title:
            _result.append(mo.md(f"{'#' * title_level} {title}"))
        if subtitle:
            _result.append(mo.md(f"<small>{subtitle}</small>"))
        return mo.vstack(_result)

    def build_error_callout(
        message: str, code: str = None, traceback_string: str = None
    ) -> Any:
        stack_items = [mo.md(message)]
        if traceback_string:
            stack_items.append(
                mo.accordion(
                    {
                        "Show stacktrace": mo.ui.code_editor(
                            traceback_string, language="python", disabled=True
                        )
                    }
                )
            )
        return mo.callout(mo.vstack(stack_items), kind="warn")

    # =========================================================================
    # Trace Display Helpers
    # =========================================================================
    def _format_duration(started_at: Any, finished_at: Any) -> str:
        if not started_at or not finished_at:
            return "N/A"
        try:
            if isinstance(started_at, str):
                started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            if isinstance(finished_at, str):
                finished_at = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
            duration = finished_at - started_at
            total_seconds = duration.total_seconds()
            if total_seconds < 60:
                return f"{total_seconds:.2f}s"
            elif total_seconds < 3600:
                return f"{total_seconds / 60:.2f}m"
            else:
                return f"{total_seconds / 3600:.2f}h"
        except Exception:
            return "N/A"

    def trace_overview(trace: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {"property": "Pipeline Name", "value": trace.get("pipeline_name", "N/A")},
            {"property": "Transaction ID", "value": trace.get("transaction_id", "N/A")},
            {"property": "Started At", "value": trace.get("started_at", "N/A")},
            {"property": "Finished At", "value": trace.get("finished_at", "N/A")},
            {
                "property": "Duration",
                "value": _format_duration(
                    trace.get("started_at"), trace.get("finished_at")
                ),
            },
            {"property": "Steps", "value": len(trace.get("steps", []))},
        ]

    def trace_execution_context(trace: Dict[str, Any]) -> List[Dict[str, Any]]:
        ctx = trace.get("execution_context", {})
        result = []
        for key, value in ctx.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    result.append(
                        {"property": f"{key}.{sub_key}", "value": str(sub_value)}
                    )
            else:
                result.append({"property": key, "value": str(value)})
        return result

    def trace_steps_overview(trace: Dict[str, Any]) -> List[Dict[str, Any]]:
        result = []
        for step in trace.get("steps", []):
            result.append(
                {
                    "step": step.get("step", ""),
                    "started_at": step.get("started_at", "N/A"),
                    "finished_at": step.get("finished_at", "N/A"),
                    "duration": _format_duration(
                        step.get("started_at"), step.get("finished_at")
                    ),
                }
            )
        return result

    def trace_step_details(trace: Dict[str, Any], step_id: str) -> List[Any]:
        result = []
        for step in trace.get("steps", []):
            if step.get("step") == step_id:
                props = []
                for key, value in step.items():
                    if key not in ("step",) and not isinstance(value, (dict, list)):
                        props.append({"property": key, "value": str(value)})
                if props:
                    result.append(mo.ui.table(props, selection=None))
                for key, value in step.items():
                    if isinstance(value, (dict, list)) and value:
                        result.append(mo.md(f"**{key}**"))
                        result.append(mo.json(value))
                break
        return result

    def trace_resolved_config_values(trace: Dict[str, Any]) -> List[Dict[str, Any]]:
        result = []
        for cv in trace.get("resolved_config_values", []):
            result.append(
                {
                    "key": cv.get("key", ""),
                    "value": str(cv.get("value", "")),
                    "config_type": cv.get("config_type_name", ""),
                }
            )
        return result


@app.cell(hide_code=True)
def header():
    """Header with logo."""
    mo.hstack(
        [
            mo.image(
                "https://dlthub.com/docs/img/dlthub-logo.png",
                width=100,
                alt="dltHub logo",
            ),
            mo.md("## Trace Viewer"),
        ],
        justify="start",
        gap=2,
    )


@app.cell(hide_code=True)
def get_trace_url():
    """Get trace URL from CLI args or query params."""
    trace_url = cast(str, mo.cli_args().get("trace-url")) or cast(
        str, mo.query_params().get("trace_url")
    )
    return (trace_url,)


@app.cell(hide_code=True)
def load_trace(trace_url: str):
    """Load the trace from URL."""
    dlt_trace = None
    dlt_trace_error = None

    if trace_url:
        try:
            with mo.status.spinner(title="Loading trace..."):
                with urlopen(trace_url, timeout=30) as response:
                    dlt_trace = json.loads(response.read().decode())
        except Exception as e:
            dlt_trace_error = str(e)

    return dlt_trace, dlt_trace_error


@app.cell(hide_code=True)
def create_steps_table(dlt_trace: Dict[str, Any]):
    """Create steps table for selection."""
    dlt_trace_steps_table = None
    if dlt_trace and dlt_trace.get("steps"):
        dlt_trace_steps_table = mo.ui.table(trace_steps_overview(dlt_trace))
    return (dlt_trace_steps_table,)


@app.cell(hide_code=True)
def section_trace(
    trace_url: str,
    dlt_trace: Dict[str, Any],
    dlt_trace_error: str,
    dlt_trace_steps_table: mo.ui.table,
):
    """Display the trace section."""
    _result = [section_marker(TRACE_SECTION_NAME, has_content=dlt_trace is not None)]

    # Page header
    _result.append(
        mo.hstack(
            [
                build_title_and_subtitle(TRACE_TITLE, TRACE_SUBTITLE),
            ],
            align="center",
        )
    )

    if not trace_url:
        _result.append(
            mo.callout(
                mo.md(
                    "**No trace URL provided.**\n\n"
                    "Run with: `marimo run trace_dashboard.py -- --trace-url <URL>`\n\n"
                    "Or add `?trace_url=<URL>` to the browser URL."
                ),
                kind="warn",
            )
        )
    elif dlt_trace_error:
        _result.append(
            build_error_callout(f"Error loading trace: {dlt_trace_error}")
        )
    elif not dlt_trace:
        _result.append(mo.callout(mo.md(TRACE_NO_TRACE_TEXT), kind="warn"))
    else:
        try:
            # Trace Overview
            _result.append(build_title_and_subtitle(TRACE_OVERVIEW_TITLE, title_level=3))
            _result.append(mo.ui.table(trace_overview(dlt_trace), selection=None))

            # Execution Context
            ctx_data = trace_execution_context(dlt_trace)
            if ctx_data:
                _result.append(
                    build_title_and_subtitle(
                        TRACE_EXECUTION_CONTEXT_TITLE,
                        TRACE_EXECUTION_CONTEXT_SUBTITLE,
                        title_level=3,
                    )
                )
                _result.append(mo.ui.table(ctx_data, selection=None))

            # Steps Overview
            if dlt_trace_steps_table:
                _result.append(
                    build_title_and_subtitle(
                        TRACE_STEPS_OVERVIEW_TITLE,
                        TRACE_STEPS_OVERVIEW_SUBTITLE,
                        title_level=3,
                    )
                )
                _result.append(dlt_trace_steps_table)

                # Step Details for selected steps
                if dlt_trace_steps_table.value:
                    for item in dlt_trace_steps_table.value:
                        step_id = item["step"]
                        _result.append(
                            build_title_and_subtitle(
                                TRACE_STEP_DETAILS_TITLE.format(step_id.capitalize()),
                                title_level=3,
                            )
                        )
                        _result.extend(trace_step_details(dlt_trace, step_id))

            # Resolved Config Values
            config_data = trace_resolved_config_values(dlt_trace)
            if config_data:
                _result.append(
                    build_title_and_subtitle(
                        TRACE_RESOLVED_CONFIG_TITLE,
                        TRACE_RESOLVED_CONFIG_SUBTITLE,
                        title_level=3,
                    )
                )
                _result.append(mo.ui.table(config_data, selection=None))

            # Raw Trace
            _result.append(build_title_and_subtitle(TRACE_RAW_TRACE_TITLE, title_level=3))
            _result.append(
                mo.accordion({"Show Raw JSON": mo.json(dlt_trace)}, lazy=True)
            )

        except Exception as e:
            _result.append(
                build_error_callout(
                    f"Error displaying trace: {e}",
                    traceback_string=traceback.format_exc(),
                )
            )

    mo.vstack(_result)


if __name__ == "__main__":
    app.run()
