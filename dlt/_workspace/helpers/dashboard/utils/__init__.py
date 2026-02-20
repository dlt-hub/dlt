import shutil
import functools
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
    NamedTuple,
    get_args,
)
import datetime  # noqa: 251
import os
import platform
import subprocess
import sqlglot

import dlt
import marimo as mo
import pyarrow
import traceback

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.pendulum import pendulum
from dlt.common.pipeline import LoadInfo
from dlt.common.schema import Schema
from dlt.common.storages import LoadPackageInfo
from dlt.common.storages.load_package import PackageStorage
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.destination.exceptions import SqlClientNotAvailable
from dlt.common.storages.configuration import WithLocalFiles
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.utils import map_nested_keys_in_place

from dlt._workspace.helpers.dashboard.utils import ui
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.const import (
    LOAD_PACKAGE_STATUS_COLORS,
    PENDING_LOAD_STATUSES,
    PIPELINE_RUN_STEP_COLORS,
    TPipelineRunStatus,
    TVisualPipelineStep,
    VISUAL_PIPELINE_STEPS,
)
from dlt._workspace.helpers.dashboard.utils.formatters import (
    dict_to_table_items,
    format_exception_message,
    format_duration,
    humanize_datetime_values,
)
from dlt._workspace.helpers.dashboard.utils.ui import dlt_table
from dlt._workspace.helpers.dashboard.utils.schema import (  # noqa: E402
    create_column_list,
    create_table_list,
    get_schema_by_version,
    get_source_and_resource_state_for_table,
    schemas_to_table_items,
)
from dlt._workspace.cli import utils as cli_utils
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationUndefinedEntity
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.pipeline.trace import PipelineTrace, PipelineStepTrace


#
# App helpers
#


def get_dashboard_config_sections(p: Optional[dlt.Pipeline]) -> Tuple[str, ...]:
    """Find dashboard config section layout for a particular pipeline or for active
    run context type.
    """
    sections: Tuple[str, ...] = ()

    if p is None:
        # use workspace section layout
        context = dlt.current.run_context()
        if context.config is None or not context.config.__class__.__recommended_sections__:
            pass
        else:
            sections = tuple(context.config.__class__.__recommended_sections__) + sections
    else:
        # pipeline section layout
        sections = (known_sections.PIPELINES, p.pipeline_name) + sections

    return sections


def resolve_dashboard_config(p: Optional[dlt.Pipeline]) -> DashboardConfiguration:
    """Resolve the dashboard configuration"""
    return resolve_configuration(
        DashboardConfiguration(),
        sections=get_dashboard_config_sections(p),
    )


def get_pipeline(pipeline_name: str, pipelines_dir: str) -> dlt.Pipeline:
    """Get a pipeline by name. Attach exceptions must be handled by the caller

    Args:
        pipeline_name (str): The name of the pipeline to get.

    Returns:
        dlt.Pipeline: The pipeline.
    """
    p = dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)
    p.config.use_single_dataset = False
    return p


#
# Pipeline details
#


def get_destination_config(pipeline: dlt.Pipeline) -> DestinationClientConfiguration:
    """Get the destination config of a pipeline."""
    # NOTE: this uses internal interfaces for now...
    return pipeline.dataset().destination_client.config


def pipeline_details(
    c: DashboardConfiguration, pipeline: dlt.Pipeline, pipelines_dir: str
) -> List[Dict[str, Any]]:
    """
    Get the details of a pipeline.
    """
    try:
        credentials = str(get_destination_config(pipeline).credentials)
    except Exception:
        credentials = "Could not resolve credentials."

    # find the pipeline in all_pipelines and get the timestamp
    trace = pipeline.last_trace

    last_executed = "No trace found"
    if trace and hasattr(trace, "started_at"):
        last_executed = cli_utils.date_from_timestamp_with_ago(trace.started_at, c.datetime_format)

    details_dict = {
        "pipeline_name": pipeline.pipeline_name,
        "destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else "No destination set"
        ),
        "last executed": last_executed,
        "credentials": credentials,
        "dataset_name": pipeline.dataset_name,
        "working_dir": pipeline.working_dir,
        "state_version": pipeline.state["_state_version"] if pipeline.state else "No state found",
    }

    table_items = dict_to_table_items(details_dict)
    table_items += schemas_to_table_items(pipeline.schemas.values(), pipeline.default_schema_name)
    return table_items


def remote_state_details(pipeline: dlt.Pipeline) -> List[Dict[str, Any]]:
    """
    Get the remote state details of a pipeline.
    """
    error_details = ""
    remote_state = None
    try:
        remote_state = pipeline._restore_state_from_destination()
    except Exception as exc:
        error_details = format_exception_message(exc)

    if not remote_state:
        return dict_to_table_items(
            {"Info": "Could not restore state from destination", "Details": error_details}
        )
    remote_schemas = pipeline._get_schemas_from_destination(
        remote_state["schema_names"], always_download=True
    )

    table_items = dict_to_table_items({"state_version": remote_state["_state_version"]})
    table_items += schemas_to_table_items(remote_schemas, pipeline.default_schema_name)
    return table_items


#
# Cached Queries
#


def clear_query_cache(pipeline: dlt.Pipeline) -> None:
    """
    Clear the query cache and history
    """

    get_query_result_cached.cache_clear()
    get_schema_by_version.cache_clear()
    # get_row_counts.cache_clear()


def get_default_query_for_table(
    pipeline: dlt.Pipeline, schema_name: str, table_name: str, limit: bool
) -> Tuple[str, str, str]:
    """Build a default SELECT query for a table. Returns (sql_query, error_message, traceback)."""
    try:
        _dataset = pipeline.dataset(schema=schema_name)
        _sql_query = (
            _dataset.table(table_name)
            .limit(1000 if limit else None)
            .to_sql(pretty=True, _raw_query=True)
        )
        return _sql_query, None, None
    except Exception as exc:
        return "", format_exception_message(exc), traceback.format_exc()


def get_example_query_for_dataset(pipeline: dlt.Pipeline, schema_name: str) -> Tuple[str, str, str]:
    """Return an example query for the first data table in the schema. Returns (sql_query, error_message, traceback)."""
    schema = pipeline.schemas.get(schema_name)
    if schema and (tables := schema.data_tables()):
        return get_default_query_for_table(pipeline, schema_name, tables[0]["name"], True)
    return "", "Schema does not contain any tables.", None


def get_query_result(pipeline: dlt.Pipeline, query: str) -> Tuple[pyarrow.Table, str, str]:
    """
    Get the result of a query. Parses the query to ensure it is a valid SQL query before sending it to the destination.
    """
    try:
        sqlglot.parse_one(
            query,
            dialect=pipeline.destination.capabilities().sqlglot_dialect,
        )
        return get_query_result_cached(pipeline, query), None, None
    except Exception as exc:
        return pyarrow.table({}), format_exception_message(exc), traceback.format_exc()


@functools.cache
def get_query_result_cached(pipeline: dlt.Pipeline, query: str) -> pyarrow.Table:
    """Execute a raw SQL query against the pipeline dataset and return the result as an Arrow table (cached)."""
    return pipeline.dataset()(query, _execute_raw_query=True).arrow()


def get_row_counts(
    pipeline: dlt.Pipeline, selected_schema_name: str = None, load_id: str = None
) -> Dict[str, Any]:
    """Get the row counts for a pipeline.

    Args:
        pipeline (dlt.Pipeline): The pipeline to get the row counts for.
        load_id (str): The load id to get the row counts for.
    """
    row_counts = {}
    try:
        row_counts = {
            i["table_name"]: i["row_count"]
            for i in pipeline.dataset(schema=selected_schema_name)
            .row_counts(dlt_tables=True, load_id=load_id)
            .arrow()
            .to_pylist()
        }
    except (
        DatabaseUndefinedRelation,
        DestinationUndefinedEntity,
        SqlClientNotAvailable,
        PipelineConfigMissing,
        ConnectionError,
    ):
        # TODO: somehow propagate errors to the user here
        pass

    return row_counts


def get_row_counts_list(
    pipeline: dlt.Pipeline, selected_schema_name: str = None, load_id: str = None
) -> List[Dict[str, Any]]:
    """Get the row counts for a pipeline as a list."""
    row_counts_dict = get_row_counts(pipeline, selected_schema_name, load_id)
    row_counts = [{"name": k, "row_count": v} for k, v in row_counts_dict.items()]
    row_counts.sort(key=lambda x: str(x["name"]))
    return row_counts


def get_loads(
    c: DashboardConfiguration, pipeline: dlt.Pipeline, limit: int = 100
) -> Tuple[Any, str, str]:
    """
    Get the loads of a pipeline.
    """
    try:
        loads = (
            pipeline.dataset()
            ._dlt_loads.filter("schema_name", "in", pipeline.schema_names)
            .order_by("inserted_at", "desc")
        )
        if limit:
            loads = loads.limit(limit)

        loads_list = loads.arrow().to_pylist()
        loads_list = [humanize_datetime_values(c, load) for load in loads_list]
        return loads_list, None, None
    except Exception as exc:
        return [], format_exception_message(exc), traceback.format_exc()


#
# trace helpers
#


def trace_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, Any]]:
    """
    Get the overview of a trace.
    """
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
    """
    Get the execution context of a trace.
    """
    return dict_to_table_items(dict(trace.execution_context) or {})


def trace_steps_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, Any]]:
    """
    Get the steps overview of a trace.
    """

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
    """
    Get the resolved config values of a trace.
    """
    return [v.asdict() for v in trace.resolved_config_values]  # type: ignore[misc]


def trace_step_details(c: DashboardConfiguration, trace: PipelineTrace, step_id: str) -> List[Any]:
    """
    Get the details of a step.
    """
    _result = []
    for step_obj in trace.steps:
        if step_obj.step == step_id:
            step = step_obj.asdict()
            info_section = step.get(f"{step_id}_info", {})
            if "table_metrics" in info_section:
                _result.append(
                    ui.build_title_and_subtitle(
                        f"{step_id} table metrics",
                        title_level=4,
                    )
                )
                table_metrics = info_section.get("table_metrics", [])
                table_metrics = [humanize_datetime_values(c, t) for t in table_metrics]
                _result.append(dlt_table(table_metrics, freeze_column="table_name"))

            if "job_metrics" in info_section:
                _result.append(
                    ui.build_title_and_subtitle(
                        f"{step_id} job metrics",
                        title_level=4,
                    )
                )
                job_metrics = info_section.get("job_metrics", [])
                job_metrics = [humanize_datetime_values(c, j) for j in job_metrics]
                _result.append(dlt_table(job_metrics, freeze_column="table_name"))

    return _result


#
# misc
#


def open_local_folder(folder: str) -> None:
    """Open a folder in the file explorer"""
    system = platform.system()
    if system == "Windows":
        os.startfile(folder)  # type: ignore[attr-defined,unused-ignore]
    elif system == "Darwin":
        subprocess.run(["open", folder], check=True)
    elif shutil.which("wslview"):
        # WSL detected
        subprocess.run(["wslview", folder], check=True)
    else:
        subprocess.run(["xdg-open", folder], check=True)


def get_local_data_path(pipeline: dlt.Pipeline) -> str:
    """Get the local data path of a pipeline"""
    if not pipeline.destination:
        return None
    try:
        config = pipeline._get_destination_clients(dlt.Schema("temp"))[0].config
        if isinstance(config, WithLocalFiles):
            return config.local_dir
    except (PipelineConfigMissing, ConfigFieldMissingException):
        # If configs are missing or anything like that, we can fail silently here
        pass
    return None


def build_pipeline_link_list(
    config: DashboardConfiguration, pipelines: List[Dict[str, Any]]
) -> str:
    """Build a list of links to the pipeline."""
    if not pipelines:
        return "No pipelines found."

    count = 0
    link_list: str = ""
    for _p in pipelines:
        link = f"* [{_p['name']}](?pipeline={_p['name']})"
        link = (
            link
            + " - last executed: "
            + cli_utils.date_from_timestamp_with_ago(_p["timestamp"], config.datetime_format)
        )

        link_list += f"{link}\n"
        count += 1
        if count == 10:
            break

    return link_list


def sanitize_trace_for_display(trace: PipelineTrace) -> Dict[str, Any]:
    """Sanitize a trace for display by cleaning up non-primitive keys (we use tuples as keys in nested hints)"""
    if not trace:
        return {}

    def _remove_non_primitives(obj: Any) -> Any:
        if not isinstance(obj, (str, bool, int, float)):
            return repr(obj)
        return obj

    return map_nested_keys_in_place(_remove_non_primitives, trace.asdict())


def build_exception_section(p: dlt.Pipeline) -> List[Any]:
    """Build an exception section for a pipeline"""
    if not p or not p.last_trace:
        return []

    exception_step = None
    for step in p.last_trace.steps:
        if step.step_exception:
            exception_step = step
            break

    if not exception_step:
        return []

    last_exception = exception_step.exception_traces[-1]
    title = f"{last_exception['exception_type']}: {last_exception['message']}"

    _result = []
    _result.append(
        ui.build_title_and_subtitle(
            title,
            title_level=2,
        )
    )

    _exception_traces = []
    for trace in reversed(exception_step.exception_traces):
        _exception_traces.extend(trace["stack_trace"])
        _exception_traces.append(f"{trace['exception_type']}: {trace['message']}")
        _exception_traces.append("\n")
        _exception_traces.append("\n")

    _result.append(
        mo.accordion(
            {
                "Show full stacktrace": mo.ui.code_editor(
                    "".join(_exception_traces),
                    language="python",
                    disabled=True,
                    show_copy_button=True,
                )
            },
            lazy=True,
        )
    )
    return [mo.callout(mo.vstack(_result), kind="danger")]


#
# last pipeline execution helpers
#


class PipelineStepData(NamedTuple):
    step: TVisualPipelineStep
    duration_ms: float
    failed: bool


def _build_migration_badge(count: int) -> str:
    """Build migration badge HTML using CSS classes"""
    if count == 0:
        return ""
    return (
        '<div class="status-badge status-badge-yellow">'
        f"<strong>{count} dataset migration(s)</strong>"
        "</div>"
    )


def _build_status_badge(status: TPipelineRunStatus) -> str:
    """Build status badge HTML using CSS classes"""
    badge_class = "status-badge-green" if status == "succeeded" else "status-badge-red"
    return f'<div class="status-badge {badge_class}"><strong>{status}</strong></div>'


def _build_pipeline_execution_html(
    transaction_id: str,
    status: TPipelineRunStatus,
    steps_data: List[PipelineStepData],
    migrations_count: int = 0,
    finished_at: Optional[datetime.datetime] = None,
) -> mo.Html:
    """
    Build an HTML visualization for a pipeline execution using CSS classes
    """
    total_ms = sum(step.duration_ms for step in steps_data)
    last = len(steps_data) - 1

    # Build the general info of the execution
    relative_time = ""
    if finished_at:
        time_ago = pendulum.instance(finished_at).diff_for_humans()
        relative_time = f"<div>Executed: <strong>{time_ago}</strong></div>"

    general_info = f"""
    <div>Last execution ID: <strong>{transaction_id[:8]}</strong></div>
    <div>Total time: <strong>{format_duration(total_ms)}</strong></div>
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
            f"{step.step.capitalize()} {format_duration(step.duration_ms)}</span>"
        )

    # Build the whole html using CSS classes
    html = f"""
    <div class="pipeline-execution-container">
        <!-- Main 3-column flex container -->
        <div class="pipeline-execution-layout">

            <!-- LEFT COLUMN: Run ID, Total time -->
            <div class="pipeline-execution-info">
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
                {_build_migration_badge(migrations_count)}
                {_build_status_badge(status)}
            </div>
        </div>
    </div>
    """
    return mo.Html(html)


def _get_steps_data_and_status(
    trace_steps: List[PipelineStepTrace],
) -> Tuple[List[PipelineStepData], TPipelineRunStatus]:
    """Gets trace steps data and the status of the corresponding pipeline execution"""
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


def _get_migrations_count(last_load_info: LoadInfo) -> int:
    """Counts the number of unique migrations (schema versions) from load packages"""
    migrations_count: int = 0
    seen_schema_hashes = set()
    for package in last_load_info.load_packages:
        # Only count if there are schema updates
        if len(package.schema_update) > 0:
            if package.schema_hash not in seen_schema_hashes:
                migrations_count += 1
                seen_schema_hashes.add(package.schema_hash)
    return migrations_count


def build_pipeline_execution_visualization(trace: PipelineTrace) -> Optional[mo.Html]:
    """Creates a visual timeline of pipeline run showing extract, normalize and load steps"""

    steps_data, status = _get_steps_data_and_status(trace.steps)
    migrations_count = _get_migrations_count(trace.last_load_info) if trace.last_load_info else 0

    return _build_pipeline_execution_html(
        trace.transaction_id,
        status,
        steps_data,
        migrations_count,
        trace.finished_at,
    )


#
# last pipeline executions load packages helpers
#


def _collect_load_packages_from_trace(
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
    """
    For each package in the trace, determine its visual status badge based on
    whether the package is partially loaded, pending (extracted or normalized),
    or in a final state (loaded, aborted, etc.). Returns a marimo table
    containing the load id and a badge representing its status.
    """
    packages = _collect_load_packages_from_trace(trace)
    result: List[Dict[str, Any]] = []

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

        status_html = (
            '<div class="status-badge'
            f' status-badge-{badge_color_key}"><strong>{badge_text}</strong></div>'
        )
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
