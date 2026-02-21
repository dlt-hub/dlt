"""Pipeline info, config resolution, destination details, and misc pipeline helpers."""

import os
import platform
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from dlt._workspace.helpers.dashboard.types import TNameValueItem, TPipelineListItem

import dlt
import marimo as mo

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.storages.configuration import WithLocalFiles
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.utils import map_nested_keys_in_place

from dlt._workspace.cli import utils as cli_utils
from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.formatters import (
    dict_to_table_items,
    format_exception_message,
)
from dlt._workspace.helpers.dashboard.utils.schema import schemas_to_table_items
from dlt._workspace.helpers.dashboard.utils import ui
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.pipeline.trace import PipelineTrace


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
    """Get a pipeline by name. Attach exceptions must be handled by the caller."""
    p = dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)
    p.config.use_single_dataset = False
    return p


def get_destination_config(pipeline: dlt.Pipeline) -> DestinationClientConfiguration:
    """Get the destination config of a pipeline."""
    # NOTE: this uses internal interfaces for now...
    return pipeline.dataset().destination_client.config


def pipeline_details(
    c: DashboardConfiguration, pipeline: dlt.Pipeline, pipelines_dir: str
) -> List[TNameValueItem]:
    """Get the details of a pipeline as name/value table items."""
    try:
        credentials = str(get_destination_config(pipeline).credentials)
    except Exception:
        credentials = strings.overview_credentials_error

    last_executed = strings.overview_no_trace
    if (trace := pipeline.last_trace) and hasattr(trace, "started_at"):
        last_executed = cli_utils.date_from_timestamp_with_ago(trace.started_at, c.datetime_format)

    details_dict = {
        "pipeline_name": pipeline.pipeline_name,
        "destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else strings.overview_no_destination
        ),
        "last executed": last_executed,
        "credentials": credentials,
        "dataset_name": pipeline.dataset_name,
        "working_dir": pipeline.working_dir,
        "state_version": (
            pipeline.state["_state_version"] if pipeline.state else strings.overview_no_state
        ),
    }

    table_items = dict_to_table_items(details_dict)
    table_items += schemas_to_table_items(pipeline.schemas.values(), pipeline.default_schema_name)
    return table_items


def remote_state_details(pipeline: dlt.Pipeline) -> List[TNameValueItem]:
    """Get the remote state details of a pipeline."""
    error_details = ""
    remote_state = None
    try:
        remote_state = pipeline._restore_state_from_destination()
    except Exception as exc:
        error_details = format_exception_message(exc)

    if not remote_state:
        return dict_to_table_items(
            {"Info": strings.overview_remote_state_error, "Details": error_details}
        )
    remote_schemas = pipeline._get_schemas_from_destination(
        remote_state["schema_names"], always_download=True
    )

    table_items = dict_to_table_items({"state_version": remote_state["_state_version"]})
    table_items += schemas_to_table_items(remote_schemas, pipeline.default_schema_name)
    return table_items


def get_local_data_path(pipeline: dlt.Pipeline) -> str:
    """Get the local data path of a pipeline"""
    if not pipeline.destination:
        return None
    try:
        config = pipeline._get_destination_clients(dlt.Schema("temp"))[0].config
        if isinstance(config, WithLocalFiles):
            return config.local_dir
    except (PipelineConfigMissing, ConfigFieldMissingException):
        pass
    return None


def open_local_folder(folder: str) -> None:
    """Open a folder in the file explorer"""
    system = platform.system()
    if system == "Windows":
        os.startfile(folder)  # type: ignore[attr-defined,unused-ignore]
    elif system == "Darwin":
        subprocess.run(["open", folder], check=True)
    elif shutil.which("wslview"):
        subprocess.run(["wslview", folder], check=True)
    else:
        subprocess.run(["xdg-open", folder], check=True)


def pipeline_link_list(config: DashboardConfiguration, pipelines: List[TPipelineListItem]) -> str:
    """Build a markdown list of links to pipelines."""
    if not pipelines:
        return strings.overview_no_pipelines

    count = 0
    link_list: str = ""
    for _p in pipelines:
        link = f"* [{_p['name']}](?pipeline={_p['name']})"
        link = (
            link
            + strings.overview_last_executed_label
            + cli_utils.date_from_timestamp_with_ago(_p["timestamp"], config.datetime_format)
        )

        link_list += f"{link}\n"
        count += 1
        if count == 10:
            break

    return link_list


def exception_section(p: dlt.Pipeline) -> List[mo.Html]:
    """Build an exception section for a pipeline"""
    if not p or not p.last_trace:
        return []

    if not (exception_step := next((s for s in p.last_trace.steps if s.step_exception), None)):
        return []

    last_exception = exception_step.exception_traces[-1]
    title = f"{last_exception['exception_type']}: {last_exception['message']}"

    _result = []
    _result.append(
        ui.title_and_subtitle(
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
                strings.error_show_full_stacktrace: mo.ui.code_editor(
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


def sanitize_trace_for_display(trace: PipelineTrace) -> Dict[str, Any]:
    """Sanitize a trace for display by cleaning up non-primitive keys."""
    if not trace:
        return {}

    def _remove_non_primitives(obj: Any) -> Any:
        if not isinstance(obj, (str, bool, int, float)):
            return repr(obj)
        return obj

    return map_nested_keys_in_place(_remove_non_primitives, trace.asdict())
