import shutil
import functools
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union, cast
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
from dlt.common.destination.client import WithStateSync
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.destination.client import DestinationClientConfiguration
from dlt.common.destination.exceptions import SqlClientNotAvailable
from dlt.common.storages.configuration import WithLocalFiles
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.typing import DictStrAny
from dlt.common.utils import map_nested_keys_in_place

from dlt._workspace.helpers.dashboard import ui_elements as ui
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationUndefinedEntity
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.pipeline.exceptions import CannotRestorePipelineException
from dlt.pipeline.trace import PipelineTrace


PICKLE_TRACE_FILE = "trace.pickle"


#
# App helpers
#


def _exception_to_string(exception: Exception) -> str:
    """Convert an exception to a string"""
    if isinstance(exception, (PipelineConfigMissing, ConfigFieldMissingException)):
        return "Could not connect to destination, configuration values are missing."
    elif isinstance(exception, (SqlClientNotAvailable)):
        return "The destination of this pipeline does not support querying data with sql."
    elif isinstance(exception, (DestinationUndefinedEntity, DatabaseUndefinedRelation)):
        return (
            "Could connect to destination, but the required table or dataset does not exist in the"
            " destination."
        )
    return str(exception)


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


def get_trace_file_path(pipeline_name: str, pipelines_dir: str) -> Path:
    """Get the path to the pickle file for a pipeline"""
    return Path(pipelines_dir) / pipeline_name / PICKLE_TRACE_FILE


def get_pipeline_last_run(pipeline_name: str, pipelines_dir: str) -> float:
    """Get the last run of a pipeline"""
    trace_file = get_trace_file_path(pipeline_name, pipelines_dir)
    if trace_file.exists():
        return os.path.getmtime(trace_file)
    return 0


def get_local_pipelines(
    pipelines_dir: str = None, sort_by_trace: bool = True, addtional_pipelines: List[str] = None
) -> Tuple[str, List[Dict[str, Any]]]:
    """Get the local pipelines directory and the list of pipeline names in it.

    Args:
        pipelines_dir (str, optional): The local pipelines directory. Defaults to get_dlt_pipelines_dir().
        sort_by_trace (bool, optional): Whether to sort the pipelines by the latet timestamp of trace. Defaults to True.
    Returns:
        Tuple[str, List[str]]: The local pipelines directory and the list of pipeline names in it.
    """
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)

    try:
        pipelines = storage.list_folder_dirs(".", to_root=False)
    except Exception:
        pipelines = []

    if addtional_pipelines:
        for pipeline in addtional_pipelines:
            if pipeline and pipeline not in pipelines:
                pipelines.append(pipeline)

    # check last trace timestamp and create dict
    pipelines_with_timestamps = []
    for pipeline in pipelines:
        pipelines_with_timestamps.append(
            {"name": pipeline, "timestamp": get_pipeline_last_run(pipeline, pipelines_dir)}
        )

    pipelines_with_timestamps.sort(key=lambda x: cast(float, x["timestamp"]), reverse=True)

    return pipelines_dir, pipelines_with_timestamps


def get_pipeline(pipeline_name: str, pipelines_dir: str) -> dlt.Pipeline:
    """Get a pipeline by name.

    Args:
        pipeline_name (str): The name of the pipeline to get.

    Returns:
        dlt.Pipeline: The pipeline.
    """
    try:
        p = dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)
        p.config.use_single_dataset = False
        return p
    except CannotRestorePipelineException:
        return None


#
# Pipeline details
#


def get_destination_config(pipeline: dlt.Pipeline) -> DestinationClientConfiguration:
    """Get the destination config of a pipeline."""
    # NOTE: this uses internal interfaces for now...
    return pipeline.dataset().destination_client.config


def schemas_to_table_items(
    schemas: Iterable[Schema], default_schema_name: str
) -> List[Dict[str, Any]]:
    """
    Convert a list of schemas to a list of table items.
    """
    # Sort schemas so that the default schema is at the top
    schemas = sorted(
        schemas, key=lambda s: 0 if getattr(s, "name", None) == default_schema_name else 1
    )
    table_items = []
    count = 0
    for schema in schemas:
        table_items.append(
            {
                "name": "schemas" if count == 0 else "",
                "value": (
                    schema.name
                    + f" ({schema.version}, {schema.version_hash[:8]}) "
                    + (" (default)" if schema.name == default_schema_name else "")
                ),
            }
        )
        count += 1
    return table_items


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
    pipeline_timestamp = get_pipeline_last_run(pipeline.pipeline_name, pipeline.pipelines_dir)

    details_dict = {
        "pipeline_name": pipeline.pipeline_name,
        "destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else "No destination set"
        ),
        "last executed": _date_from_timestamp_with_ago(c, pipeline_timestamp),
        "credentials": credentials,
        "dataset_name": pipeline.dataset_name,
        "working_dir": pipeline.working_dir,
        "state_version": pipeline.state["_state_version"] if pipeline.state else "No state found",
    }

    table_items = _dict_to_table_items(details_dict)
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
        error_details = _exception_to_string(exc)

    if not remote_state:
        return _dict_to_table_items(
            {"Info": "Could not restore state from destination", "Details": error_details}
        )
    remote_schemas = pipeline._get_schemas_from_destination(
        remote_state["schema_names"], always_download=True
    )

    table_items = _dict_to_table_items({"state_version": remote_state["_state_version"]})
    table_items += schemas_to_table_items(remote_schemas, pipeline.default_schema_name)
    return table_items


#
# Schema helpers
#


def create_table_list(
    c: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    selected_schema_name: str = None,
    show_internals: bool = False,
    show_child_tables: bool = True,
    show_row_counts: bool = False,
) -> List[Dict[str, Any]]:
    """Create a list of tables for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline to create the table list for.
    """

    # get tables and filter as needed
    tables = list(
        pipeline.schemas[selected_schema_name].data_tables(
            seen_data_only=True, include_incomplete=False
        )
    )
    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    if show_internals:
        tables = tables + list(pipeline.schemas[selected_schema_name].dlt_tables())

    row_counts = get_row_counts(pipeline, selected_schema_name) if show_row_counts else {}
    table_list: List[Dict[str, Union[str, int, None]]] = [
        {
            **{prop: table.get(prop, None) for prop in ["name", *c.table_list_fields]},  # type: ignore[misc]
            "row_count": row_counts.get(table["name"], None),
        }
        for table in tables
    ]
    table_list.sort(key=lambda x: str(x["name"]))

    return _align_dict_keys(table_list)


def create_column_list(
    c: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    table_name: str,
    selected_schema_name: str = None,
    show_internals: bool = False,
    show_type_hints: bool = True,
    show_other_hints: bool = False,
    show_custom_hints: bool = False,
) -> List[Dict[str, Any]]:
    """Create a list of columns for a table.

    Args:
        pipeline_name (str): The name of the pipeline to create the column list for.
        table_name (str): The name of the table to create the column list for.
    """
    column_list: List[Dict[str, Any]] = []
    for column in (
        pipeline.schemas[selected_schema_name]
        .get_table_columns(table_name, include_incomplete=False)
        .values()
    ):
        column_dict: Dict[str, Any] = {
            "name": column["name"],
        }

        # show type hints if requested
        if show_type_hints:
            column_dict = {
                **column_dict,
                **{hint: column.get(hint, None) for hint in c.column_type_hints},
            }

        # show "other" hints if requested
        if show_other_hints:
            column_dict = {
                **column_dict,
                **{hint: column.get(hint, None) for hint in c.column_other_hints},
            }

        # show custom hints (x-) if requesed
        if show_custom_hints:
            for key in column:
                if key.startswith("x-"):
                    column_dict[key] = column[key]  # type: ignore

        column_list.append(column_dict)

    if not show_internals:
        column_list = [c for c in column_list if not c["name"].lower().startswith("_dlt")]
    return _align_dict_keys(column_list)


def get_source_and_resouce_state_for_table(
    table: TTableSchema, pipeline: dlt.Pipeline, schema_name: str
) -> Tuple[str, DictStrAny, DictStrAny]:
    if "resource" not in table:
        return None, {}, {}

    pipeline.activate()
    resource_name = table["resource"]
    source_state = dlt.extract.state.source_state(schema_name)
    resource_state = dlt.extract.state.resource_state(resource_name, source_state)
    # note, we remove the resources key from the source state
    source_state = {k: v for k, v in source_state.items() if k != "resources"}

    return table["resource"], source_state, resource_state


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
    try:
        _dataset = pipeline.dataset(schema=schema_name)
        _sql_query = (
            _dataset.table(table_name)
            .limit(1000 if limit else None)
            .to_sql(pretty=True, _raw_query=True)
        )
        return _sql_query, None, None
    except Exception as exc:
        return "", _exception_to_string(exc), traceback.format_exc()


def get_example_query_for_dataset(pipeline: dlt.Pipeline, schema_name: str) -> Tuple[str, str, str]:
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
        return pyarrow.table({}), _exception_to_string(exc), traceback.format_exc()


@functools.cache
def get_query_result_cached(pipeline: dlt.Pipeline, query: str) -> pyarrow.Table:
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
        loads = pipeline.dataset()._dlt_loads
        if limit:
            loads = loads.limit(limit)
        loads = loads.order_by("inserted_at", "desc")
        loads_list = loads.arrow().to_pylist()
        loads_list = [_humanize_datetime_values(c, load) for load in loads_list]
        return loads_list, None, None
    except Exception as exc:
        return [], _exception_to_string(exc), traceback.format_exc()


@functools.cache
def get_schema_by_version(pipeline: dlt.Pipeline, version_hash: str) -> Schema:
    """
    Get the schema version of a pipeline.
    """
    with pipeline.destination_client() as client:
        if isinstance(client, WithStateSync):
            stored_schema = client.get_stored_schema_by_hash(version_hash)
            if not stored_schema:
                return None
            return Schema.from_stored_schema(json.loads(stored_schema.schema))
    return None


#
# trace helpers
#


def trace_overview(c: DashboardConfiguration, trace: PipelineTrace) -> List[Dict[str, Any]]:
    """
    Get the overview of a trace.
    """
    return _dict_to_table_items(
        _humanize_datetime_values(
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
    return _dict_to_table_items(dict(trace.execution_context) or {})


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
        step = _humanize_datetime_values(c, step)
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
                table_metrics = _align_dict_keys(info_section.get("table_metrics", []))
                table_metrics = [_humanize_datetime_values(c, t) for t in table_metrics]
                _result.append(
                    mo.ui.table(
                        table_metrics,
                        selection=None,
                        freeze_columns_left=(["table_name"] if table_metrics else None),
                    )
                )

            if "job_metrics" in info_section:
                _result.append(
                    ui.build_title_and_subtitle(
                        f"{step_id} job metrics",
                        title_level=4,
                    )
                )
                job_metrics = _align_dict_keys(info_section.get("job_metrics", []))
                job_metrics = [_humanize_datetime_values(c, j) for j in job_metrics]
                _result.append(
                    mo.ui.table(
                        job_metrics,
                        selection=None,
                        freeze_columns_left=(["table_name"] if job_metrics else None),
                    )
                )

    return _result


#
# misc
#


def style_cell(row_id: str, name: str, __: Any) -> Dict[str, str]:
    """
    Style a cell in a table.

    Args:
        row_id (str): The id of the row.
        name (str): The name of the column.
        __ (Any): The value of the cell.

    Returns:
        Dict[str, str]: The css style of the cell.
    """
    style = {"background-color": "white" if (int(row_id) % 2 == 0) else "#f4f4f9"}
    if name.lower() == "name":
        style["font-weight"] = "bold"
    return style


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
        return "No local pipelines found."

    count = 0
    link_list: str = ""
    for _p in pipelines:
        link = f"* [{_p['name']}](?pipeline={_p['name']})"
        link = link + " - last executed: " + _date_from_timestamp_with_ago(config, _p["timestamp"])

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

    _result = []
    _result.append(
        ui.build_title_and_subtitle(
            f"Exception encountered during last pipeline run in step '{step.step}'",
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
# internal utils
#


def _date_from_timestamp_with_ago(
    config: DashboardConfiguration, timestamp: Union[int, float]
) -> str:
    """Return a date with ago section"""
    if not timestamp or timestamp == 0:
        return "never"
    p_ts = pendulum.from_timestamp(timestamp)
    time_formatted = p_ts.format(config.datetime_format)
    ago = p_ts.diff_for_humans()
    return f"{ago} ({time_formatted})"


def _without_none_or_empty_string(d: Mapping[Any, Any]) -> Mapping[Any, Any]:
    """Return a new dict with all `None` values removed"""
    return {k: v for k, v in d.items() if v is not None and v != ""}


def _align_dict_keys(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Makes sure all dicts have the same keys, sets "-" as default. Makes for nicer rendering in marimo table
    """
    items = cast(List[Dict[str, Any]], [_without_none_or_empty_string(i) for i in items])
    all_keys = set(chain.from_iterable(i.keys() for i in items))
    for i in items:
        i.update({key: "-" for key in all_keys if key not in i})
    return items


def _humanize_datetime_values(c: DashboardConfiguration, d: Dict[str, Any]) -> Dict[str, Any]:
    """Humanize datetime values in a dict, expects certain keys to be present as found in the trace, could be made more configurable"""

    started_at = d.get("started_at", "")
    finished_at = d.get("finished_at", "")
    inserted_at = d.get("inserted_at", "")
    created = d.get("created", "")
    last_modified = d.get("last_modified", "")
    load_id = d.get("load_id", "")

    def _humanize_datetime(dt: Union[str, int]) -> str:
        from datetime import datetime  # noqa: I251

        if dt in ["", None, "-"]:
            return "-"
        elif isinstance(dt, datetime):
            p = pendulum.instance(dt)
        elif isinstance(dt, str) and dt.replace(".", "").isdigit():
            p = pendulum.from_timestamp(float(dt))
        elif isinstance(dt, str):
            p = cast(pendulum.DateTime, pendulum.parse(dt))
        elif isinstance(dt, int) or isinstance(dt, float):
            p = pendulum.from_timestamp(dt)
        else:
            raise ValueError(f"Invalid datetime value: {dt}")
        return p.format(c.datetime_format)

    if started_at:
        d["started_at"] = _humanize_datetime(started_at)
    if finished_at:
        d["finished_at"] = _humanize_datetime(finished_at)
    if started_at not in ["", None, "-"] and finished_at not in ["", None, "-"]:
        d["duration"] = (
            f"{pendulum.instance(finished_at).diff(pendulum.instance(started_at)).in_words()}"
        )
    if created:
        d["created"] = _humanize_datetime(created)
    if last_modified:
        d["last_modified"] = _humanize_datetime(last_modified)
    if inserted_at:
        d["inserted_at"] = _humanize_datetime(inserted_at)
    if load_id:
        d["load_package_created_at"] = _humanize_datetime(load_id)

    return d


def _dict_to_table_items(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a dict to a list of dicts with name and value keys"""
    return [{"name": k, "value": v} for k, v in d.items()]
