import functools
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple, Union, cast

import dlt
import marimo as mo
import pandas as pd

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.destination.client import WithStateSync
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.destination.client import DestinationClientConfiguration

from dlt.helpers.dashboard import ui_elements as ui
from dlt.helpers.dashboard.config import DashboardConfiguration

PICKLE_TRACE_FILE = "trace.pickle"


#
# App helpers
#


def resolve_dashboard_config(p: dlt.Pipeline) -> DashboardConfiguration:
    """Resolve the dashboard configuration"""
    return resolve_configuration(
        DashboardConfiguration(),
        sections=(known_sections.DASHBOARD, p.pipeline_name if p else None),
    )


def get_local_pipelines(
    pipelines_dir: str = None, sort_by_trace: bool = True
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

    # check last trace timestamp and create dict
    pipelines_with_timestamps = []
    for pipeline in pipelines:
        trace_file = Path(pipelines_dir) / pipeline / PICKLE_TRACE_FILE
        if trace_file.exists():
            pipelines_with_timestamps.append(
                {"name": pipeline, "timestamp": trace_file.stat().st_mtime}
            )
        else:
            pipelines_with_timestamps.append({"name": pipeline, "timestamp": 0})

    pipelines_with_timestamps.sort(key=lambda x: cast(float, x["timestamp"]), reverse=True)

    return pipelines_dir, pipelines_with_timestamps


def get_pipeline(pipeline_name: str, pipelines_dir: str) -> dlt.Pipeline:
    """Get a pipeline by name.

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
    return cast(DestinationClientConfiguration, pipeline.dataset().destination_client.config)  # type: ignore[attr-defined]


def pipeline_details(pipeline: dlt.Pipeline) -> List[Dict[str, Any]]:
    """
    Get the details of a pipeline.
    """
    try:
        credentials = str(get_destination_config(pipeline).credentials)
    except Exception:
        credentials = "Could not resolve credentials"

    details_dict = {
        "pipeline_name": pipeline.pipeline_name,
        "destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else "No destination set"
        ),
        "credentials": credentials,
        "dataset_name": pipeline.dataset_name,
        "schema": (
            pipeline.default_schema_name
            if pipeline.default_schema_name
            else "No default schema set"
        ),
        "working_dir": pipeline.working_dir,
    }

    return _dict_to_table_items(details_dict)


#
# Schema helpers
#


def create_table_list(
    c: DashboardConfiguration,
    pipeline: dlt.Pipeline,
    show_internals: bool = False,
    show_child_tables: bool = True,
    show_row_counts: bool = False,
) -> List[Dict[str, str]]:
    """Create a list of tables for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline to create the table list for.
    """

    # get tables and filter as needed
    tables = list(
        pipeline.default_schema.data_tables(seen_data_only=True, include_incomplete=False)
    )
    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    if show_internals:
        tables = tables + list(pipeline.default_schema.dlt_tables())

    row_counts = get_row_counts(pipeline) if show_row_counts else {}
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
    for column in pipeline.default_schema.get_table_columns(
        table_name, include_incomplete=False
    ).values():
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

    column_list.sort(key=lambda x: x["name"])
    if not show_internals:
        column_list = [c for c in column_list if not c["name"].lower().startswith("_dlt")]
    return _align_dict_keys(column_list)


#
# Cached Queries
#


def clear_query_cache(pipeline: dlt.Pipeline) -> None:
    """
    Clear the query cache and history
    """

    get_query_result.cache_clear()
    get_loads.cache_clear()
    get_schema_by_version.cache_clear()
    # get_row_counts.cache_clear()


@functools.cache
def get_query_result(pipeline: dlt.Pipeline, query: str) -> pd.DataFrame:
    """
    Get the result of a query.
    """
    return pipeline.dataset()(query).df()


def get_row_counts(pipeline: dlt.Pipeline, load_id: str = None) -> Dict[str, int]:
    """Get the row counts for a pipeline.

    Args:
        pipeline (dlt.Pipeline): The pipeline to get the row counts for.
        load_id (str): The load id to get the row counts for.
    """
    return {
        i["table_name"]: i["row_count"]
        for i in pipeline.dataset()
        .row_counts(dlt_tables=True, load_id=load_id)
        .df()
        .to_dict(orient="records")
    }


@functools.cache
def get_loads(c: DashboardConfiguration, pipeline: dlt.Pipeline, limit: int = 100) -> Any:
    """
    Get the loads of a pipeline.
    """
    loads = pipeline.dataset()._dlt_loads
    if limit:
        loads = loads.limit(limit)
    loads = loads.order_by("inserted_at", "desc")

    loads_list = loads.df().to_dict(orient="records")

    loads_list = [_humanize_datetime_values(c, load) for load in loads_list]

    return loads_list


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


def trace_overview(c: DashboardConfiguration, trace: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get the overview of a trace.
    """
    return _dict_to_table_items(
        _humanize_datetime_values(
            c,
            {
                k: v
                for k, v in trace.items()
                if k in ["transaction_id", "pipeline_name", "started_at", "finished_at"]
            },
        )
    )


def trace_execution_context(
    c: DashboardConfiguration, trace: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Get the execution context of a trace.
    """
    return _dict_to_table_items(trace.get("execution_context", {}))


def trace_steps_overview(c: DashboardConfiguration, trace: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get the steps overview of a trace.
    """

    result = []
    for step in trace.get("steps", []):
        step = _humanize_datetime_values(c, step)
        result.append(
            {k: step[k] for k in ["step", "started_at", "finished_at", "duration"] if k in step}
        )
    return result


def trace_resolved_config_values(
    c: DashboardConfiguration, trace: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Get the resolved config values of a trace.
    """
    return [v.asdict() for v in trace.get("resolved_config_values", [])]


def trace_step_details(
    c: DashboardConfiguration, trace: Dict[str, Any], step_id: str
) -> List[Dict[str, Any]]:
    """
    Get the details of a step.
    """
    _result = []
    for step in trace.get("steps", []):
        if step.get("step") == step_id:
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
                    mo.ui.table(table_metrics, selection=None, freeze_columns_left=["table_name"])
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
                    mo.ui.table(job_metrics, selection=None, freeze_columns_left=["table_name"])
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


#
# internal utils
#


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

        if isinstance(dt, datetime):
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
    if started_at and finished_at:
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
