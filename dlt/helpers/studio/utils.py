from typing import List, Tuple, Any, Dict, Union, cast
from itertools import chain
import functools
import dlt
import marimo as mo
import pandas as pd
from pathlib import Path

from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages import FileStorage
from dlt.common.utils import without_none


PICKLE_TRACE_FILE = "trace.pickle"


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
    return dlt.attach(pipeline_name, pipelines_dir=pipelines_dir)


def _align_dict_keys(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Makes sure all dicts have the same keys, sets "-" as default. Makes for nicer rendering in marimo table
    """
    items = cast(List[Dict[str, Any]], [without_none(i) for i in items])
    all_keys = set(chain.from_iterable(i.keys() for i in items))
    for i in items:
        i.update({key: "-" for key in all_keys if key not in i})
    return items


ROW_COUNTS_CACHE: Dict[str, Dict[str, int]] = {}


def create_table_list(
    pipeline: dlt.Pipeline,
    show_internals: bool = False,
    show_child_tables: bool = True,
    show_row_counts: bool = False,
) -> List[Dict[str, str]]:
    """Create a list of tables for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline to create the table list for.
    """
    global ROW_COUNTS_CACHE
    if not show_row_counts:
        ROW_COUNTS_CACHE.pop(pipeline.pipeline_name, {})
    elif show_row_counts and not ROW_COUNTS_CACHE.get(pipeline.pipeline_name):
        ROW_COUNTS_CACHE[pipeline.pipeline_name] = {
            i["table_name"]: i["row_count"]
            for i in pipeline.dataset().row_counts(dlt_tables=True).df().to_dict(orient="records")
        }

    # get tables and filter as needed
    tables = list(pipeline.default_schema.tables.values())
    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    table_list: List[Dict[str, Union[str, int, None]]] = [
        {
            "Name": table["name"],
            "Parent": table.get("parent", "-"),
            "Resource": table.get("resource", "-"),
            "Write disposition": table.get("write_disposition", ""),
            "Description": table.get("description", None),
            "Row count": ROW_COUNTS_CACHE.get(pipeline.pipeline_name, {}).get(table["name"], None),
        }
        for table in tables
    ]
    table_list.sort(key=lambda x: str(x["Name"]))
    if not show_internals:
        table_list = [t for t in table_list if not str(t["Name"]).lower().startswith("_dlt")]
    return _align_dict_keys(table_list)


def create_column_list(
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
    table = pipeline.default_schema.tables[table_name]

    column_list: List[Dict[str, Any]] = []
    for column in table["columns"].values():
        column_dict: Dict[str, Any] = {
            "Column name": column["name"],
        }

        # show type hints if requested
        if show_type_hints:
            column_dict["Data Type"] = column.get("data_type", None)
            column_dict["Nullable"] = column.get("nullable", None)
            column_dict["Precision"] = column.get("precision", None)
            column_dict["Scale"] = column.get("scale", None)
            column_dict["Timezone"] = column.get("timezone", None)

        # show "other" hints if requested, TODO: define what are these?
        if show_other_hints:
            column_dict["Primary Key"] = column.get("primary_key", None)
            column_dict["Merge Key"] = column.get("merge_key", None)
            column_dict["Unique"] = column.get("unique", None)

        # show custom hints (x-) if requesed
        if show_custom_hints:
            for key in column:
                if key.startswith("x-"):
                    column_dict[key] = column[key]  # type: ignore

        column_list.append(column_dict)

    column_list.sort(key=lambda x: x["Column name"])
    if not show_internals:
        column_list = [c for c in column_list if not c["Column name"].lower().startswith("_dlt")]
    return _align_dict_keys(column_list)


def pipeline_details(pipeline: dlt.Pipeline) -> List[Dict[str, Any]]:
    """
    Get the details of a pipeline.
    """
    try:
        credentials = str(pipeline.dataset().destination_client.config.credentials)
    except Exception:
        credentials = "Could not resolve credentials"

    details_dict = {
        "Pipeline name": pipeline.pipeline_name,
        "Destination": (
            pipeline.destination.destination_description
            if pipeline.destination
            else "No destination set"
        ),
        "Credentials": credentials,
        "Dataset name": pipeline.dataset_name,
        "Schema name": (
            pipeline.default_schema_name
            if pipeline.default_schema_name
            else "No default schema set"
        ),
        "Pipeline working dir": pipeline.working_dir,
    }

    return [{"Key": k, "Value": v} for k, v in details_dict.items()]


# cache last of query [pipeline_name] = result
LAST_QUERY_RESULT: Dict[str, pd.DataFrame] = {}
LAST_QUERY: Dict[str, str] = {}


@functools.cache
def get_query_result(pipeline: dlt.Pipeline, query: str) -> pd.DataFrame:
    """
    Get the result of a query.
    """

    result = pipeline.dataset()(query).df()
    # remember last result
    global LAST_QUERY_RESULT
    LAST_QUERY_RESULT[pipeline.pipeline_name] = result
    LAST_QUERY[pipeline.pipeline_name] = query

    return result


def get_last_query_result(pipeline: dlt.Pipeline) -> pd.DataFrame:
    """
    Get the last successful query result.
    """
    global LAST_QUERY_RESULT
    return LAST_QUERY_RESULT.get(pipeline.pipeline_name, pd.DataFrame())


def get_last_query(pipeline: dlt.Pipeline) -> str:
    """
    Get the last successful query.
    """
    global LAST_QUERY
    return LAST_QUERY.get(pipeline.pipeline_name, "")


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
