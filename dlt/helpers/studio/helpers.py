from typing import List, Tuple, Any, Dict, Dict, cast
from itertools import chain
import dlt

from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages import FileStorage
from dlt.common.utils import without_none


def get_local_pipelines(pipelines_dir: str = None) -> Tuple[str, List[str]]:
    """Get the local pipelines directory and the list of pipeline names in it.

    Args:
        pipelines_dir (str, optional): The local pipelines directory. Defaults to get_dlt_pipelines_dir().

    Returns:
        Tuple[str, List[str]]: The local pipelines directory and the list of pipeline names in it.
    """
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)
    dirs = storage.list_folder_dirs(".", to_root=False)
    return pipelines_dir, dirs


def get_pipeline(pipeline_name: str) -> dlt.Pipeline:
    """Get a pipeline by name.

    Args:
        pipeline_name (str): The name of the pipeline to get.

    Returns:
        dlt.Pipeline: The pipeline.
    """
    return dlt.attach(pipeline_name)


def _align_dict_keys(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Makes sure all dicts have the same keys, sets "-" as default. Makes for nicer rendering in marimo table
    """
    items = cast(List[Dict[str, Any]], [without_none(i) for i in items])
    all_keys = set(chain.from_iterable(i.keys() for i in items))
    for i in items:
        i.update({key: "-" for key in all_keys if key not in i})
    return items


def create_table_list(
    pipeline: dlt.Pipeline, show_internals: bool = False, show_child_tables: bool = True
) -> List[Dict[str, str]]:
    """Create a list of tables for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline to create the table list for.
    """

    tables = list(pipeline.default_schema.tables.values())

    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    table_list = [
        {
            "Name": table["name"],
            "Parent": table.get("parent", "-"),
            "Resource": table.get("resource", "-"),
            "Write disposition": table.get("write_disposition", ""),
            "Description": table.get("description", None),
        }
        for table in tables
    ]
    table_list.sort(key=lambda x: x["Name"])
    if not show_internals:
        table_list = [t for t in table_list if not t["Name"].lower().startswith("_dlt")]
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
