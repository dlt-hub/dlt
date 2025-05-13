from typing import List, Tuple, Any, Dict, Mapping

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


def create_table_list(
    pipeline_name: str, show_internals: bool = False, show_child_tables: bool = True
) -> List[dict[str, str]]:
    """Create a list of tables for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline to create the table list for.
    """

    tables = list(get_pipeline(pipeline_name).default_schema.tables.values())

    if not show_child_tables:
        tables = [t for t in tables if t.get("parent") is None]

    table_list = [
        {
            "Name": table["name"],
            "Parent": table.get("parent", "-"),
            "Resource": table.get("resource", "-"),
            "Write disposition": table.get("write_disposition", ""),
            "Description": table.get("description", ""),
        }
        for table in tables
    ]
    table_list.sort(key=lambda x: x["Name"])
    if not show_internals:
        table_list = [t for t in table_list if not t["Name"].lower().startswith("_dlt")]
    return table_list


def create_column_list(
    pipeline_name: str,
    table_name: str,
    show_internals: bool = False,
    show_type_hints: bool = True,
    show_other_hints: bool = False,
    show_custom_hints: bool = False,
) -> List[Mapping[str, Any]]:
    """Create a list of columns for a table.

    Args:
        pipeline_name (str): The name of the pipeline to create the column list for.
        table_name (str): The name of the table to create the column list for.
    """
    table = get_pipeline(pipeline_name).default_schema.tables[table_name]

    column_list: List[Mapping[str, Any]] = []
    for column in table["columns"].values():
        column_dict: Dict[str, Any] = {
            "Name": column["name"],
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

        column_list.append(without_none(column_dict))

    column_list.sort(key=lambda x: x["Name"])
    if not show_internals:
        column_list = [c for c in column_list if not c["Name"].lower().startswith("_dlt")]
    return column_list


def style_cell(row_id: str, name: str, __: Any) -> Dict[str, str]:
    style = {"background-color": "white" if (int(row_id) % 2 == 0) else "#f4f4f9"}
    if name.lower() == "name":
        style["font-weight"] = "bold"
    return style
