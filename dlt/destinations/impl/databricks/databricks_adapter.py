from typing import Any, Optional, Literal, Dict, List, Union

from dateutil import parser

from dlt.common.destination import PreparedTableSchema
from dlt.common.pendulum import timezone
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


CLUSTER_HINT: Literal["x-databricks-cluster"] = "x-databricks-cluster"
TABLE_COMMENT_HINT: Literal["x-databricks-table-comment"] = "x-databricks-table-comment"
TABLE_TAGS_HINT: Literal["x-databricks-table-tags"] = "x-databricks-table-tags"
COLUMN_COMMENT_HINT: Literal["x-databricks-column-comment"] = "x-databricks-column-comment"
COLUMN_TAGS_HINT: Literal["x-databricks-column-tags"] = "x-databricks-column-tags"

def databricks_adapter(
    data: Any,
    cluster: TColumnNames = None,
    table_comment: Optional[str] = None,
    table_tags: Optional[List[Union[str, Dict[str, str]]]] = None,
    column_hints: Optional[TTableSchemaColumns] = None,
) -> DltResource:
    """
    Prepares data for loading into Databricks.

    This function takes data, which can be raw or already wrapped in a DltResource object,
    and prepares it for Databricks by optionally specifying clustering and table description.

    Args:
        data (Any): The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        cluster (TColumnNames, optional): A column name or list of column names to cluster the Databricks table by.
        table_comment (str, optional): A description for the Databricks table.
        table_tags (List[Union[str, Dict[str, str]]], optional): A list of tags for the Databricks table.
            Can contain a mix of strings and key-value pairs as dictionaries.
            Example: ["production", {"environment": "prod"}, "employees"]

    Returns:
        A `DltResource` object that is ready to be loaded into Databricks.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Raccoon Engineer", "date_hired": 1700784000}]
        >>> databricks_adapter(data, cluster="date_hired", table_comment="Employee Data", 
        ...     table_tags=["production", {"environment": "prod"}, "employees"])
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    additional_column_hints: TTableSchemaColumns = {}

    if cluster:
        if isinstance(cluster, str):
            cluster = [cluster]
        if not isinstance(cluster, list):
            raise ValueError(
                "`cluster` must be a list of column names or a single column name as a string."
            )
        for column_name in cluster:
            additional_column_hints[column_name] = {"name": column_name, CLUSTER_HINT: True}  # type: ignore[typeddict-unknown-key]

    if column_hints:
        for column_name in column_hints.keys():
            additional_column_hints[column_name] = { "name": column_name }  # type: ignore[typeddict-unknown-key]
            if column_hints[column_name].get("column_comment"):
                additional_column_hints[column_name][COLUMN_COMMENT_HINT] = column_hints[column_name].get("column_comment")
            if column_hints[column_name].get("column_tags"):
                if not isinstance(column_hints[column_name].get("column_tags"), list):
                    raise ValueError("`column_tags` must be a list of strings and/or key-value pairs.")
                for tag in column_hints[column_name].get("column_tags"):
                    if isinstance(tag, str):
                        continue
                    elif isinstance(tag, dict) and len(tag) == 1:
                        # Ensure the dictionary has exactly one key-value pair
                        continue
                    else:
                        raise ValueError("Each tag must be either a string or a dictionary with a single key-value pair.")
                additional_column_hints[column_name][COLUMN_TAGS_HINT] = column_hints[column_name].get("column_tags")

    if table_comment:
        if not isinstance(table_comment, str):
            raise ValueError(
                "`table_comment` must be string representing Databricks table description."
            )
        additional_table_hints[TABLE_COMMENT_HINT] = table_comment

    if table_tags:
        if not isinstance(table_tags, list):
            raise ValueError("`table_tags` must be a list of strings and/or key-value pairs.")
        for tag in table_tags:
            if isinstance(tag, str):
                continue
            elif isinstance(tag, dict) and len(tag) == 1:
                # Ensure the dictionary has exactly one key-value pair
                continue
            else:
                raise ValueError("Each tag must be either a string or a dictionary with a single key-value pair.")
        
        additional_table_hints[TABLE_TAGS_HINT] = table_tags

    resource.apply_hints(columns=additional_column_hints, additional_table_hints=additional_table_hints)

    return resource