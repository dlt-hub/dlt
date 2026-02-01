from typing import Any, Optional, Literal, Dict, List, Union, cast

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate
from dlt.destinations.impl.databricks.typing import TDatabricksTableSchemaColumns


CLUSTER_HINT: Literal["x-databricks-cluster"] = "x-databricks-cluster"
TABLE_COMMENT_HINT: Literal["x-databricks-table-comment"] = "x-databricks-table-comment"
TABLE_TAGS_HINT: Literal["x-databricks-table-tags"] = "x-databricks-table-tags"
TABLE_PROPERTIES_HINT: Literal["x-databricks-table-properties"] = "x-databricks-table-properties"
COLUMN_COMMENT_HINT: Literal["x-databricks-column-comment"] = "x-databricks-column-comment"
COLUMN_TAGS_HINT: Literal["x-databricks-column-tags"] = "x-databricks-column-tags"


def databricks_adapter(
    data: Any,
    cluster: Union[TColumnNames, Literal["AUTO"]] = None,
    partition: TColumnNames = None,
    table_format: Literal["DELTA", "ICEBERG"] = "DELTA",
    table_comment: Optional[str] = None,
    table_tags: Optional[List[Union[str, Dict[str, str]]]] = None,
    table_properties: Optional[Dict[str, Union[str, int, bool, float]]] = None,
    column_hints: Optional[TDatabricksTableSchemaColumns] = None,
) -> DltResource:
    """
    Prepares data for loading into Databricks.

    This function takes data, which can be raw or already wrapped in a DltResource object,
    and prepares it for Databricks by optionally specifying clustering, partitioning, and table description.

    Args:
        data (Any): The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        cluster (Union[TColumnNames, Literal["AUTO"]], optional): A column name, list of column names, or "AUTO" to cluster the Databricks table by.
            Use "AUTO" to let Databricks automatically determine the best clustering.
        partition (TColumnNames, optional): A column name or list of column names to partition the Databricks table by.
            Partitioning divides the table into separate files based on the partition column values.
        table_format (Literal["DELTA", "ICEBERG"], optional): The table format to use. Defaults to "DELTA".
            Use "ICEBERG" to create Apache Iceberg tables for better schema evolution and time travel capabilities.
        table_comment (str, optional): A description for the Databricks table.
        table_tags (List[Union[str, Dict[str, str]]], optional): A list of tags for the Databricks table.
            Can contain a mix of strings and key-value pairs as dictionaries.
            Example: ["production", {"environment": "prod"}, "employees"]
        table_properties (Dict[str, Union[str, int, bool, float]], optional): A dictionary of table properties
            to be added to the Databricks table using TBLPROPERTIES. These are key-value pairs for metadata
            and Delta Lake optimization settings. Example: {"delta.appendOnly": True, "delta.logRetentionDuration": "30 days"}
        column_hints (TTableSchemaColumns, optional): A dictionary of column hints.
            Each key is a column name, and the value is a dictionary of hints.
            The supported hints are:
            - `column_comment` - adds a comment to the column. Supports basic markdown format [basic-syntax](https://www.markdownguide.org/cheat-sheet/#basic-syntax).
            - `column_tags` - adds tags to the column. Supports a list of strings and/or key-value pairs.

    Returns:
        A `DltResource` object that is ready to be loaded into Databricks.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Raccoon Engineer", "date_hired": 1700784000}]
        >>> databricks_adapter(data, cluster="date_hired", table_comment="Employee Data",
        ...     table_tags=["production", {"environment": "prod"}, "employees"])
        >>> # Use AUTO clustering
        >>> databricks_adapter(data, cluster="AUTO", table_comment="Auto-clustered table")
        >>> # Use partitioning
        >>> databricks_adapter(data, partition=["year", "month"], cluster="customer_id")
        >>> # Create Iceberg table
        >>> databricks_adapter(data, table_format="ICEBERG", cluster="customer_id")
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    additional_column_hints: TDatabricksTableSchemaColumns = {}

    # Handle table format
    if table_format not in ["DELTA", "ICEBERG"]:
        raise ValueError("`table_format` must be either 'DELTA' or 'ICEBERG'.")

    # Store table format at table level (lowercase to match destination format)
    additional_table_hints["table_format"] = table_format.lower()

    if cluster:
        if cluster == "AUTO":
            # Handle AUTO clustering at table level
            additional_table_hints[CLUSTER_HINT] = "AUTO"
        else:
            # Handle specific column clustering
            if isinstance(cluster, str):
                cluster = [cluster]
            if not isinstance(cluster, list):
                raise ValueError(
                    "`cluster` must be a list of column names, a single column name as a string, or"
                    " 'AUTO'."
                )
            for column_name in cluster:
                additional_column_hints[column_name] = {"name": column_name, CLUSTER_HINT: True}  # type: ignore[typeddict-unknown-key]

    if partition:
        if isinstance(partition, str):
            partition = [partition]
        if not isinstance(partition, list):
            raise ValueError(
                "`partition` must be a list of column names or a single column name as a string."
            )
        for column_name in partition:
            if column_name not in additional_column_hints:
                additional_column_hints[column_name] = {"name": column_name}
            additional_column_hints[column_name]["partition"] = True

    if column_hints:
        for column_name, hints in column_hints.items():
            if column_name not in additional_column_hints:
                additional_column_hints[column_name] = {"name": column_name}

            # cast to a generic dict to access keys not in TColumnSchema
            hints_dict = cast(Dict[str, Any], hints)
            if "column_comment" in hints_dict:
                additional_column_hints[column_name][COLUMN_COMMENT_HINT] = hints_dict[  # type: ignore[typeddict-unknown-key]
                    "column_comment"
                ]
            if "column_tags" in hints_dict:
                column_tags = hints_dict["column_tags"]
                if not isinstance(column_tags, list):
                    raise ValueError(
                        "`column_tags` must be a list of strings and/or key-value pairs."
                    )
                for tag in column_tags:
                    if isinstance(tag, str):
                        continue
                    if isinstance(tag, dict) and len(tag) == 1:
                        # Ensure the dictionary has exactly one key-value pair
                        continue
                    raise ValueError(
                        "Each tag must be either a string or a dictionary with a single key-value"
                        " pair."
                    )
                additional_column_hints[column_name][COLUMN_TAGS_HINT] = column_tags  # type: ignore[typeddict-unknown-key]

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
                raise ValueError(
                    "Each tag must be either a string or a dictionary with a single key-value pair."
                )

        additional_table_hints[TABLE_TAGS_HINT] = table_tags

    if table_properties:
        if not isinstance(table_properties, dict):
            raise ValueError("`table_properties` must be a dictionary of key-value pairs.")

        # Reserved keys that should not be used in TBLPROPERTIES
        reserved_keys = {"external", "location", "owner", "provider"}

        for key, value in table_properties.items():
            if not isinstance(key, str):
                raise ValueError("Table property keys must be strings.")

            # Check for reserved keys
            if key.lower() in reserved_keys:
                raise ValueError(
                    f"Table property key '{key}' is reserved and cannot be used. "
                    f"Reserved keys are: {', '.join(reserved_keys)}"
                )

            # Check for keys starting with 'option.'
            if key.startswith("option."):
                raise ValueError(
                    f"Table property key '{key}' starts with 'option.' which is reserved."
                )

            # Validate value types
            if not isinstance(value, (str, int, bool, float)):
                raise ValueError(
                    f"Table property value for key '{key}' must be a string, integer, boolean, or"
                    f" float. Got {type(value).__name__}."
                )

        additional_table_hints[TABLE_PROPERTIES_HINT] = table_properties

    resource.apply_hints(
        columns=cast(TTableSchemaColumns, additional_column_hints),
        additional_table_hints=additional_table_hints,
    )

    return resource
