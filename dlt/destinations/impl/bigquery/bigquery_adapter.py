from typing import Any, Optional

from dateutil import parser

from dlt.common import pendulum
from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.extract import DltResource, resource as make_resource


PARTITION_HINT = "x-bigquery-partition"
CLUSTER_HINT = "x-bigquery-cluster"
EXPIRATION_HINT = "x-bigquery-expiration"


def bigquery_adapter(
    data: Any,
    partition: TColumnNames = None,
    cluster: TColumnNames = None,
    table_expiration_datetime: Optional[str] = None,
) -> DltResource:
    """
    Prepares data for loading into BigQuery.

    This function takes data, which can be raw or already wrapped in a DltResource object,
    and prepares it for BigQuery by optionally specifying partitioning, clustering, and
    table expiration settings.

    Args:
        data (Any): The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition (TColumnNames, optional): The name of the column to partition the BigQuery table by.
            This should be a string representing a single column name.
        cluster (TColumnNames, optional): A column name or list of column names to cluster the BigQuery table by.
        table_expiration_datetime (str, optional): String representing the datetime when the BigQuery table expires.

    Returns:
        A `DltResource` object that is ready to be loaded into BigQuery.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Alice", "description": "Software developer", "date_hired": pendulum.from_timestamp(1700784000)}]
        >>> bigquery_adapter(data, partition="date_hired", table_expiration_datetime="2024-01-30")
        [DltResource with hints applied]
    """
    # Wrap `data` in a resource if not an instance already.
    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name = None if hasattr(data, "__name__") else "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data

    column_hints: TTableSchemaColumns = {}

    if partition:
        if not isinstance(partition, str):
            raise ValueError("`partition` must be a single column name as a string.")
        column_hints[partition] = {
            "name": partition,
            PARTITION_HINT: True,  # type: ignore
        }

    if cluster:
        if isinstance(cluster, str):
            cluster = [cluster]
        if not isinstance(cluster, list):
            raise ValueError(
                "`cluster` must be a list of column names or a single column name as a string."
            )
        for column_name in cluster:
            column_hints[column_name] = {
                "name": column_name,
                CLUSTER_HINT: True,  # type: ignore
            }

    if table_expiration_datetime:
        if not isinstance(table_expiration_datetime, str):
            raise ValueError(
                "`expiration` must be string representing the datetime when the BigQuery table."
            )
        try:
            parser.parse(table_expiration_datetime)
        except ValueError as e:
            raise ValueError(f"{table_expiration_datetime} could not be parsed.") from e

        raise NotImplementedError

    if not column_hints:
        raise ValueError(
            "At least one of `partition`, `cluster` or `table_expiration_datetime` must be"
            " specified."
        )
    else:
        resource.apply_hints(columns=column_hints)
    return resource
