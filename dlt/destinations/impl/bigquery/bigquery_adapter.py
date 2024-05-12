from typing import Any, Optional, Literal, Dict

from dateutil import parser

from dlt.common.pendulum import timezone
from dlt.common.schema.typing import (
    TColumnNames,
    TTableSchemaColumns,
)
from dlt.destinations.utils import ensure_resource
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


PARTITION_HINT: Literal["x-bigquery-partition"] = "x-bigquery-partition"
CLUSTER_HINT: Literal["x-bigquery-cluster"] = "x-bigquery-cluster"
ROUND_HALF_AWAY_FROM_ZERO_HINT: Literal["x-bigquery-round-half-away-from-zero"] = (
    "x-bigquery-round-half-away-from-zero"
)
ROUND_HALF_EVEN_HINT: Literal["x-bigquery-round-half-even"] = "x-bigquery-round-half-even"
TABLE_EXPIRATION_HINT: Literal["x-bigquery-table-expiration"] = "x-bigquery-table-expiration"
TABLE_DESCRIPTION_HINT: Literal["x-bigquery-table-description"] = "x-bigquery-table-description"


def bigquery_adapter(
    data: Any,
    partition: TColumnNames = None,
    cluster: TColumnNames = None,
    round_half_away_from_zero: TColumnNames = None,
    round_half_even: TColumnNames = None,
    table_description: Optional[str] = None,
    table_expiration_datetime: Optional[str] = None,
    insert_api: Optional[Literal["streaming", "default"]] = None,
) -> DltResource:
    """
    Prepares data for loading into BigQuery.

    This function takes data, which can be raw or already wrapped in a DltResource object,
    and prepares it for BigQuery by optionally specifying partitioning, clustering, table description and
    table expiration settings.

    Args:
        data (Any): The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition (TColumnNames, optional): The name of the column to partition the BigQuery table by.
            This should be a string representing a single column name.
        cluster (TColumnNames, optional): A column name or list of column names to cluster the BigQuery table by.
        round_half_away_from_zero (TColumnNames, optional): Determines how values in the column are rounded when written to the table.
            This mode rounds halfway cases away from zero.
            The columns specified must be mutually exclusive from `round_half_even`.
            See https://cloud.google.com/bigquery/docs/schemas#rounding_mode for more information.
        round_half_even (TColumnNames, optional): Determines how values in the column are rounded when written to the table.
            This mode rounds halfway cases towards the nearest even digit.
            The columns specified must be mutually exclusive from `round_half_away_from_zero`.
            See https://cloud.google.com/bigquery/docs/schemas#rounding_mode for more information.
        table_description (str, optional): A description for the BigQuery table.
        table_expiration_datetime (str, optional): String representing the datetime when the BigQuery table expires.
            This is always interpreted as UTC, BigQuery's default.
        insert_api (Optional[Literal["streaming", "default"]]): The API to use for inserting data into BigQuery.
            If "default" is chosen, the original SQL query mechanism is used.
            If "streaming" is chosen, the streaming API (https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery)
            is used.
            NOTE: due to BigQuery features, streaming insert is only available for `append` write_disposition.

    Returns:
        A `DltResource` object that is ready to be loaded into BigQuery.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Raccoon Engineer", "date_hired": 1700784000}]
        >>> bigquery_adapter(data, partition="date_hired", table_expiration_datetime="2024-01-30", table_description="Employee Data")
        [DltResource with hints applied]
    """
    resource = ensure_resource(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    column_hints: TTableSchemaColumns = {}

    if partition:
        if not isinstance(partition, str):
            raise ValueError("`partition` must be a single column name as a string.")

        # Can only have one partition column.
        for column in resource.columns.values():  # type: ignore[union-attr]
            column.pop(PARTITION_HINT, None)  # type: ignore[typeddict-item]
        column_hints[partition] = {"name": partition, PARTITION_HINT: True}  # type: ignore[typeddict-unknown-key]

    if cluster:
        if isinstance(cluster, str):
            cluster = [cluster]
        if not isinstance(cluster, list):
            raise ValueError(
                "`cluster` must be a list of column names or a single column name as a string."
            )
        for column_name in cluster:
            column_hints[column_name] = {"name": column_name, CLUSTER_HINT: True}  # type: ignore[typeddict-unknown-key]

    # Implementing rounding logic flags
    if round_half_away_from_zero:
        if isinstance(round_half_away_from_zero, str):
            round_half_away_from_zero = [round_half_away_from_zero]
        if not isinstance(round_half_away_from_zero, list):
            raise ValueError(
                "`round_half_away_from_zero` must be a list of column names or a single column"
                " name."
            )
        for column_name in round_half_away_from_zero:
            column_hints[column_name] = {"name": column_name, ROUND_HALF_AWAY_FROM_ZERO_HINT: True}  # type: ignore[typeddict-unknown-key]

    if round_half_even:
        if isinstance(round_half_even, str):
            round_half_even = [round_half_even]
        if not isinstance(round_half_even, list):
            raise ValueError(
                "`round_half_even` must be a list of column names or a single column name."
            )
        for column_name in round_half_even:
            column_hints[column_name] = {"name": column_name, ROUND_HALF_EVEN_HINT: True}  # type: ignore[typeddict-unknown-key]

    if round_half_away_from_zero and round_half_even:
        if intersection_columns := set(round_half_away_from_zero).intersection(
            set(round_half_even)
        ):
            raise ValueError(
                f"Columns `{intersection_columns}` are present in both `round_half_away_from_zero`"
                " and `round_half_even` which is not allowed. They must be mutually exclusive."
            )

    if table_description:
        if not isinstance(table_description, str):
            raise ValueError(
                "`table_description` must be string representing BigQuery table description."
            )
        additional_table_hints[TABLE_DESCRIPTION_HINT] = table_description

    if table_expiration_datetime:
        if not isinstance(table_expiration_datetime, str):
            raise ValueError(
                "`table_expiration_datetime` must be string representing the datetime when the"
                " BigQuery table will be deleted."
            )
        try:
            parsed_table_expiration_datetime = parser.parse(table_expiration_datetime).replace(
                tzinfo=timezone.utc
            )
            additional_table_hints[TABLE_EXPIRATION_HINT] = parsed_table_expiration_datetime
        except ValueError as e:
            raise ValueError(f"{table_expiration_datetime} could not be parsed!") from e

    if insert_api is not None:
        if insert_api == "streaming" and data.write_disposition != "append":
            raise ValueError(
                "BigQuery streaming insert can only be used with `append` write_disposition, while "
                f"the given resource has `{data.write_disposition}`."
            )
        additional_table_hints["x-insert-api"] = insert_api

    if column_hints or additional_table_hints:
        resource.apply_hints(columns=column_hints, additional_table_hints=additional_table_hints)
    else:
        raise ValueError(
            "AT LEAST one of `partition`, `cluster`, `round_half_away_from_zero`,"
            " `round_half_even`, `table_description` or `table_expiration_datetime` must be"
            " specified."
        )
    return resource
