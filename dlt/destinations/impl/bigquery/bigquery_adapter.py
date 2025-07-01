from typing import Any, Optional, Literal, Dict, Union, Sequence

from dateutil import parser

from dlt.common.destination import PreparedTableSchema
from dlt.common.pendulum import timezone
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames
from dlt.destinations.utils import get_resource_for_adapter
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
AUTODETECT_SCHEMA_HINT: Literal["x-bigquery-autodetect-schema"] = "x-bigquery-autodetect-schema"
PARTITION_EXPIRATION_DAYS_HINT: Literal["x-bigquery-partition-expiration-days"] = (
    "x-bigquery-partition-expiration-days"
)
CLUSTER_COLUMNS_HINT: Literal["x-bigquery-cluster-columns"] = "x-bigquery-cluster-columns"


class PartitionTransformation:
    template: str
    """Template string of the transformation including column name placeholder. E.g. `RANGE_BUCKET({column_name}, GENERATE_ARRAY(0, 1000000, 10000))`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, template: str, column_name: str) -> None:
        self.template = template
        self.column_name = column_name


class bigquery_partition:
    """Helper class to generate BigQuery partition transformations."""

    @staticmethod
    def range_bucket(
        column_name: str,
        start: int,
        end: int,
        interval: int = 1,
    ) -> PartitionTransformation:
        """Partition by an integer column with the specified range, where:

        Args:
            column_name: The column to partition by
            start: The start of range partitioning (inclusive)
            end: The end of the range partitioning (exclusive)
            interval: The width of each range within the partition (default: 1)

        Returns:
            A PartitionTransformation object for integer range partitioning
        """

        template = f"RANGE_BUCKET({{column_name}}, GENERATE_ARRAY({start}, {end}, {interval}))"

        return PartitionTransformation(template, column_name)


def bigquery_adapter(
    data: Any,
    partition: Union[TColumnNames, PartitionTransformation] = None,
    cluster: TColumnNames = None,
    round_half_away_from_zero: TColumnNames = None,
    round_half_even: TColumnNames = None,
    table_description: Optional[str] = None,
    table_expiration_datetime: Optional[str] = None,
    insert_api: Optional[Literal["streaming", "default"]] = None,
    autodetect_schema: Optional[bool] = None,
    partition_expiration_days: Optional[int] = None,
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
        partition (Union[TColumnNames, PartitionTransformation], optional): The column to partition the BigQuery table by.
            This can be a string representing a single column name for simple partitioning,
            or a PartitionTransformation object for advanced partitioning.
            Use the bigquery_partition helper class to create transformation objects.
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
        autodetect_schema (bool, optional): If set to True, BigQuery schema autodetection will be used to create data tables. This
            allows to create structured types from nested data.
        partition_expiration_days (int, optional): For date/time based partitions it tells when partition is expired and removed.
            Partitions are expired based on a partitioned column value. (https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration)

    Returns:
        A `DltResource` object that is ready to be loaded into BigQuery.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Raccoon Engineer", "date_hired": 1700784000}]
        >>> bigquery_adapter(data, partition="date_hired", table_expiration_datetime="2024-01-30", table_description="Employee Data")
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    column_hints: TTableSchemaColumns = {}

    if partition:
        if not (isinstance(partition, str) or isinstance(partition, PartitionTransformation)):
            raise ValueError(
                "`partition` must be a single column name as a `str` or a"
                " `PartitionTransformation`."
            )

        # Can only have one partition column.
        for column in resource.columns.values():  # type: ignore[union-attr]
            column.pop(PARTITION_HINT, None)  # type: ignore[typeddict-item]

        if isinstance(partition, str):
            column_hints[partition] = {"name": partition, PARTITION_HINT: True}  # type: ignore[typeddict-unknown-key]

        if isinstance(partition, PartitionTransformation):
            partition_hint: Dict[str, str] = {}
            partition_hint[partition.column_name] = partition.template
            additional_table_hints[PARTITION_HINT] = partition_hint

    if cluster:
        if isinstance(cluster, str):
            cluster = [cluster]
        if not isinstance(cluster, list):
            raise ValueError(
                "`cluster` must be a list of column names or a single column name as a string."
            )
        for column_name in cluster:
            column_hints[column_name] = {"name": column_name, CLUSTER_HINT: True}  # type: ignore[typeddict-unknown-key]
        additional_table_hints[CLUSTER_COLUMNS_HINT] = cluster

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

    if autodetect_schema:
        additional_table_hints[AUTODETECT_SCHEMA_HINT] = autodetect_schema

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
            raise ValueError(
                f"`table_expiration_datetime={table_expiration_datetime}` could not be parsed!"
            ) from e

    if partition_expiration_days is not None:
        assert isinstance(
            partition_expiration_days, int
        ), "partition_expiration_days must be an integer (days)"
        additional_table_hints[PARTITION_EXPIRATION_DAYS_HINT] = partition_expiration_days

    if insert_api is not None:
        if insert_api == "streaming" and data.write_disposition != "append":
            raise ValueError(
                "BigQuery streaming insert only accepts `write_disposition='append'`. "
                f"Received `write_disposition={data.write_disposition}`."
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


def should_autodetect_schema(table: PreparedTableSchema) -> bool:
    """Tells if schema should be auto detected for a given prepared `table`"""
    return table.get(AUTODETECT_SCHEMA_HINT, False)  # type: ignore[return-value]
