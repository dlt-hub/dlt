from typing import Any, Optional, Dict, Protocol, Sequence, Union, Final

from dateutil import parser

from dlt.common.pendulum import timezone
from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns, TColumnSchema
from dlt.destinations.utils import ensure_resource
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


PARTITION_HINT: Final[str] = "x-athena-partition"


class athena_partition:
    """Helper class to generate iceberg partition transform strings.

    E.g. `athena_partition.bucket(16, "id")` will return `bucket(16, "id")`.
    """

    @staticmethod
    def year(column_name: str) -> str:
        """Partition by year part of a date or timestamp column."""
        return f"year({column_name})"

    @staticmethod
    def month(column_name: str) -> str:
        """Partition by month part of a date or timestamp column."""
        return f"month({column_name})"

    @staticmethod
    def day(column_name: str) -> str:
        """Partition by day part of a date or timestamp column."""
        return f"day({column_name})"

    @staticmethod
    def hour(column_name: str) -> str:
        """Partition by hour part of a date or timestamp column."""
        return f"hour({column_name})"

    @staticmethod
    def bucket(n: int, column_name: str) -> str:
        """Partition by hashed value to n buckets."""
        return f"bucket({n}, {column_name})"

    @staticmethod
    def truncate(length: int, column_name: str) -> str:
        """Partition by value truncated to length."""
        return f"truncate({length}, {column_name})"


def athena_adapter(
    data: Any,
    partition: Union[str, Sequence[str]] = None,
) -> DltResource:
    """
    Prepares data for loading into Athena

    Args:
        data: The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition: Column name(s) partition transform string(s) to partition table by

    Returns:
        A `DltResource` object that is ready to be loaded into BigQuery.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "department": "Engineering", "date_hired": "2024-01-30"}]
        >>> athena_adapter(data, partition=["department", athena_partition.year("date_hired"), athena_partition.bucket(8, "name")])
        [DltResource with hints applied]
    """
    resource = ensure_resource(data)
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if partition:
        if isinstance(partition, str):
            partition = [partition]

        # Note: PARTITIONED BY clause identifiers are not allowed to be quoted. They are added as-is.
        additional_table_hints[PARTITION_HINT] = list(partition)

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("A value for `partition` must be specified.")
    return resource
