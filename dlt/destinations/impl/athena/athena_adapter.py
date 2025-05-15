from typing import Any, Dict, Sequence, Union, Final

from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


PARTITION_HINT: Final[str] = "x-athena-partition"


class PartitionTransformation:
    template: str
    """Template string of the transformation including column name placeholder. E.g. `bucket(16, {column_name})`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, template: str, column_name: str) -> None:
        self.template = template
        self.column_name = column_name


class athena_partition:
    """Helper class to generate iceberg partition transformations

    E.g. `athena_partition.bucket(16, "id")` will return a transformation with template `bucket(16, {column_name})`
    This can be correctly rendered by the athena loader with escaped column name.
    """

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        """Partition by year part of a date or timestamp column."""
        return PartitionTransformation("year({column_name})", column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        """Partition by month part of a date or timestamp column."""
        return PartitionTransformation("month({column_name})", column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        """Partition by day part of a date or timestamp column."""
        return PartitionTransformation("day({column_name})", column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        """Partition by hour part of a date or timestamp column."""
        return PartitionTransformation("hour({column_name})", column_name)

    @staticmethod
    def bucket(n: int, column_name: str) -> PartitionTransformation:
        """Partition by hashed value to n buckets."""
        return PartitionTransformation(f"bucket({n}, {{column_name}})", column_name)

    @staticmethod
    def truncate(length: int, column_name: str) -> PartitionTransformation:
        """Partition by value truncated to length."""
        return PartitionTransformation(f"truncate({length}, {{column_name}})", column_name)


def athena_adapter(
    data: Any,
    partition: Union[
        str, PartitionTransformation, Sequence[Union[str, PartitionTransformation]]
    ] = None,
) -> DltResource:
    """
    Prepares data for loading into Athena

    Args:
        data: The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition: Column name(s) or instances of `PartitionTransformation` to partition the table by.
            To use a transformation it's best to use the methods of the helper class `athena_partition`
            to generate correctly escaped SQL in the loader.

    Returns:
        A `DltResource` object that is ready to be loaded into Athena.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "department": "Engineering", "date_hired": "2024-01-30"}]
        >>> athena_adapter(data, partition=["department", athena_partition.year("date_hired"), athena_partition.bucket(8, "name")])
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if partition:
        if isinstance(partition, str) or not isinstance(partition, Sequence):
            partition = [partition]

        # Partition hint is `{column_name: template}`, e.g. `{"department": "{column_name}", "date_hired": "year({column_name})"}`
        # Use one dict for all hints instead of storing on column so order is preserved
        partition_hint: Dict[str, str] = {}

        for item in partition:
            if isinstance(item, PartitionTransformation):
                # Client will generate the final SQL string with escaped column name injected
                partition_hint[item.column_name] = item.template
            else:
                # Item is the column name
                partition_hint[item] = "{column_name}"

        additional_table_hints[PARTITION_HINT] = partition_hint

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("A value for `partition` must be specified.")
    return resource
