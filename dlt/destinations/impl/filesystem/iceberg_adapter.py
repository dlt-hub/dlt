from dataclasses import dataclass
from typing import Any, List, Dict, Union, Sequence, Optional, cast

from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource

PARTITION_HINT = "x-iceberg-partition"
TABLE_PROPERTIES_HINT = "x-iceberg-table-properties"


@dataclass(frozen=True)
class PartitionSpec:
    source_column: str
    transform: str = "identity"
    param_value: Optional[int] = None
    partition_field: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "transform": self.transform,
            "source_column": self.source_column,
        }
        if self.partition_field:
            d["partition_field"] = self.partition_field
        if self.param_value is not None:
            d["param_value"] = self.param_value
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PartitionSpec":
        return cls(
            source_column=d["source_column"],
            transform=d["transform"],
            param_value=d.get("param_value"),
            partition_field=d.get("partition_field"),
        )


class iceberg_partition:
    """Helper class with factory methods for creating partition specs."""

    @staticmethod
    def identity(column_name: str) -> PartitionSpec:
        """Create an identity partition on a column.

        Args:
            column_name: The name of the column to partition on

        Returns:
            A PartitionSpec for identity partitioning
        """
        return PartitionSpec(column_name, "identity")

    @staticmethod
    def year(column_name: str, partition_field_name: Optional[str] = None) -> PartitionSpec:
        """Create a year partition on a timestamp/date column.

        Args:
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for year partitioning
        """
        return PartitionSpec(column_name, "year", partition_field=partition_field_name)

    @staticmethod
    def month(column_name: str, partition_field_name: Optional[str] = None) -> PartitionSpec:
        """Create a month partition on a timestamp/date column.

        Args:
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for month partitioning
        """
        return PartitionSpec(column_name, "month", partition_field=partition_field_name)

    @staticmethod
    def day(column_name: str, partition_field_name: Optional[str] = None) -> PartitionSpec:
        """Create a day partition on a timestamp/date column.

        Args:
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for day partitioning
        """
        return PartitionSpec(column_name, "day", partition_field=partition_field_name)

    @staticmethod
    def hour(column_name: str, partition_field_name: Optional[str] = None) -> PartitionSpec:
        """Create an hour partition on a timestamp column.

        Args:
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for hour partitioning
        """
        return PartitionSpec(column_name, "hour", partition_field=partition_field_name)

    @staticmethod
    def bucket(
        num_buckets: int, column_name: str, partition_field_name: Optional[str] = None
    ) -> PartitionSpec:
        """Create a bucket partition on a column.

        Args:
            num_buckets: The number of buckets to create
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for bucket partitioning
        """
        return PartitionSpec(
            source_column=column_name,
            transform="bucket",
            param_value=num_buckets,
            partition_field=partition_field_name,
        )

    @staticmethod
    def truncate(
        width: int, column_name: str, partition_field_name: Optional[str] = None
    ) -> PartitionSpec:
        """Create a truncate partition on a string column.

        Args:
            width: The width to truncate to
            column_name: The name of the column to partition on
            partition_field_name: Optional custom name for the partition field

        Returns:
            A PartitionSpec for truncate partitioning
        """
        return PartitionSpec(
            source_column=column_name,
            transform="truncate",
            param_value=width,
            partition_field=partition_field_name,
        )


def iceberg_adapter(
    data: Any,
    partition: Union[str, PartitionSpec, Sequence[Union[str, PartitionSpec]]] = None,
    table_properties: Optional[Dict[str, str]] = None,
) -> DltResource:
    """Prepares data or a DltResource for loading into Apache Iceberg table.

    Takes raw data or an existing DltResource and configures it for Iceberg
    by defining partitioning strategies and/or table properties via the
    DltResource's hints.

    Args:
        data: The data to be transformed. This can be raw data (e.g., list of dicts)
            or an instance of `DltResource`. If raw data is provided, it will be
            encapsulated into a `DltResource` instance.
        partition: Defines how the Iceberg table should be partitioned.
            It accepts:
            - A single column name (string): Defaults to an identity transform.
            - A `PartitionSpec` object: Allows for detailed partition configuration,
              including transformation types (year, month, day, hour, bucket, truncate).
              Use the `iceberg_partition` helper class to create these specs.
            - A sequence of the above: To define multiple partition columns.
        table_properties: A dictionary of Iceberg table properties to set on the
            table at creation time. Keys and values must be strings. These correspond
            to Iceberg table properties such as ``write.format.default`` or
            ``write.target-file-size-bytes``. Properties are only applied when the
            table is first created.

    Returns:
        A `DltResource` instance configured with Iceberg-specific hints,
        ready for loading.

    Raises:
        ValueError: If neither `partition` nor `table_properties` is specified,
            or if an invalid partition transform is requested within a
            `PartitionSpec`, or if `table_properties` is not a dict with string
            keys and values.

    Examples:
        >>> data = [{"id": 1, "event_time": "2023-03-15T10:00:00Z", "category": "A"}]
        >>> resource = iceberg_adapter(
        ...     data,
        ...     partition=[
        ...         "category",  # Identity partition on category
        ...         iceberg_partition.year("event_time"),
        ...     ],
        ...     table_properties={"write.format.default": "parquet"},
        ... )
    """
    resource = get_resource_for_adapter(data)
    additional_table_hints: Dict[str, Any] = {}

    if partition:
        if isinstance(partition, (str, PartitionSpec)):
            partition = [partition]

        specs: List[PartitionSpec] = []
        for item in partition:
            if isinstance(item, PartitionSpec):
                specs.append(item)
            else:
                # item is the column name, use identity transform
                specs.append(iceberg_partition.identity(item))

        additional_table_hints[PARTITION_HINT] = [spec.to_dict() for spec in specs]

    if table_properties is not None:
        if not isinstance(table_properties, dict):
            raise ValueError("`table_properties` must be a dictionary.")
        for key, value in table_properties.items():
            if not isinstance(key, str):
                raise ValueError("Table property keys must be strings.")
            if not isinstance(value, str):
                raise ValueError(
                    f"Table property values must be strings. Got {type(value).__name__}"
                    f" for key '{key}'."
                )
        additional_table_hints[TABLE_PROPERTIES_HINT] = table_properties

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("At least one of `partition` or `table_properties` must be specified.")

    return resource


def parse_partition_hints(table_schema: PreparedTableSchema) -> List[PartitionSpec]:
    """Parse PARTITION_HINT from table schema into PartitionSpec list.

    Args:
        table_schema: dlt table schema containing partition hints

    Returns:
        List of PartitionSpec objects from hints, empty list if no hints found
    """
    partition_hints = cast(List[Dict[str, Any]], table_schema.get(PARTITION_HINT, []))
    specs = []
    for spec_data in partition_hints:
        spec = PartitionSpec.from_dict(spec_data)
        specs.append(spec)
    return specs


def create_identity_specs(column_names: List[str]) -> List[PartitionSpec]:
    """Create identity partition specs from column names.

    Args:
        column_names: List of column names to partition by identity

    Returns:
        List of PartitionSpec objects with identity transform
    """
    return [iceberg_partition.identity(column_name) for column_name in column_names]


def get_column_descriptions(table_schema: PreparedTableSchema) -> Dict[str, str]:
    """Extracts column descriptions from dlt table schema columns.

    Args:
        table_schema: The dlt prepared table schema containing column definitions.

    Returns:
        A dictionary mapping column names to their description strings.
        Columns without descriptions are omitted.
    """
    descriptions: Dict[str, str] = {}
    for col_name, col in table_schema.get("columns", {}).items():
        desc = col.get("description")
        if desc:
            descriptions[col_name] = desc

    return descriptions
