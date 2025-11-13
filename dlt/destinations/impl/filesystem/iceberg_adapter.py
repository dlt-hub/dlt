from dataclasses import dataclass
from typing import Any, List, Dict, Union, Sequence, Optional, Callable, cast

from dlt import version
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.typing import PreparedTableSchema

try:
    from pyiceberg.transforms import (
        Transform,
        IdentityTransform,
        YearTransform,
        MonthTransform,
        DayTransform,
        HourTransform,
        BucketTransform,
        TruncateTransform,
        S,
    )
    from pyiceberg.partitioning import (
        PartitionSpec as IcebergPartitionSpec,
        PartitionField,
        PARTITION_FIELD_ID_START,
    )
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.table.name_mapping import NameMapping, MappedField
except ImportError:
    raise MissingDependencyException(
        "dlt iceberg adapter",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` for dlt iceberg adapter to work",
    )

from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource

PARTITION_HINT = "x-iceberg-partition"

_TRANSFORM_LOOKUP: Dict[str, Callable[[Optional[int]], Transform[S, Any]]] = {
    "identity": lambda _: IdentityTransform(),
    "year": lambda _: YearTransform(),
    "month": lambda _: MonthTransform(),
    "day": lambda _: DayTransform(),
    "hour": lambda _: HourTransform(),
    "bucket": lambda n: BucketTransform(n),
    "truncate": lambda w: TruncateTransform(w),
}


@dataclass(frozen=True)
class PartitionSpec:
    source_column: str
    transform: str = "identity"
    param_value: Optional[int] = None
    partition_field: Optional[str] = None

    def get_transform(self) -> Transform[S, Any]:
        """Get the PyIceberg Transform object for this partition.

        Returns:
            A PyIceberg Transform object

        Raises:
            ValueError: If the transform is not recognized
        """
        try:
            factory = _TRANSFORM_LOOKUP[self.transform]
        except KeyError as exc:
            raise ValueError(f"Unknown partition transformation type: {self.transform}") from exc
        return factory(self.param_value)

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
) -> DltResource:
    """Prepares data or a DltResource for loading into Apache Iceberg table.

    Takes raw data or an existing DltResource and configures it for Iceberg,
    primarily by defining partitioning strategies via the DltResource's hints.

    Args:
        data: The data to be transformed. This can be raw data (e.g., list of dicts)
            or an instance of `DltResource`. If raw data is provided, it will be
            encapsulated into a `DltResource` instance.
        partition: Defines how the Iceberg table should be partitioned.
            Must be provided. It accepts:
            - A single column name (string): Defaults to an identity transform.
            - A `PartitionSpec` object: Allows for detailed partition configuration,
              including transformation types (year, month, day, hour, bucket, truncate).
              Use the `iceberg_partition` helper class to create these specs.
            - A sequence of the above: To define multiple partition columns.

    Returns:
        A `DltResource` instance configured with Iceberg-specific partitioning hints,
        ready for loading.

    Raises:
        ValueError: If `partition` is not specified or if an invalid
            partition transform is requested within a `PartitionSpec`.

    Examples:
        >>> data = [{"id": 1, "event_time": "2023-03-15T10:00:00Z", "category": "A"}]
        >>> resource = iceberg_adapter(
        ...     data,
        ...     partition=[
        ...         "category",  # Identity partition on category
        ...         iceberg_partition.year("event_time"),
        ...     ]
        ... )
        >>> # The resource's hints now contain the Iceberg partition specs:
        >>> # resource.compute_table_schema().get('x-iceberg-partition')
        >>> # [
        >>> #     {'transform': 'identity', 'source_column': 'event_time'},
        >>> #     {'transform': 'year', 'source_column': 'event_time'},
        >>> # ]
        >>> #
        >>> # Or in case of using an existing DltResource
        >>> @dlt.resource
        ... def my_data():
        ...     yield [{"value": "abc"}]
        >>> iceberg_adapter(my_data, partition="value")
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
                # Item is the column name, use identity transform
                specs.append(iceberg_partition.identity(item))

        additional_table_hints[PARTITION_HINT] = [spec.to_dict() for spec in specs]

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("A value for `partition` must be specified.")

    return resource


def _default_field_name(spec: PartitionSpec) -> str:
    """
    Replicate Iceberg's automatic partition-field naming by delegating to the private
    _PartitionNameGenerator. Falls back to the user-supplied `partition_field` if present.
    """
    from pyiceberg.partitioning import _PartitionNameGenerator

    name_generator = _PartitionNameGenerator()

    if spec.partition_field:  # user-supplied `partition_field`
        return spec.partition_field

    # name generator requires field_id and source_id, but does not use them
    dummy_field_id = 0
    dummy_source_id = 0

    if spec.transform == "bucket":
        # bucket / truncate need the numeric parameter
        return name_generator.bucket(
            dummy_field_id, spec.source_column, dummy_source_id, spec.param_value
        )
    if spec.transform == "truncate":
        return name_generator.truncate(
            dummy_field_id, spec.source_column, dummy_source_id, spec.param_value
        )

    # identity, year, month, day, hour – all have the same signature
    method = getattr(name_generator, spec.transform)

    return method(dummy_field_id, spec.source_column, dummy_source_id)  # type: ignore[no-any-return]


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


def build_iceberg_partition_spec(
    arrow_schema: pa.Schema,
    spec_list: Sequence[PartitionSpec],
) -> tuple[IcebergPartitionSpec, IcebergSchema]:
    """
    Turn a dlt PartitionSpec list into a PyIceberg PartitionSpec.
    Returns the PartitionSpec and the IcebergSchema derived from the Arrow schema.
    """
    name_mapping = NameMapping(
        [
            MappedField(field_id=i + 1, names=[name])  # type: ignore[call-arg]
            for i, name in enumerate(arrow_schema.names)
        ]
    )
    iceberg_schema: IcebergSchema = pyarrow_to_schema(arrow_schema, name_mapping)

    fields: list[PartitionField] = []
    for pos, spec in enumerate(spec_list):
        iceberg_field = iceberg_schema.find_field(spec.source_column)

        fields.append(
            PartitionField(
                field_id=PARTITION_FIELD_ID_START + pos,
                source_id=iceberg_field.field_id,
                transform=spec.get_transform(),
                name=_default_field_name(spec),
            )
        )

    return IcebergPartitionSpec(*fields), iceberg_schema
