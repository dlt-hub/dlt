from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, Dict, Type, TypeVar, Union, Literal

# Constant to avoid magic strings
PARTITION_SPEC_TYPE_KEY = "_dlt_partition_spec_type"

# Generic type for partition specs
T = TypeVar("T", bound="SerializablePartitionSpec")


@dataclass(frozen=True)
class SerializablePartitionSpec(ABC):
    """Base class for partition specs that automatically handles serialization/deserialization."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert partition spec to dictionary with type information."""
        data = asdict(self)
        data[PARTITION_SPEC_TYPE_KEY] = self.__class__.__name__
        return data

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Reconstruct partition spec from dictionary."""
        # Remove the type marker
        spec_data = {k: v for k, v in data.items() if k != PARTITION_SPEC_TYPE_KEY}
        return cls(**spec_data)


@dataclass(frozen=True)
class BigQueryRangeBucketPartition(SerializablePartitionSpec):
    column_name: str
    start: int
    end: int
    interval: int = 1

    def __post_init__(self) -> None:
        if self.interval <= 0:
            raise ValueError("interval must be a positive integer")
        if self.start >= self.end:
            raise ValueError("start must be less than end")


@dataclass(frozen=True)
class BigQueryDateTruncPartition(SerializablePartitionSpec):
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["MONTH", "YEAR"]

        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryIngestionTimePartition(SerializablePartitionSpec):
    column_name: str


@dataclass(frozen=True)
class BigQueryDateColumnPartition(SerializablePartitionSpec):
    column_name: str


@dataclass(frozen=True)
class BigQueryTimestampOrDateTimePartition(SerializablePartitionSpec):
    column_name: str


@dataclass(frozen=True)
class BigQueryDatetimeTruncPartition(SerializablePartitionSpec):
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["DAY", "HOUR", "MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryTimestampTruncPartition(SerializablePartitionSpec):
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["DAY", "HOUR", "MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryTimestampTruncIngestionPartition(SerializablePartitionSpec):
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["DAY", "HOUR", "MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


BigQueryPartitionSpec = Union[
    BigQueryRangeBucketPartition,
    BigQueryDateTruncPartition,
    BigQueryIngestionTimePartition,
    BigQueryDateColumnPartition,
    BigQueryTimestampOrDateTimePartition,
    BigQueryDatetimeTruncPartition,
    BigQueryTimestampTruncPartition,
    BigQueryTimestampTruncIngestionPartition,
]
