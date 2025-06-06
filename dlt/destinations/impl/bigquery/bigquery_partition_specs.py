from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class BigQueryRangeBucketPartition:
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
class BigQueryDateTruncPartition:
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryIngestionTimePartition:
    column_name: str


@dataclass(frozen=True)
class BigQueryDateColumnPartition:
    column_name: str


@dataclass(frozen=True)
class BigQueryTimestampOrDateTimePartition:
    column_name: str


@dataclass(frozen=True)
class BigQueryDatetimeTruncPartition:
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["DAY", "HOUR", "MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryTimestampTruncPartition:
    column_name: str
    granularity: str

    def __post_init__(self) -> None:
        valid_granularities = ["DAY", "HOUR", "MONTH", "YEAR"]
        if self.granularity not in valid_granularities:
            raise ValueError(
                f"granularity must be one of {valid_granularities}, got {self.granularity}"
            )


@dataclass(frozen=True)
class BigQueryTimestampTruncIngestionPartition:
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
