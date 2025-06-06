from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class BigQueryRangeBucketPartition:
    column_name: str
    start: int
    end: int
    interval: int = 1


@dataclass(frozen=True)
class BigQueryDateTruncPartition:
    column_name: str
    granularity: str


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


@dataclass(frozen=True)
class BigQueryTimestampTruncPartition:
    column_name: str
    granularity: str


@dataclass(frozen=True)
class BigQueryTimestampTruncIngestionPartition:
    column_name: str
    granularity: str


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
