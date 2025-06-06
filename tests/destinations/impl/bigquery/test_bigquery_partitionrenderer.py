import pytest
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    BigQueryPartitionRenderer,
    BigQueryIngestionTimePartition,
    BigQueryDateColumnPartition,
    BigQueryTimestampOrDateTimePartition,
    BigQueryDatetimeTruncPartition,
    BigQueryTimestampTruncPartition,
    BigQueryTimestampTruncIngestionPartition,
    BigQueryRangeBucketPartition,
    BigQueryDateTruncPartition,
)

def test_render_ingestion_time_partition():
    part = BigQueryIngestionTimePartition()
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == "PARTITION BY _PARTITIONDATE"

def test_render_date_column_partition():
    part = BigQueryDateColumnPartition("created_at")
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == "PARTITION BY created_at"

def test_render_timestamp_or_datetime_partition():
    part = BigQueryTimestampOrDateTimePartition("event_time")
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == "PARTITION BY DATE(event_time)"

@pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
def test_render_datetime_trunc_partition(granularity):
    part = BigQueryDatetimeTruncPartition("dt_col", granularity)
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == f"PARTITION BY DATETIME_TRUNC(dt_col, '{granularity}')"

@pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
def test_render_timestamp_trunc_partition(granularity):
    part = BigQueryTimestampTruncPartition("ts_col", granularity)
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == f"PARTITION BY TIMESTAMP_TRUNC(ts_col, '{granularity}')"

@pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
def test_render_timestamp_trunc_ingestion_partition(granularity):
    part = BigQueryTimestampTruncIngestionPartition(granularity)
    sql = BigQueryPartitionRenderer.render_sql([part])
    assert sql == f"PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, '{granularity}')"

def test_render_range_bucket_partition():
    part = BigQueryRangeBucketPartition("user_id", 0, 100, 10)
    sql = BigQueryPartitionRenderer.render_sql([part])
    expected = "PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 100, 10))"
    assert sql == expected

def test_render_date_trunc_partition():
    part = BigQueryDateTruncPartition("created_at", "MONTH")
    sql = BigQueryPartitionRenderer.render_sql([part])
    expected = "PARTITION BY DATE_TRUNC(created_at, 'MONTH')"
    assert sql == expected

def test_render_multiple_partitions_raises():
    part1 = BigQueryRangeBucketPartition("user_id", 0, 100, 10)
    part2 = BigQueryDateTruncPartition("created_at", "YEAR")
    with pytest.raises(ValueError, match="only supports partitioning by a single column"):
        BigQueryPartitionRenderer.render_sql([part1, part2])
