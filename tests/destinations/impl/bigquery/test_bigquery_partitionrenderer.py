import pytest
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    BigQueryRangeBucketPartition,
    BigQueryDateTruncPartition,
    BigQueryPartitionRenderer,
)

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
