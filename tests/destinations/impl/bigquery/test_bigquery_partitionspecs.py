import pytest
from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryRangeBucketPartition, BigQueryDateTruncPartition


def test_bigquery_range_bucket_partition_valid():
    part = BigQueryRangeBucketPartition("user_id", 0, 100, 10)
    assert part.column_name == "user_id"
    assert part.start == 0
    assert part.end == 100
    assert part.interval == 10


def test_bigquery_range_bucket_partition_invalid_interval():
    with pytest.raises(ValueError, match="interval must be a positive integer"):
        BigQueryRangeBucketPartition("user_id", 0, 100, 0)


def test_bigquery_range_bucket_partition_invalid_range():
    with pytest.raises(ValueError, match="start must be less than end"):
        BigQueryRangeBucketPartition("user_id", 10, 5, 1)


def test_bigquery_date_trunc_partition_valid():
    part = BigQueryDateTruncPartition("created_at", "MONTH")
    assert part.column_name == "created_at"
    assert part.granularity == "MONTH"
    part2 = BigQueryDateTruncPartition("created_at", "YEAR")
    assert part2.granularity == "YEAR"


def test_bigquery_date_trunc_partition_invalid_granularity():
    with pytest.raises(ValueError, match="granularity must be one of"):
        BigQueryDateTruncPartition("created_at", "DAY")
