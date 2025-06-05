import pytest
from dlt.destinations.impl.bigquery.bigquery_adapter import (
    BigQueryRangeBucketPartition,
    BigQueryDateTruncPartition,
)


class TestRangeBucketPartition:
    def test_valid(self):
        part = BigQueryRangeBucketPartition("user_id", 0, 100, 10)
        assert part.column_name == "user_id"
        assert part.start == 0
        assert part.end == 100
        assert part.interval == 10

    def test_invalid_interval(self):
        with pytest.raises(ValueError, match="interval must be a positive integer"):
            BigQueryRangeBucketPartition("user_id", 0, 100, 0)

    def test_invalid_range(self):
        with pytest.raises(ValueError, match="start must be less than end"):
            BigQueryRangeBucketPartition("user_id", 10, 5, 1)


class TestDateTruncPartition:
    def test_valid(self):
        part = BigQueryDateTruncPartition("created_at", "MONTH")
        assert part.column_name == "created_at"
        assert part.granularity == "MONTH"
        part2 = BigQueryDateTruncPartition("created_at", "YEAR")
        assert part2.granularity == "YEAR"

    def test_invalid_granularity(self):
        with pytest.raises(ValueError, match="granularity must be one of"):
            BigQueryDateTruncPartition("created_at", "DAY")


class TestDatetimeTruncPartition:
    @pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
    def test_valid(self, granularity):
        from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryDatetimeTruncPartition

        part = BigQueryDatetimeTruncPartition("dt_col", granularity)
        assert part.column_name == "dt_col"
        assert part.granularity == granularity

    def test_invalid_granularity(self):
        from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryDatetimeTruncPartition

        with pytest.raises(ValueError, match="granularity must be one of"):
            BigQueryDatetimeTruncPartition("dt_col", "MINUTE")


class TestTimestampTruncPartition:
    @pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
    def test_valid(self, granularity):
        from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryTimestampTruncPartition

        part = BigQueryTimestampTruncPartition("ts_col", granularity)
        assert part.column_name == "ts_col"
        assert part.granularity == granularity

    def test_invalid_granularity(self):
        from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryTimestampTruncPartition

        with pytest.raises(ValueError, match="granularity must be one of"):
            BigQueryTimestampTruncPartition("ts_col", "SECOND")


class TestTimestampTruncIngestionPartition:
    @pytest.mark.parametrize("granularity", ["DAY", "HOUR", "MONTH", "YEAR"])
    def test_valid(self, granularity):
        from dlt.destinations.impl.bigquery.bigquery_adapter import (
            BigQueryTimestampTruncIngestionPartition,
        )

        part = BigQueryTimestampTruncIngestionPartition(granularity)
        assert part.granularity == granularity

    def test_invalid_granularity(self):
        from dlt.destinations.impl.bigquery.bigquery_adapter import (
            BigQueryTimestampTruncIngestionPartition,
        )

        with pytest.raises(ValueError, match="granularity must be one of"):
            BigQueryTimestampTruncIngestionPartition("SECOND")
