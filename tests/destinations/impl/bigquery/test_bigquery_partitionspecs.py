import pytest
import json

from dlt.destinations.impl.bigquery.bigquery_adapter import (
    BigQueryDateTruncPartition,
    BigQueryRangeBucketPartition,
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
        with pytest.raises(ValueError, match="granularity must be one of \\['MONTH', 'YEAR'\\], got DAY"):
            BigQueryDateTruncPartition("created_at", "DAY")
        
        # Test other invalid granularities
        with pytest.raises(ValueError, match="granularity must be one of \\['MONTH', 'YEAR'\\], got QUARTER"):
            BigQueryDateTruncPartition("created_at", "QUARTER")
        
        with pytest.raises(ValueError, match="granularity must be one of \\['MONTH', 'YEAR'\\], got WEEK"):
            BigQueryDateTruncPartition("created_at", "WEEK")


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

        part = BigQueryTimestampTruncIngestionPartition("_PARTITIONTIME", granularity)
        assert part.granularity == granularity

    def test_invalid_granularity(self):
        from dlt.destinations.impl.bigquery.bigquery_adapter import (
            BigQueryTimestampTruncIngestionPartition,
        )

        with pytest.raises(ValueError, match="granularity must be one of"):
            BigQueryTimestampTruncIngestionPartition("_PARTITIONTIME", "SECOND")


class TestSerialization:
    """Test serialization/deserialization of partition specs."""
    
    def test_json_serialization_round_trip(self):
        """Test that partition specs survive JSON serialization with explicit and default values."""
        # Test RangeBucketPartition with explicit values
        range_explicit = BigQueryRangeBucketPartition("user_id", 100, 2000, 50)
        range_dict = range_explicit.to_dict()
        
        # Verify dict structure contains type marker
        assert "_dlt_partition_spec_type" in range_dict
        assert range_dict["_dlt_partition_spec_type"] == "BigQueryRangeBucketPartition"
        
        # Serialize to JSON and back
        json_string = json.dumps(range_dict)
        json_dict = json.loads(json_string)
        range_reconstructed = BigQueryRangeBucketPartition.from_dict(json_dict)
        
        assert range_reconstructed == range_explicit
        
        # Test RangeBucketPartition with default interval value
        range_default = BigQueryRangeBucketPartition("score", 0, 100)  # Uses default interval=1
        range_default_dict = range_default.to_dict()
        assert range_default_dict["interval"] == 1  # Default value should be in dict
        
        json_string = json.dumps(range_default_dict)
        json_dict = json.loads(json_string)
        range_default_reconstructed = BigQueryRangeBucketPartition.from_dict(json_dict)
        assert range_default_reconstructed == range_default
        assert range_default_reconstructed.interval == 1
