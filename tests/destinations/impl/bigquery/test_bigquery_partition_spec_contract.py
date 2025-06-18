import pytest
from typing import Iterator, cast
from unittest.mock import Mock, patch
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient
from dlt.destinations.impl.bigquery.bigquery_partition_specs import BigQueryTimestampTruncPartition, PARTITION_SPEC_TYPE_KEY
from dlt.common.schema import Schema
from tests.load.utils import yield_client_with_storage


@pytest.fixture(scope="module")
def client() -> Iterator[BigQueryClient]:
    """Create BigQuery client with proper test credentials."""
    yield from cast(Iterator[BigQueryClient], yield_client_with_storage("bigquery"))


class TestBigQueryPartitionStrictHandling:
    """Test strict partition spec handling enforces correct types."""
    
    def test_reconstruct_partition_spec_with_invalid_type_raises_error(self, client: BigQueryClient):
        """Test that _reconstruct_partition_spec raises ValueError for non-dict input."""
        with pytest.raises(ValueError, match=r"_reconstruct_partition_spec expects a dict, got str.*"):
            client._reconstruct_partition_spec("invalid_string")
        
        with pytest.raises(ValueError, match=r"_reconstruct_partition_spec expects a dict, got int.*"):
            client._reconstruct_partition_spec(123)
        
        with pytest.raises(ValueError, match=r"_reconstruct_partition_spec expects a dict, got NoneType.*"):
            client._reconstruct_partition_spec(None)
    
    def test_reconstruct_partition_spec_with_missing_type_key_raises_error(self, client: BigQueryClient):
        """Test that _reconstruct_partition_spec raises ValueError if '_dlt_partition_spec_type' is missing."""
        partition_dict = {
            "column_name": "created_at",
            "granularity": "DAY"
        }
        with pytest.raises(ValueError, match=r"Missing '_dlt_partition_spec_type' in partition hint dict.*"):
            client._reconstruct_partition_spec(partition_dict)

    def test_reconstruct_partition_spec_with_valid_dict_succeeds(self, client: BigQueryClient):
        """Test that _reconstruct_partition_spec works with valid dict input.
        Note: Full serialization/deserialization round-trip is tested in test_bigquery_partitionspecs.py
        """
        partition_dict = {
            PARTITION_SPEC_TYPE_KEY: "BigQueryTimestampTruncPartition",
            "column_name": "created_at",
            "granularity": "DAY"
        }
        
        result = client._reconstruct_partition_spec(partition_dict)
        assert isinstance(result, BigQueryTimestampTruncPartition)
        assert result.column_name == "created_at"
        assert result.granularity == "DAY"
    
    def test_bigquery_partition_clause_with_invalid_type_raises_error(self, client: BigQueryClient):
        """Test that _bigquery_partition_clause raises ValueError for non-spec input."""
        with pytest.raises(ValueError, match=r"_bigquery_partition_clause only accepts BigQueryPartitionSpec instances, got str.*"):
            client._bigquery_partition_clause("invalid_string")
        
        with pytest.raises(ValueError, match=r"_bigquery_partition_clause only accepts BigQueryPartitionSpec instances, got dict.*"):
            client._bigquery_partition_clause({"column_name": "test"})
        
        with pytest.raises(ValueError, match=r"_bigquery_partition_clause only accepts BigQueryPartitionSpec instances, got NoneType.*"):
            client._bigquery_partition_clause(None)
    
    def test_bigquery_partition_clause_with_valid_spec_succeeds(self, client: BigQueryClient):
        """Test that _bigquery_partition_clause works with valid BigQueryTimestampTruncPartition."""
        spec = BigQueryTimestampTruncPartition(
            column_name="created_at",
            granularity="DAY"
        )
        
        result = client._bigquery_partition_clause(spec)
        assert "PARTITION BY" in result
        assert "created_at" in result
        assert "DAY" in result
    
    def test_strict_partition_spec_processing_end_to_end(self, client: BigQueryClient):
        """Test complete flow from dict to partition clause through strict processing."""
        # Valid dict input -> should work
        partition_dict = {
            PARTITION_SPEC_TYPE_KEY: "BigQueryTimestampTruncPartition",
            "column_name": "event_time", 
            "granularity": "HOUR"
        }
        
        # Should reconstruct successfully
        spec = client._reconstruct_partition_spec(partition_dict)
        assert isinstance(spec, BigQueryTimestampTruncPartition)
        
        # Should generate clause successfully
        clause = client._bigquery_partition_clause(spec)
        assert "PARTITION BY" in clause
        assert "event_time" in clause
        assert "HOUR" in clause
