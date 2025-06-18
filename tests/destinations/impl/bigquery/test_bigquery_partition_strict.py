"""Test edge cases and error conditions for our refactored partition logic."""

import pytest
from dlt.destinations.impl.bigquery.bigquery import BigQueryClient
from dlt.destinations.impl.bigquery.bigquery_adapter import BigQueryRangeBucketPartition
from dlt.destinations import bigquery
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.schema import Schema
from tests.utils import uniq_id


def create_test_client() -> BigQueryClient:
    """Create a minimal BigQuery client for testing."""
    schema = Schema("test_schema")
    creds = GcpServiceAccountCredentials()
    creds.project_id = "test_project_id"
    return bigquery().client(
        schema,
        BigQueryClientConfiguration(
            credentials=creds,
        )._bind_dataset_name(dataset_name=f"test_{uniq_id()}"),
    )


def test_reconstruct_partition_spec_strict_error_handling():
    """Test that _reconstruct_partition_spec now raises ValueError for invalid input."""
    client = create_test_client()
    
    # Test with invalid input - should now raise ValueError
    with pytest.raises(ValueError, match="_reconstruct_partition_spec expects a dict"):
        client._reconstruct_partition_spec("invalid_string")
    
    with pytest.raises(ValueError, match="_reconstruct_partition_spec expects a dict"):
        client._reconstruct_partition_spec(123)
    
    with pytest.raises(ValueError, match="_reconstruct_partition_spec expects a dict"):
        client._reconstruct_partition_spec(True)
    
    with pytest.raises(ValueError, match="_reconstruct_partition_spec expects a dict"):
        client._reconstruct_partition_spec(None)
    
    # Test with dict missing required type key - should raise ValueError
    with pytest.raises(ValueError, match="Missing '_dlt_partition_spec_type' in partition hint dict"):
        client._reconstruct_partition_spec({"column_name": "test"})
    
    # Test with unknown partition spec type
    with pytest.raises(ValueError, match="Unknown partition spec type 'UnknownType'"):
        client._reconstruct_partition_spec({
            "_dlt_partition_spec_type": "UnknownType",
            "column_name": "test"
        })
    
    # Test successful reconstruction with valid dict
    valid_spec_dict = {
        "_dlt_partition_spec_type": "BigQueryRangeBucketPartition",
        "column_name": "user_id",
        "start": 0,
        "end": 1000,
        "interval": 10
    }
    result = client._reconstruct_partition_spec(valid_spec_dict)
    assert isinstance(result, BigQueryRangeBucketPartition)
    assert result.column_name == "user_id"
    assert result.start == 0
    assert result.end == 1000
    assert result.interval == 10


def test_bigquery_partition_clause_strict_error_handling():
    """Test that _bigquery_partition_clause now only accepts BigQueryPartitionSpec objects."""
    client = create_test_client()
    
    # Test with non-BigQueryPartitionSpec input - should raise ValueError
    with pytest.raises(ValueError, match="_bigquery_partition_clause only accepts BigQueryPartitionSpec"):
        client._bigquery_partition_clause("invalid_string")
    
    with pytest.raises(ValueError, match="_bigquery_partition_clause only accepts BigQueryPartitionSpec"):
        client._bigquery_partition_clause({"type": "dict"})
    
    with pytest.raises(ValueError, match="_bigquery_partition_clause only accepts BigQueryPartitionSpec"):
        client._bigquery_partition_clause(None)
    
    # Test successful clause generation with valid spec
    valid_spec = BigQueryRangeBucketPartition("user_id", 0, 1000, 10)
    result = client._bigquery_partition_clause(valid_spec)
    assert "PARTITION BY RANGE_BUCKET" in result
    assert "user_id" in result
    assert "GENERATE_ARRAY(0, 1000, 10)" in result
