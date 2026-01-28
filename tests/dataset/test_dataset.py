
import pytest
import dlt
import os
import shutil
from dlt.common.schema import Schema

import time

def test_sync_schema() -> None:
    pipeline_name = 'test_sync_schema_unit_test'
    dataset_name = 'test_data_sync_schema'
    
    # Use unique dir to avoid windows locking issues
    pipelines_dir = os.path.abspath(f"temp_pipelines_dir_{int(time.time())}")
    
    try:
        # Setup primary pipeline
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination='duckdb',
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir
        )

        # Run with first resource
        pipeline.run([{'id': 1, 'name': 'Sharma'}], table_name='users')

        # Create dataset
        dataset = pipeline.dataset()
        assert 'users' in dataset.tables

        # Use a SECOND pipeline instance to modify the destination of the first dataset
        pipeline_b = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination='duckdb',
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir
        )
        pipeline_b.run([{'id': 1, 'amount': 100}], table_name='orders')

        # Check if the FIRST dataset sees the change (Should be stale initially)
        tables_before = dataset.tables        

        # Sync schema
        dataset.sync_schema()
        tables_after = dataset.tables
        assert 'orders' in tables_after
        
        # Test Behavior: Local only
        dataset.sync_schema(local_only=True)
        # CURRENT BEHAVIOR: local_only=True creates a NEW dlt.Schema(name) which is empty (except dlt tables).
        # It does NOT load from file because Dataset doesn't know where the pipeline file is.
        # So 'orders' will vanish.
        assert 'orders' not in dataset.tables

        # Test Behavior: Explicit schema
        dummy_schema = Schema("dummy")
        dummy_schema.update_table({"name": "fake_table", "columns": {"col1": {"name": "col1", "data_type": "text"}}})
        
        dataset.sync_schema(schema=dummy_schema)
        assert dataset.schema.name == "dummy"
        assert "fake_table" in dataset.tables
        assert "users" not in dataset.tables

    finally:
        time.sleep(1) # else it might give error on windows ¯\_(ツ)_/¯
        if os.path.exists(pipelines_dir):
            shutil.rmtree(pipelines_dir, ignore_errors=True)


if __name__ == "__main__":
    test_sync_schema()
