import pytest
import dlt
import os
import shutil
from dlt.common.schema import Schema
from dlt.dataset.dataset import Dataset

import time

def test_refresh_returns_new_instance_with_updated_schema() -> None:
    """refresh() returns a new Dataset that picks up destination changes without mutating the original."""
    pipeline_name = "test_refresh_new_instance"
    dataset_name = "test_data_refresh_new"

    pipelines_dir = os.path.abspath(f"temp_pipelines_refresh_{int(time.time())}")

    try:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir,
        )
        pipeline.run([{"id": 1, "name": "Alice"}], table_name="users")

        dataset = pipeline.dataset()
        assert "users" in dataset.tables

        # Second pipeline run adds a new table
        pipeline_b = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name=dataset_name,
            pipelines_dir=pipelines_dir,
        )
        time.sleep(2)  # allow Windows to release file handles from first run
        pipeline_b.run([{"id": 1, "amount": 99}], table_name="orders")

        # Original dataset is stale – orders not visible yet
        assert "orders" not in dataset.tables

        # refresh() returns a *new* Dataset instance
        refreshed = dataset.refresh()
        assert refreshed is not dataset
        assert isinstance(refreshed, Dataset)

        # New instance sees the updated schema
        assert "orders" in refreshed.tables
        assert "users" in refreshed.tables

        # Original instance is still stale (it was NOT mutated)
        assert "orders" not in dataset.tables

    finally:
        time.sleep(1)
        if os.path.exists(pipelines_dir):
            shutil.rmtree(pipelines_dir, ignore_errors=True)


def test_refresh_raises_when_schema_instance_provided() -> None:
    """refresh() raises TypeError if the Dataset was created with a Schema instance."""
    schema = Schema("my_schema")

    dataset = Dataset(
        destination="duckdb",
        dataset_name="dummy_ds",
        schema=schema,
    )

    with pytest.raises(TypeError, match="refresh\\(\\) is not supported"):
        dataset.refresh()


if __name__ == "__main__":
    test_refresh_returns_new_instance_with_updated_schema()
    test_refresh_raises_when_schema_instance_provided()
