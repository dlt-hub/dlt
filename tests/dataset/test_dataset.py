import pytest
import dlt
from dlt.common.schema import Schema
from dlt.dataset.dataset import Dataset

import time

def test_pipeline_dataset_refresh_raises_type_error(tmp_path) -> None:
    """refresh() raises TypeError when Dataset is created via pipeline.dataset() (which uses a Schema instance)."""
    pipeline_name = "test_refresh_type_error"
    dataset_name = "test_data_refresh_type_error"

    pipelines_dir = str(tmp_path / "pipelines_error")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name=dataset_name,
        pipelines_dir=pipelines_dir,
    )
    pipeline.run([{"id": 1, "name": "Alice"}], table_name="users")

    dataset = pipeline.dataset()
    assert "users" in dataset.tables

    # dataset from pipeline has a dlt.Schema instance, so refresh() is unsupported
    with pytest.raises(TypeError, match="refresh\\(\\) is not supported"):
        dataset.refresh()


def test_refresh_returns_new_instance_with_updated_schema(tmp_path) -> None:
    """refresh() returns a new Dataset that picks up destination changes when initialized with a schema string."""
    pipeline_name = "test_refresh_new_instance"
    dataset_name = "test_data_refresh_new"

    pipelines_dir = str(tmp_path / "pipelines_refresh")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name=dataset_name,
        pipelines_dir=pipelines_dir,
    )
    pipeline.run([{"id": 1, "name": "Alice"}], table_name="users")

    # Create Dataset manually providing the schema name as string
    dataset = Dataset(
        destination=pipeline.destination,
        dataset_name=dataset_name,
        schema=pipeline.default_schema_name,
    )
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
