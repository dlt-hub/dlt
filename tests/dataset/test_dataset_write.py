import duckdb

import dlt
from dlt.dataset.dataset import _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE, is_same_physical_destination


def test_dataset_get_write_pipeline():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    expected_pipeline_name = _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE.format(dataset_name=dataset_name)

    write_pipeline = dataset.get_write_pipeline()
    write_dataset = write_pipeline.dataset()

    assert isinstance(write_pipeline, dlt.Pipeline)
    assert write_pipeline.pipeline_name == expected_pipeline_name
    assert write_pipeline.dataset_name == dataset_name
    assert write_pipeline.destination == destination
    assert is_same_physical_destination(dataset, write_dataset)
