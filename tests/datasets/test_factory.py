from typing import Any

import pytest

import dlt
import dlt._dataset.factory as dataset_factory
from dlt.destinations.dataset import ReadableDBAPIDataset


@pytest.fixture
def pipeline_that_ran() -> dlt.Pipeline:
    @dlt.resource(standalone=True)
    def mock_resource():
        yield from [{"value": 0}, {"value": 1}]

    pipeline = dlt.pipeline(
        pipeline_name="dataset_factory_pipe",
        destination="duckdb",
        dataset_name="pytest_dataset_name",
    )
    pipeline.run(mock_resource())
    return pipeline


@pytest.fixture
def expected_pipeline_properties(pipeline_that_ran: dlt.Pipeline) -> dict[str, Any]:
    properties = {}

    for prop in dlt.Pipeline.STATE_PROPS:
        if not prop.startswith("_"):
            properties[prop] = getattr(pipeline_that_ran, prop)

    properties["destination_type"] = pipeline_that_ran._destination.destination_type
    properties["destination_name"] = pipeline_that_ran._destination.configured_name

    return properties


def test_access_dataset_via_factory(
    pipeline_that_ran: dlt.Pipeline,
    expected_pipeline_properties: dict[str, Any],
) -> None:
    dataset_name = expected_pipeline_properties["dataset_name"]
    dataset = dataset_factory.dataset(dataset_name=dataset_name)
    assert isinstance(dataset, ReadableDBAPIDataset)


def test_access_dataset_via_top_level(
    pipeline_that_ran: dlt.Pipeline,
    expected_pipeline_properties: dict[str, Any],
) -> None:
    dataset_name = expected_pipeline_properties["dataset_name"]
    dataset = dlt.dataset(dataset_name=dataset_name)
    assert isinstance(dataset, ReadableDBAPIDataset)
