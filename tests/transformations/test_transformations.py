import pytest

from typing import Any

import dlt
import os

from dlt.common.destination.dataset import SupportsReadableDataset
from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import SqlModel

from tests.load.utils import DestinationTestConfiguration
from tests.load.transformations.utils import (
    transformation_configs,
    setup_transformation_pipelines,
    get_job_types,
)

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_simple_query_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines andpopulate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(3)

    # transform into transformed dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_purchases") == {
        "copied_purchases": 3,
    }

    # all transformations are sql, except for filesystem destination
    assert get_job_types(dest_p) == {
        "copied_purchases": (
            {"model": 1} if destination_config.destination_type != "filesystem" else {"arrow": 1}
        )
    }


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_extract_without_source_name_or_pipeline(
    destination_config: DestinationTestConfiguration,
) -> None:
    # get pipelines andpopulate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def buffer_size_test(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["customers"]

    # transformations switch to model extraction
    fruit_p.deactivate()
    model_rows = list(buffer_size_test(fruit_p.dataset()))
    assert len(model_rows) == 1
    assert isinstance(model_rows[0], SqlModel)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_extract_without_destination(destination_config: DestinationTestConfiguration) -> None:
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def extract_test(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["customers"]

    pipeline_no_destination = dlt.pipeline(pipeline_name="no_destination")
    pipeline_no_destination._destination = None
    extract_info = pipeline_no_destination.extract(extract_test(fruit_p.dataset()))

    # there is no destination, so we should have arrow extraction
    found_job = False
    for job in extract_info.load_packages[0].jobs["new_jobs"]:
        if job.job_file_info.table_name == "extract_test":
            assert job.job_file_info.file_format == "model"
            found_job = True
    assert found_job
