import pytest

from typing import Any

import dlt

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.load.utils import DestinationTestConfiguration
from tests.load.transformations.utils import (
    transformation_configs,
    setup_transformation_pipelines,
    load_fruit_dataset,
    row_counts,
    get_job_types,
)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_simple_query_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines andpopulate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    load_fruit_dataset(fruit_p)

    @dlt.transformation()
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    # transform into transformed dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    # all transformations are sql, except for filesystem destination
    assert get_job_types(dest_p) == {
        "copied_purchases": (
            {"model": 1} if destination_config.destination_type != "filesystem" else {"arrow": 1}
        )
    }
