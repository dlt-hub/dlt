import pytest

from typing import Any

import dlt

from dlt.common.destination.dataset import SupportsReadableDataset
from tests.pipeline.utils import load_table_counts

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
