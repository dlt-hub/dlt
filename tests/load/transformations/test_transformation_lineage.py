import pytest

from typing import Any

import dlt

from tests.pipeline.utils import load_table_counts

from tests.load.transformations.utils import (
    transformation_configs,
    setup_transformation_pipelines,
)
from tests.load.utils import DestinationTestConfiguration
from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_simple_lineage(
    destination_config: DestinationTestConfiguration,
) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    s = fruitshop_source()
    s.customers.apply_hints(columns={"name": {"x-annotation-pii": True}})  # type: ignore
    fruit_p.run(s)

    @dlt.transformation(write_disposition="append")
    def enriched_purchases(dataset: dlt.Dataset) -> Any:
        purchases = dataset.table("purchases", table_type="ibis")
        customers = dataset.table("customers", table_type="ibis")
        yield purchases.join(customers, purchases.customer_id == customers.id)

    dest_p.run(enriched_purchases(fruit_p.dataset()))

    # check the rowcounts in the dest
    assert load_table_counts(dest_p, "enriched_purchases") == {"enriched_purchases": 100}

    # check that ppi column hint was preserved for name col
    assert dest_p.dataset().schema.tables["enriched_purchases"]["columns"]["name"]["x-annotation-pii"] is True  # type: ignore
    assert (
        dest_p.dataset()
        .schema.tables["enriched_purchases"]["columns"]["id"]
        .get("x-annotation-pii", False)
    ) is False
