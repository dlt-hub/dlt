import pytest

from typing import Any

import dlt

from dlt.transformations.reference import TLineageMode, TTransformationType
from dlt.extract.exceptions import ResourceExtractionError

from dlt.common.destination.dataset import SupportsReadableDataset
from tests.load.transformations.utils import (
    row_counts,
    transformation_configs,
    EXPECTED_FRUIT_ROW_COUNTS,
    setup_transformation_pipelines,
    load_fruit_dataset,
)
from tests.load.utils import DestinationTestConfiguration


# NOTE: move to duckdb only transformation tests
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("transformation_type", ["sql", "python"])
def test_simple_lineage(
    destination_config: DestinationTestConfiguration,
    transformation_type: TTransformationType,
) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, transformation_type)
    load_fruit_dataset(fruit_p)

    @dlt.transformation(write_disposition="append", transformation_type=transformation_type)
    def enriched_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        purchases = dataset["purchases"]
        customers = dataset["customers"]
        return purchases.join(customers, purchases.customer_id == customers.id)

    dest_p.run(enriched_purchases(fruit_p.dataset()))

    # check the rowcounts in the dest
    assert row_counts(dest_p.dataset(), tables=["enriched_purchases"]) == {
        "enriched_purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"]
    }

    # check that ppi column hint was preserved for name col
    assert dest_p.dataset().schema.tables["enriched_purchases"]["columns"]["name"]["x-pii"] is True  # type: ignore
    assert (
        dest_p.dataset().schema.tables["enriched_purchases"]["columns"]["id"].get("x-pii", False)
    ) is False


# NOTE: move to duckdb only transformation tests
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("lineage_mode", ["strict", "best_effort", "disabled"])
@pytest.mark.parametrize("add_unknown_column", [True, False])
def test_lineage_modes(
    destination_config: DestinationTestConfiguration,
    lineage_mode: TLineageMode,
    add_unknown_column: bool,
) -> None:
    if lineage_mode == "strict" and add_unknown_column:
        # TODO: lineage is better now, so I need a better way to figure out an unknown column type for this test
        pytest.skip("Skipping test for strict lineage mode with unknown column")

    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "python")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(
        write_disposition="append", transformation_type="python", lineage_mode=lineage_mode
    )
    def enriched_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        purchases = dataset["purchases"]
        customers = dataset["customers"]
        joined_table = purchases.join(customers, purchases.customer_id == customers.id)
        if add_unknown_column:
            joined_table = joined_table.mutate(new_column=5)
        return joined_table

    # if we add an unknown column in strict mode, we should raise (not tested at the moment...)
    if lineage_mode == "strict" and add_unknown_column:
        with pytest.raises(ResourceExtractionError):
            list(enriched_purchases(fruit_p.dataset()))
        return

    dest_p.run(enriched_purchases(fruit_p.dataset()))

    # for all modes except disabled, the name column should have the ppi hint
    assert dest_p.dataset().schema.tables["enriched_purchases"]["columns"]["name"].get(
        "x-pii", False
    ) == (lineage_mode != "disabled")
