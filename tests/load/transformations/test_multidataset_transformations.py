from typing import Any

import dlt
import pytest

from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.transformations.reference import TTransformationType


@pytest.mark.skip(reason="TODO: needs support from lineage to work")
@pytest.mark.parametrize("transformation_type", ["sql", "python"])
def test_combine_two_datasets(
    fruit_p: dlt.Pipeline, private_fruit_p: dlt.Pipeline, transformation_type: TTransformationType
) -> None:
    @dlt.transformation(transformation_type=transformation_type)
    def customers_with_ages(
        dataset: SupportsReadableDataset[Any], dataset2: SupportsReadableDataset[Any]
    ) -> Any:
        return dataset["customers"].join(
            dataset2["customers_ages"], dataset["customers"].id == dataset2["customers_ages"].id
        )

    fruit_p.run(customers_with_ages(fruit_p.dataset(), private_fruit_p.dataset()))

    assert fruit_p.dataset().customers_with_ages.select("age", "id", "name").df().to_dict(
        orient="records"
    ) == [
        {"age": 25, "id": 1, "name": "andrea"},
        {"age": 30, "id": 2, "name": "violetta"},
        {"age": 35, "id": 3, "name": "marcin"},
        {"age": 40, "id": 4, "name": "dave"},
    ]
