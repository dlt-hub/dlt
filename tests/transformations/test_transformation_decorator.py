import pytest

from typing import Any

import dlt
import os

from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.transformations.exceptions import (
    TransformationTypeMismatch,
    TransformationInvalidReturnTypeException,
    LineageFailedException,
    IncompatibleDatasetsException,
)
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed


def test_no_datasets_used() -> None:
    with pytest.raises(ResourceExtractionError) as excinfo:

        @dlt.transformation()
        def transform() -> Any:
            return {"some": "data"}

        list(transform())

    assert "No datasets detected in transformation. Please supply all used datasets via" in str(
        excinfo.value
    )
    assert isinstance(excinfo.value.__context__, IncompatibleDatasetsException)


def test_dataset_not_on_same_destination() -> None:
    os.environ["BUCKET_URL"] = "./local"
    with pytest.raises(ResourceExtractionError) as excinfo:

        @dlt.transformation()
        def transform(
            dataset: SupportsReadableDataset[Any], dataset2: SupportsReadableDataset[Any]
        ) -> Any:
            return dataset["table_name"]

        list(
            transform(
                dlt.dataset("duckdb", "dataset_name"), dlt.dataset("filesystem", "dataset_name2")
            )
        )
    assert "All datasets used in transformation must be on the" in str(excinfo.value)
    assert isinstance(excinfo.value.__context__, IncompatibleDatasetsException)


def test_iterator_function_as_transform_function() -> None:
    with pytest.raises(TransformationInvalidReturnTypeException):

        @dlt.transformation()
        def transform(dataset: SupportsReadableDataset[Any]) -> Any:
            yield [{"some": "data"}]


def test_incorrect_transform_function_return_type() -> None:
    p = dlt.pipeline("test_pipeline", destination="duckdb")

    @dlt.transformation()
    def transform(dataset: SupportsReadableDataset[Any]) -> Any:
        return {"some": "data"}

    with pytest.raises(PipelineStepFailed) as excinfo:
        p.run(transform(dlt.dataset("duckdb", "dataset_name")))

    assert "Please either return a valid sql string or a SupportsReadableRelation instance" in str(
        excinfo.value
    )
    assert isinstance(
        excinfo.value.__context__.__context__, TransformationInvalidReturnTypeException
    )
