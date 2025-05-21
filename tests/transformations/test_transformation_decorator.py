import pytest

from typing import Any

import dlt
from unittest.mock import patch
from dlt.common import logger

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
    with pytest.raises(IncompatibleDatasetsException) as excinfo:

        @dlt.transformation()
        def transform() -> Any:
            return {"some": "data"}

        list(transform())

    assert "No datasets detected in transformation. Please supply all used datasets via" in str(
        excinfo.value
    )


def test_iterator_function_as_transform_function() -> None:
    # test that a generator function is used as a regular resource
    @dlt.transformation()
    def transform(dataset: SupportsReadableDataset[Any]) -> Any:
        yield [{"some": "data"}]

    assert list(transform(dlt.dataset("duckdb", "dataset_name"))) == [{"some": "data"}]


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


def test_incremental_argument_is_not_supported(caplog) -> None:
    # test incremental default arg
    with patch.object(logger, "warning") as mock_warning:
        # NOTE: this error is not raised by the incremental, but because there is nothing to transform here
        with pytest.raises(ResourceExtractionError):

            @dlt.transformation()
            def transform_1(
                dataset: SupportsReadableDataset[Any], incremental_arg=dlt.sources.incremental()
            ) -> Any:
                return "SELECT col1 FROM table1"

            list(transform_1(dlt.dataset("duckdb", "dataset_name")))

    assert mock_warning.call_count == 1
    assert (
        "Incremental arguments are not supported in transformation functions"
        in mock_warning.call_args[0][0]
    )

    # test incremental arg type
    with patch.object(logger, "warning") as mock_warning:
        # NOTE: this error is not raised by the incremental, but because there is nothing to transform here
        with pytest.raises(ResourceExtractionError):

            @dlt.transformation()
            def transform_2(
                dataset: SupportsReadableDataset[Any],
                incremental_arg: dlt.sources.incremental = "some_value",  # type: ignore
            ) -> Any:
                return "SELECT col1 FROM table1"

            list(transform_2(dlt.dataset("duckdb", "dataset_name")))

    assert mock_warning.call_count == 1
    assert (
        "Incremental arguments are not supported in transformation functions"
        in mock_warning.call_args[0][0]
    )

    # no incremental, no warning
    with patch.object(logger, "warning") as mock_warning:
        with pytest.raises(ResourceExtractionError):

            @dlt.transformation()
            def transform_3(dataset: SupportsReadableDataset[Any]) -> Any:
                return "SELECT col1 FROM table1"

            list(transform_3(dlt.dataset("duckdb", "dataset_name")))

    assert mock_warning.call_count == 0
