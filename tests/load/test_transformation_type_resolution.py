import pytest

from typing import Any

import dlt

from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.transformations.exceptions import TransformationTypeMismatch


def test_infer_transformation_type() -> None:
    @dlt.transformation()
    def transform(dataset: SupportsReadableDataset[Any]) -> Any:
        yield {"a": 1}

    # generator functions are set to python
    assert transform.transformation_type == "python"

    # sql transformations are set to sql
    @dlt.transformation(transformation_type="sql")
    def transform_sql(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["example_table"].limit(5)

    assert transform_sql.transformation_type == "sql"


def test_set_transformation_type() -> None:
    @dlt.transformation(transformation_type="sql")
    def transform_sql(dataset: SupportsReadableDataset[Any]) -> Any:
        return "some query"

    assert transform_sql.transformation_type == "sql"

    # we can also set it to python but not yield
    @dlt.transformation(transformation_type="python")
    def transform_python(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["example_table"].limit(5)

    # NOTE: test this properly here, we need a real dataset and need
    # to execute the query
    assert transform_python.transformation_type == "python"

    # setting sql for a yielding transformation will fail
    with pytest.raises(TransformationTypeMismatch):

        @dlt.transformation(transformation_type="sql")
        def transform_sql_yielding(dataset: SupportsReadableDataset[Any]) -> Any:
            yield {"a": 1}
