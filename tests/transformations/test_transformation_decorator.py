import os
import pytest
from pytest import LogCaptureFixture
from typing import Any, Optional

import dlt
from unittest.mock import patch
from dlt.common import logger

from dlt.common.configuration.inject import get_fun_last_config, get_fun_spec
from dlt.common.configuration.specs.base_configuration import configspec
from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.extract.hints import SqlModel
from dlt.transformations.configuration import TransformationConfiguration
from dlt.transformations.exceptions import (
    IncompatibleDatasetsException,
)
from dlt.extract.exceptions import ResourceExtractionError
from dlt.pipeline.exceptions import PipelineStepFailed


def test_no_datasets_used() -> None:
    # valid sql string with out dataset will raise
    with pytest.raises(IncompatibleDatasetsException) as excinfo:

        @dlt.transformation()
        def transform() -> Any:
            yield "SELECT * FROM table1"

        list(transform())

    assert "No datasets found in transformation function arguments" in str(excinfo.value)

    # invalid sql string without dataset will be interpreted as string item
    @dlt.transformation()
    def other_transform() -> Any:
        yield "Hello I am a string"

    assert list(other_transform()) == ["Hello I am a string"]


def test_iterator_function_as_transform_function() -> None:
    # test that a generator function is used as a regular resource
    @dlt.transformation()
    def transform(dataset: SupportsReadableDataset[Any]) -> Any:
        yield [{"some": "data"}]

    assert list(transform(dlt.dataset("duckdb", "dataset_name"))) == [{"some": "data"}]


def test_incremental_argument_is_not_supported(caplog: LogCaptureFixture) -> None:
    # test incremental default arg
    with patch.object(logger, "warning") as mock_warning:
        # NOTE: this error is not raised by the incremental, but because there is nothing to transform here
        with pytest.raises(ResourceExtractionError):

            @dlt.transformation()
            def transform_1(
                dataset: SupportsReadableDataset[Any],
                incremental_arg=dlt.sources.incremental("col1"),
            ) -> Any:
                yield "SELECT col1 FROM table1"

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
                # TODO: this may be edge case when we have native value but nevertheless dlt complains that there's no default
                incremental_arg: Optional[dlt.sources.incremental] = "some_value",  # type: ignore
            ) -> Any:
                return "SELECT col1 FROM table1"

            list(transform_2(dlt.dataset("duckdb", "dataset_name")))

    # TODO: verify why skipped
    # assert mock_warning.call_count == 1
    # assert (
    #     "Incremental arguments are not supported in transformation functions"
    #     in mock_warning.call_args[0][0]
    # )

    # no incremental, no warning
    with patch.object(logger, "warning") as mock_warning:
        with pytest.raises(ResourceExtractionError):

            @dlt.transformation()
            def transform_3(dataset: SupportsReadableDataset[Any]) -> Any:
                return "SELECT col1 FROM table1"

            list(transform_3(dlt.dataset("duckdb", "dataset_name")))

    assert mock_warning.call_count == 0

    # TODO: test if incremental works when we yield


def test_base_transformation_spec() -> None:
    os.environ["BUFFER_MAX_ITEMS"] = "100"

    @dlt.transformation
    def default_spec(dataset: dlt.Dataset):
        config = get_fun_last_config(default_spec)
        # config must be TransformConfiguration because it is used as derivation base
        assert isinstance(config, TransformationConfiguration)
        # but type is derived
        assert type(config) is not TransformationConfiguration
        # config got passed
        assert config.buffer_max_items == 100
        yield "SELECT col1 FROM table1"

    schema = Schema("_data")
    schema.update_table(new_table("table1", columns=[{"name": "col1", "data_type": "text"}]))
    # provide dataset for lineage
    ds_ = dlt.dataset(dlt.destinations.filesystem("_data"), "dataset_name", schema=schema)

    # we return SQL so we expect a model to be created
    model = list(default_spec(ds_))[0]
    assert isinstance(model, SqlModel)
    # TODO: why dialect is not set??
    # assert model.dialect is not None

    # test parametrized transformation
    @dlt.transformation
    def default_transformation_with_args(
        dataset: dlt.Dataset, last_id: str = dlt.config.value, limit: int = 5
    ):
        assert last_id == "test_last_id"
        yield dataset.table1[["col1"]]

    spec = get_fun_spec(default_transformation_with_args)
    assert "last_id" in spec().get_resolvable_fields()
    assert issubclass(spec, TransformationConfiguration) is True

    # set required config
    os.environ["LAST_ID"] = "test_last_id"

    model = list(default_transformation_with_args(ds_))[0]
    assert isinstance(model, SqlModel)
    assert get_fun_last_config(default_transformation_with_args)["last_id"] == "test_last_id"

    # test explicit spec
    @configspec
    class DefaultTrConfig(TransformationConfiguration):
        buffer_max_items: int = 10000  # change default buffer
        limit: Optional[int] = 5  # otherwise would not be injected
        last_idx: str = None

    @dlt.transformation(spec=DefaultTrConfig, name="default_name_ovr", section="default_name_ovr")
    def default_transformation_spec(
        dataset: dlt.Dataset, last_idx: str = dlt.config.value, limit: int = 5
    ):
        assert last_idx is not None
        print("LAST_IDX", last_idx)
        config: DefaultTrConfig = get_fun_last_config(default_transformation_spec)  # type: ignore[assignment]
        assert isinstance(config, DefaultTrConfig)
        assert config.buffer_max_items == 100000
        assert limit == 100

        table1_ = dataset(f"SELECT * FROM table1 WHERE col1 = '{last_idx}' LIMIT {limit}")
        yield table1_

    assert default_transformation_spec.name == "default_name_ovr"
    assert default_transformation_spec.section == "default_name_ovr"

    os.environ["SOURCES__DEFAULT_NAME_OVR__BUFFER_MAX_ITEMS"] = "100000"
    os.environ["SOURCES__DEFAULT_NAME_OVR__LAST_IDX"] = "uniq_last_id"
    os.environ["SOURCES__DEFAULT_NAME_OVR__LIMIT"] = "100"

    model = list(default_transformation_spec(ds_))[0]
    assert isinstance(model, SqlModel)
    query = model.query
    # make sure we have our args in query
    assert "uniq_last_id" in query
    assert "100" in query


def test_yield_tables_fallback() -> None:
    """Tests if fallback to dataframes happens when physical destinations are different"""
