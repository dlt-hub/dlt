import duckdb
import pytest

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.dataset.dataset import (
    _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE,
    is_same_physical_destination,
    _get_internal_pipeline,
)
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from tests.utils import preserve_environ, patch_home_dir, autouse_test_storage


def test_get_internal_pipeline():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    expected_pipeline_name = _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE.format(
        dataset_name=dataset_name
    )

    internal_pipeline = _get_internal_pipeline(dataset_name=dataset_name, destination=destination)
    internal_dataset = internal_pipeline.dataset()

    assert isinstance(internal_pipeline, dlt.Pipeline)
    assert internal_pipeline.pipeline_name == expected_pipeline_name
    assert internal_pipeline.dataset_name == dataset_name
    assert internal_pipeline.destination == destination
    assert is_same_physical_destination(dataset, internal_dataset)
    assert dataset.schema == internal_dataset.schema


def test_dataset_get_write_pipeline():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    expected_pipeline_name = _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE.format(
        dataset_name=dataset_name
    )

    write_pipeline = dataset.get_write_pipeline()
    write_dataset = write_pipeline.dataset()

    assert isinstance(write_pipeline, dlt.Pipeline)
    assert write_pipeline.pipeline_name == expected_pipeline_name
    assert write_pipeline.dataset_name == dataset_name
    assert write_pipeline.destination == destination
    assert is_same_physical_destination(dataset, write_dataset)
    assert dataset.schema == write_dataset.schema


def test_dataset_write():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    table_name = "bar"
    items = [{"id": 0, "value": "bingo"}, {"id": 1, "value": "bongo"}]

    # TODO this is currently odd because the tables exists on the `Schema`
    # used by the `Dataset` but the tables don't exist on the destination yet
    assert dataset.tables == ["_dlt_version", "_dlt_loads"]
    with pytest.raises(DatabaseUndefinedRelation):
        dataset.table("_dlt_version").fetchall()
    with pytest.raises(DatabaseUndefinedRelation):
        dataset.table("_dlt_loads").fetchall()

    load_info = dataset.write(items, table_name=table_name)

    assert isinstance(load_info, LoadInfo)
    assert dataset.tables == ["bar", "_dlt_version", "_dlt_loads"]
    assert dataset.table("bar").select("id", "value").fetchall() == [
        tuple(i.values()) for i in items
    ]
