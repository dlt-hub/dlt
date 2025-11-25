import pathlib
from typing import Any, Dict, List, Sequence, Tuple

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

from tests.utils import preserve_environ, patch_home_dir, autouse_test_storage, TEST_STORAGE_ROOT
from tests.pipeline.utils import assert_load_info, assert_records_as_set, assert_table_counts


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

    with dataset.write_pipeline() as write_pipeline:
        write_dataset = write_pipeline.dataset()

        assert isinstance(write_pipeline, dlt.Pipeline)
        assert write_pipeline.pipeline_name == expected_pipeline_name
        assert write_pipeline.dataset_name == dataset_name
        assert write_pipeline.destination == destination

    assert is_same_physical_destination(dataset, write_dataset)
    assert dataset.schema == write_dataset.schema


def test_dataset_write():
    dlt.pipeline()
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


def test_dataset_write_to_existing_table():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    pipeline = dlt.pipeline(destination=destination, dataset_name=dataset_name)

    # create existing table
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="numbers")
    assert_table_counts(pipeline, {"numbers": 2})

    # execute
    load_info = dataset.write([{"id": 2, "value": 3}], table_name="numbers")

    # verify data got written
    assert_load_info(load_info, expected_load_packages=1)
    assert_table_counts(pipeline, {"numbers": 3})

    # verify both pipeline.dataset() and dataset() see the same data
    pipeline_dataset = pipeline.dataset()
    expected_rows = data + [{"id": 2, "value": 3}]
    assert dataset.row_counts().fetchall() == [("numbers", 3)]
    dataset_numbers = dataset.table("numbers").select("id", "value")
    assert dataset_numbers.fetchall() == [tuple(i.values()) for i in expected_rows]

    # compare with pipeline
    # assert pipeline_dataset.row_counts().fetchall() == [("numbers", 3)]
    # # get data from both datasets as dicts and compare to expected rows
    # pipeline_numbers = pipeline_dataset.table("numbers").select("id", "value")
    # assert pipeline_numbers.fetchall() == dataset_numbers.fetchall()


def test_dataset_writes_new_table_to_existing_schema():
    # setup
    destination = dlt.destinations.duckdb(duckdb.connect())
    # createa standalone Dataset
    dataset_name = "foo"
    dataset = dlt.dataset(destination, dataset_name)

    # create existing table in the destination
    pipeline = dlt.pipeline(destination=destination, dataset_name=dataset_name)
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="numbers")
    assert_table_counts(pipeline, {"numbers": 2})

    # execute
    load_info = dataset.write(
        [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}], table_name="letters"
    )

    assert_load_info(load_info, expected_load_packages=1)

    # new table should show up in the Dataset schema
    assert "numbers" in dataset.schema.data_table_names()
    assert "letters" in dataset.schema.data_table_names()
    new_table_in_dataset_schema = dataset.schema.get_table("letters")
    assert "id" in new_table_in_dataset_schema["columns"]
    assert "value" in new_table_in_dataset_schema["columns"]

    # data is queryable from the Dataset
    assert dataset.table("letters").select("id", "value").fetchall() == [
        tuple(i.values()) for i in [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    ]

    # unexplored territory: how to refresh the schema/relation in the pipeline?
    # pipeline.sync_destination(destination=destination, dataset_name=dataset_name)
    # assert "letters" in pipeline.dataset().schema.data_table_names()

    # using sql client on pipeline dataset will work
    # pipeline_dataset = pipeline.dataset()
    # assert pipeline_dataset.query("SELECT id, value FROM letters").fetchall() == [
    # tuple(i.values()) for i in [{"id": 1, "value": 'a'}, {"id": 2, "value": 'b'}]
    # ]

    # # verify both pipeline.dataset() and dataset() see the same data
    # pipeline_dataset = pipeline.dataset()

    # assert pipeline_dataset.row_counts().fetchall() == [("numbers", 2), ("letters", 2)]
    # assert dataset.row_counts().fetchall() == [("numbers", 2), ("letters", 2)]

    # todo after what action, if any
    # # should the schema in the pipeline be the same as in the standalone Dataset?
    # pipeline.sync_schema()
    # assert pipeline_dataset.schema == dataset.schema


def test_dataset_write_updates_schema():
    # lets modify an existing schema by changing the type of column value from int to double
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset_name = "foo"
    pipeline = dlt.pipeline(destination=destination, dataset_name=dataset_name)
    data = [{"id": 0, "wrongly_typed_column": 1}, {"id": 1, "wrongly_typed_column": 2}]
    pipeline.run(data, table_name="bar")
    assert_table_counts(pipeline, {"bar": 2})

    # get standalone Dataset
    dataset = dlt.dataset(destination, dataset_name)
    assert (
        dataset.schema.get_table("bar")["columns"]["wrongly_typed_column"]["data_type"] == "bigint"
    )

    new_data = [{"id": 2, "wrongly_typed_column": 1.0}, {"id": 3, "wrongly_typed_column": 2.0}]
    dataset.write(new_data, table_name="bar", write_disposition="append")
    assert "wrongly_typed_column__v_double" in dataset.schema.tables["bar"]["columns"]


def test_data_write_temporary_dir():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    table_name = "bar"
    items = [{"id": 0, "value": "bingo"}, {"id": 1, "value": "bongo"}]
    storage_dir = pathlib.Path(TEST_STORAGE_ROOT)

    dataset.write(items, table_name=table_name)

    # check that pipeline used a temp directory
    # the patched test `pipelines_dir` should be empty
    assert [str(p) for p in storage_dir.iterdir()] == ["_storage/.dlt"]


def test_internal_pipeline_can_write_to_scratchpad_dataset():
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    dataset = dlt.dataset(destination, dataset_name)
    items = [{"id": 0, "value": "something"}]

    # write to the main dataset
    dataset.write(items, table_name="bar")

    # write to a scratchpad dataset
    # todo: maybe this could be exposed to dataset.write(data, dataset_name="scratchpad_1")
    with dataset.write_pipeline() as write_pipeline:
        new_items = [{"id": 1, "value": "something else"}]
        load_info = write_pipeline.run(new_items, table_name="bar", dataset_name="scratchpad_1")
        assert_load_info(load_info, expected_load_packages=1)

    # check that main dataset is still the same
    dataset.row_counts().fetchall() == [("bar", 1)]
    assert dataset.table("bar").select("id", "value").fetchall() == [
        (0, "something"),
    ]

    # but new data exists in the scratchpad dataset
    scratchpad_dataset = dlt.dataset(destination, "scratchpad_1")
    scratchpad_dataset.table("bar").select("id", "value").fetchall() == [
        (1, "something else"),
    ]


# Helpers
def _rows_to_dicts(rows: List[Tuple[Any, ...]], columns: Sequence[str]) -> List[Dict[str, Any]]:
    """Convert SQL result tuples into dictionaries keyed by the supplied column names."""
    return [dict(zip(columns, row)) for row in rows]
