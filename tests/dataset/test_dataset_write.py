import pathlib
from typing import Any, Dict, Generator, List, Sequence, Tuple

import duckdb
import pytest

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import DictStrAny
from dlt.dataset.dataset import (
    _INTERNAL_DATASET_PIPELINE_NAME_TEMPLATE,
    is_same_physical_destination,
    _get_internal_pipeline,
)
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from tests.utils import preserve_environ, patch_home_dir, autouse_test_storage, TEST_STORAGE_ROOT
from tests.pipeline.utils import assert_load_info, assert_records_as_set, assert_table_counts

from dlt.common.destination import Destination, TDestinationReferenceArg


@pytest.fixture()
def pipeline_and_foo_dataset() -> Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]:
    dataset_name = "foo"
    destination = dlt.destinations.duckdb(duckdb.connect())
    pipeline = dlt.pipeline(destination=destination, dataset_name=dataset_name)
    dataset = dlt.dataset(destination, dataset_name)
    return pipeline, dataset, dataset_name, destination


def test_get_internal_pipeline(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    _, dataset, dataset_name, destination = pipeline_and_foo_dataset

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


def test_dataset_get_write_pipeline(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    _, dataset, dataset_name, destination = pipeline_and_foo_dataset
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


def test_dataset_write(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    _, dataset, _, _ = pipeline_and_foo_dataset
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


def test_dataset_write_to_existing_table(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    pipeline, dataset, _, _ = pipeline_and_foo_dataset

    # create existing table
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="numbers")
    assert_table_counts(pipeline, {"numbers": 2})

    schema_before = dataset.schema.clone()

    # execute
    load_info = dataset.write([{"id": 2, "value": 3}], table_name="numbers")

    # verify data got written
    assert_load_info(load_info, expected_load_packages=1)
    assert_table_counts(pipeline, {"numbers": 3})

    # verify data is readable from the dataset
    expected_rows = data + [{"id": 2, "value": 3}]
    assert dataset.table("numbers").select("id", "value").fetchall() == [
        tuple(i.values()) for i in expected_rows
    ]

    # schema didn't change
    assert schema_before == dataset.schema


def test_dataset_write_respects_write_disposition_of_existing_tables(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    pipeline, _, dataset_name, destination = pipeline_and_foo_dataset

    # create existing table with merge write disposition
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="merge_table", write_disposition="merge", primary_key="id")
    pipeline.run(data, table_name="replace_table", write_disposition="replace")
    pipeline.run(data, table_name="append_table", write_disposition="append")
    assert_table_counts(pipeline, {"merge_table": 2, "replace_table": 2, "append_table": 2})

    dataset = dlt.dataset(destination, dataset_name)
    assert dataset.schema.get_table("merge_table")["write_disposition"] == "merge"
    assert dataset.schema.get_table("replace_table")["write_disposition"] == "replace"
    assert dataset.schema.get_table("append_table")["write_disposition"] == "append"

    schema_before = dataset.schema.clone()

    # execute
    new_data = [{"id": 0, "value": 3}]
    dataset.write(new_data, table_name="merge_table")
    dataset.write(new_data, table_name="replace_table")
    dataset.write(new_data, table_name="append_table")

    assert_table_counts(pipeline, {"merge_table": 2, "replace_table": 1, "append_table": 3})

    # verify data that is returned from the dataset for id 0
    assert dataset.table("merge_table").where("id = 0").select("value").fetchall() == [(3,)]
    assert dataset.table("replace_table").where("id = 0").select("value").fetchall() == [(3,)]
    assert dataset.table("append_table").where("id = 0").select("value").fetchall() == [(1,), (3,)]

    # schema didn't change
    assert schema_before == dataset.schema


def test_dataset_writes_new_table_to_existing_schema(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    pipeline, dataset, dataset_name, destination = pipeline_and_foo_dataset

    # create existing table in the destination
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="numbers")
    assert_table_counts(pipeline, {"numbers": 2})

    schema_before = dataset.schema.clone()

    # execute
    new_table_name = "letters"
    load_info = dataset.write(
        [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}], table_name=new_table_name
    )

    assert_load_info(load_info, expected_load_packages=1)

    # assert schema has changed
    assert schema_before != dataset.schema

    # new table should show up in the Dataset schema
    assert "letters" in dataset.schema.data_table_names()

    # data is queryable from the Dataset
    assert dataset.table("letters").select("id", "value").fetchall() == [
        tuple(i.values()) for i in [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    ]

    # expect write_disposition of new table to be "append"
    assert dataset.schema.get_table("letters")["write_disposition"] == "append"


# @pytest.mark.xfail(
#     reason=(
#         "schema syncing is using version hash and doesnt check if there is a newer schema in the"
#         " _dlt_versions table"
#     )
# )
def test_pipeline_sync_destination_fetches_new_schema(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    pipeline, dataset, dataset_name, destination = pipeline_and_foo_dataset
    data = [{"id": 0, "value": 1}, {"id": 1, "value": 2}]
    pipeline.run(data, table_name="numbers")

    # future issue:
    # pipeline dataset should also see the new table after doing something
    new_data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    dataset.write(new_data, table_name="letters")
    assert dataset.schema.data_table_names() == ["numbers", "letters"]

    # without any additional action the pipeline will not be aware of the changed schema
    assert pipeline.dataset().schema != dataset.schema

    pipeline.sync_destination()
    assert pipeline.dataset().schema != dataset.schema

    # only when explicitly prompted to download!
    pipeline.sync_destination(always_download_schemas=True)

    assert pipeline.dataset().schema == dataset.schema


def test_data_write_wipes_working_directory_and_persists_data_after_running(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    _, dataset, _, _ = pipeline_and_foo_dataset

    table_name = "bar"

    # create load package with faulty data
    with dataset.write_pipeline() as write_pipeline:
        write_pipeline.extract([{"id": 0, "value": "faulty"}], table_name=table_name)

    items = [{"id": 0, "value": "correct"}, {"id": 1, "value": "also correct"}]
    load_info = dataset.write(items, table_name=table_name)
    assert_load_info(load_info, expected_load_packages=1)

    assert dataset.table("bar").select("id", "value").fetchall() == [
        (0, "correct"),
        (1, "also correct"),
    ]
    storage_dir = pathlib.Path(TEST_STORAGE_ROOT)
    pipeline_dir = storage_dir / ".dlt" / "pipelines" / write_pipeline.pipeline_name
    assert pipeline_dir.exists()


def test_data_write_overwrite_mode( pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]):
    pipeline, dataset, dataset_name, destination = pipeline_and_foo_dataset

    @dlt.resource(write_disposition="append")
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data  

    table_name = "bar"

    data_as_str = [{"id": 0, "value": "is a string"}, {"id": 1, "value": "is also a string"}]

    pipeline.run(identity_resource(data_as_str), table_name=table_name)

    assert dataset.schema.tables[table_name]["columns"]["value"]["data_type"] == "text"

    # execute
    same_data_as_int = [{"id": 0, "value": 0}, {"id": 1, "value": 1}]
    load_info = dataset.write(identity_resource(same_data_as_int), table_name=table_name, overwrite=True)
    assert_load_info(load_info, expected_load_packages=1)

    # verify
    new_dataset = dlt.dataset(destination, dataset_name)

    # assert exsiting content got dropped
    assert new_dataset.row_counts().fetchall() == [("bar", 2)]

    assert new_dataset.table("bar").select("id", "value").fetchall() == [
        (0, 0),
        (1, 1)
    ]

    assert new_dataset.schema.tables[table_name]["columns"]["value"]["data_type"] == "bigint"


def test_internal_pipeline_can_write_to_scratchpad_dataset(
    pipeline_and_foo_dataset: Tuple[dlt.Pipeline, dlt.Dataset, str, Destination]
):
    _, dataset, dataset_name, destination = pipeline_and_foo_dataset
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
