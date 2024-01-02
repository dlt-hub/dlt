import pytest
import os

import dlt
from dlt.common import json
from dlt.common.storages import (
    SchemaStorage,
    SchemaStorageConfiguration,
    NormalizeStorageConfiguration,
)
from dlt.common.storages.schema_storage import SchemaStorage

from dlt.extract import DltResource, DltSource
from dlt.extract.extract import ExtractStorage, Extract

from tests.utils import clean_test_storage, TEST_STORAGE_ROOT
from tests.extract.utils import expect_extracted_file


@pytest.fixture
def extract_step() -> Extract:
    clean_test_storage(init_normalize=True)
    schema_storage = SchemaStorage(
        SchemaStorageConfiguration(schema_volume_path=os.path.join(TEST_STORAGE_ROOT, "schemas")),
        makedirs=True,
    )
    return Extract(schema_storage, NormalizeStorageConfiguration())


def test_storage_reuse_package() -> None:
    storage = ExtractStorage(NormalizeStorageConfiguration())
    load_id = storage.create_load_package(dlt.Schema("first"))
    # assign the same load id if schema "fists" is being extracted
    assert storage.create_load_package(dlt.Schema("first")) == load_id
    load_id_2 = storage.create_load_package(dlt.Schema("second"))
    assert load_id_2 != load_id
    # make sure we have only two packages
    assert set(storage.new_packages.list_packages()) == {load_id, load_id_2}
    # commit
    storage.commit_new_load_package(load_id, dlt.Schema("first"))
    # we have a new load id (the package with schema moved to extracted)
    load_id_3 = storage.create_load_package(dlt.Schema("first"))
    assert load_id != load_id_3
    load_id_4 = storage.create_load_package(dlt.Schema("first"), reuse_exiting_package=False)
    assert load_id_4 != load_id_3

    # this will fail - not all extracts committed
    with pytest.raises(OSError):
        storage.delete_empty_extract_folder()
    # commit the rest
    storage.commit_new_load_package(load_id_2, dlt.Schema("second"))
    storage.commit_new_load_package(load_id_3, dlt.Schema("first"))
    storage.commit_new_load_package(load_id_4, dlt.Schema("first"))
    storage.delete_empty_extract_folder()

    # list extracted packages
    assert set(storage.extracted_packages.list_packages()) == {
        load_id,
        load_id_2,
        load_id_3,
        load_id_4,
    }


def test_extract_select_tables_mark(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    @dlt.resource
    def table_with_name_selectable(_range):
        for i in range(_range):
            yield dlt.mark.with_table_name(i, n_f(i))

    schema = expect_tables(extract_step, table_with_name_selectable)
    # TODO: this one should not be there but we cannot remove it really, except explicit flag
    assert "table_with_name_selectable" in schema.tables


def test_extract_select_tables_lambda(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
        yield list(range(_range))

    schema = expect_tables(extract_step, table_name_with_lambda)
    assert "table_name_with_lambda" not in schema.tables


# def test_extract_pipe_from_unknown_resource():
#         pass


def test_extract_shared_pipe(extract_step: Extract):
    def input_gen():
        yield from [1, 2, 3]

    input_r = DltResource.from_data(input_gen)
    source = DltSource(
        dlt.Schema("selectables"), "module", [input_r, input_r.with_name("gen_clone")]
    )
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source)
    # both tables got generated
    assert "input_gen" in source.schema._schema_tables
    assert "gen_clone" in source.schema._schema_tables


def test_extract_renamed_clone_and_parent(extract_step: Extract):
    def input_gen():
        yield from [1, 2, 3]

    def tx_step(item):
        return item * 2

    input_r = DltResource.from_data(input_gen)
    input_tx = DltResource.from_data(tx_step, data_from=DltResource.Empty)

    source = DltSource(
        dlt.Schema("selectables"), "module", [input_r, (input_r | input_tx).with_name("tx_clone")]
    )
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source)
    assert "input_gen" in source.schema._schema_tables
    assert "tx_clone" in source.schema._schema_tables
    # mind that pipe name of the evaluated parent will have different name than the resource
    assert source.tx_clone._pipe.parent.name == "input_gen_tx_clone"


def expect_tables(extract_step: Extract, resource: DltResource) -> dlt.Schema:
    source = DltSource(dlt.Schema("selectables"), "module", [resource(10)])
    schema = source.discover_schema()

    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source)
    # odd and even tables must be in the source schema
    assert len(source.schema.data_tables(include_incomplete=True)) == 2
    assert "odd_table" in source.schema._schema_tables
    assert "even_table" in source.schema._schema_tables
    # you must commit the files
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 0
    extract_step.extract_storage.commit_new_load_package(load_id, source.schema)
    # check resulting files
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 2
    expect_extracted_file(
        extract_step.extract_storage, "selectables", "odd_table", json.dumps([1, 3, 5, 7, 9])
    )
    expect_extracted_file(
        extract_step.extract_storage, "selectables", "even_table", json.dumps([0, 2, 4, 6, 8])
    )

    # same thing but select only odd
    source = DltSource(dlt.Schema("selectables"), "module", [resource])
    source = source.with_resources(resource.name)
    source.selected_resources[resource.name].bind(10).select_tables("odd_table")
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source)
    assert len(source.schema.data_tables(include_incomplete=True)) == 1
    assert "odd_table" in source.schema._schema_tables
    extract_step.extract_storage.commit_new_load_package(load_id, source.schema)
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 3
    expect_extracted_file(
        extract_step.extract_storage,
        "selectables",
        "odd_table",
        json.dumps([1, 3, 5, 7, 9]),
        expected_files=2,
    )
    extract_step.extract_storage.delete_empty_extract_folder()

    return schema
