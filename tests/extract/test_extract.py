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
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, ResourceExtractionError
from dlt.extract.extract import ExtractStorage, Extract
from dlt.extract.hints import make_hints

from dlt.extract.items import TableNameMeta
from tests.utils import MockPipeline, clean_test_storage, TEST_STORAGE_ROOT
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
    assert "table_with_name_selectable" not in schema.tables


def test_extract_select_tables_lambda(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
        yield list(range(_range))

    schema = expect_tables(extract_step, table_name_with_lambda)
    assert "table_name_with_lambda" not in schema.tables


def test_make_hints_default() -> None:
    hints = make_hints()
    assert hints == {"columns": {}}

    hints = make_hints(write_disposition=None)
    assert hints == {"columns": {}}


def test_extract_hints_mark(extract_step: Extract) -> None:
    @dlt.resource
    def with_table_hints():
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A"},
            make_hints(columns=[{"name": "id", "data_type": "bigint"}], primary_key="pk"),
        )
        schema = dlt.current.source_schema()
        # table and columns got updated in the schema
        assert "with_table_hints" in schema.tables
        table = schema.tables["with_table_hints"]
        assert "pk" in table["columns"]
        assert "id" in table["columns"]
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"
        # get the resource
        resource = dlt.current.source().resources[dlt.current.resource_name()]
        table = resource.compute_table_schema()
        # also there we see the hints
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"

        # add more columns and primary key
        yield dlt.mark.with_hints(
            {"id": 1, "pk2": "B"},
            make_hints(
                write_disposition="merge",
                file_format="preferred",
                columns=[{"name": "id", "precision": 16}, {"name": "text", "data_type": "decimal"}],
                primary_key="pk2",
            ),
        )
        # previous columns kept
        table = resource.compute_table_schema()
        assert schema is dlt.current.source().schema
        # previous primary key is gone from the resource
        assert "pk" not in table["columns"]
        assert table["columns"]["id"]["data_type"] == "bigint"
        assert table["columns"]["id"]["precision"] == 16
        assert "text" in table["columns"]
        assert table["write_disposition"] == "merge"
        # still it is kept in the schema that is merged from resource each time it changes
        table = schema.tables["with_table_hints"]
        assert "pk" in table["columns"]
        assert "text" in table["columns"]
        assert table["write_disposition"] == "merge"
        assert table["file_format"] == "preferred"

        # make table name dynamic
        yield dlt.mark.with_hints(
            {"namer": "dynamic"}, make_hints(table_name=lambda item: f"{item['namer']}_table")
        )
        # dynamic table was created in the schema and it contains the newest resource table schema
        table = schema.tables["dynamic_table"]
        # so pk is not available
        assert "pk" not in table["columns"]
        assert "pk2" in table["columns"]
        assert "id" in table["columns"]
        assert "text" in table["columns"]
        # get dynamic schema from resource
        with pytest.raises(DataItemRequiredForDynamicTableHints):
            table = resource.compute_table_schema()

    source = DltSource(dlt.Schema("hintable"), "module", [with_table_hints])
    extract_step.extract(source, 20, 1)
    table = source.schema.tables["dynamic_table"]
    assert "pk" not in table["columns"]


def test_extract_hints_table_variant(extract_step: Extract) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    @dlt.resource(primary_key="pk")
    def with_table_hints():
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A"},
            make_hints(table_name="table_a", columns=[{"name": "id", "data_type": "bigint"}]),
            create_table_variant=True,
        )
        # get the resource
        resource = dlt.current.source().resources[dlt.current.resource_name()]
        assert "table_a" in resource._hints_variants
        # get table
        table = resource.compute_table_schema(meta=TableNameMeta("table_a"))
        assert "pk" in table["columns"]
        assert "id" in table["columns"]
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"

        schema = dlt.current.source_schema()
        # table table_a will be created
        assert "table_a" in schema.tables
        schema_table = schema.tables["table_a"]
        assert table == schema_table

        # dispatch to table b
        yield dlt.mark.with_hints(
            {"id": 2, "pk": "B"},
            make_hints(table_name="table_b", write_disposition="replace"),
            create_table_variant=True,
        )
        assert "table_b" in resource._hints_variants
        # get table
        table = resource.compute_table_schema(meta=TableNameMeta("table_b"))
        assert table["write_disposition"] == "replace"
        schema_table = schema.tables["table_b"]
        assert table == schema_table

        # item to resource
        yield {"id": 3, "pk": "C"}

    source = DltSource(dlt.Schema("hintable"), "module", [with_table_hints])
    extract_step.extract(source, 20, 1)


def test_extract_metrics_on_exception_no_flush(extract_step: Extract) -> None:
    @dlt.resource
    def letters():
        # extract 7 items
        yield from "ABCDEFG"
        # then fail
        raise RuntimeError()
        yield from "HI"

    source = DltSource(dlt.Schema("letters"), "module", [letters])
    with pytest.raises(ResourceExtractionError):
        extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    # no jobs were created
    assert len(step_info.load_packages[0].jobs["new_jobs"]) == 0
    # make sure all writers are closed but not yet removed
    current_load_id = step_info.loads_ids[-1] if len(step_info.loads_ids) > 0 else None
    # get buffered writers
    writers = extract_step.extract_storage.item_storages["object"].buffered_writers
    assert len(writers) == 1
    for name, writer in writers.items():
        assert name.startswith(current_load_id)
        assert writer._file is None


def test_extract_metrics_on_exception_without_flush(extract_step: Extract) -> None:
    @dlt.resource
    def letters():
        # extract 7 items
        yield from "ABCDEFG"
        # then fail
        raise RuntimeError()
        yield from "HI"

    # flush buffer
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "4"
    source = DltSource(dlt.Schema("letters"), "module", [letters])
    with pytest.raises(ResourceExtractionError):
        extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    # one job created because the file was flushed
    jobs = step_info.load_packages[0].jobs["new_jobs"]
    # print(jobs[0].job_file_info.job_id())
    assert len(jobs) == 1
    current_load_id = step_info.loads_ids[-1] if len(step_info.loads_ids) > 0 else None
    # 7 items were extracted
    assert (
        step_info.metrics[current_load_id][0]["job_metrics"][
            jobs[0].job_file_info.job_id()
        ].items_count
        == 4
    )
    # get buffered writers
    writers = extract_step.extract_storage.item_storages["object"].buffered_writers
    assert len(writers) == 1
    for name, writer in writers.items():
        assert name.startswith(current_load_id)
        assert writer._file is None


def test_extract_empty_metrics(extract_step: Extract) -> None:
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    assert step_info.load_packages == step_info.loads_ids == []


# def test_extract_pipe_from_unknown_resource():
#         pass


def test_extract_shared_pipe(extract_step: Extract):
    def input_gen():
        yield from [1, 2, 3]

    input_r = DltResource.from_data(input_gen)
    source = DltSource(
        dlt.Schema("selectables"), "module", [input_r, input_r.with_name("gen_clone")]
    )
    extract_step.extract(source, 20, 1)
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
    extract_step.extract(source, 20, 1)
    assert "input_gen" in source.schema._schema_tables
    assert "tx_clone" in source.schema._schema_tables
    # mind that pipe name of the evaluated parent will have different name than the resource
    assert source.tx_clone._pipe.parent.name == "input_gen_tx_clone"


def expect_tables(extract_step: Extract, resource: DltResource) -> dlt.Schema:
    source = DltSource(dlt.Schema("selectables"), "module", [resource(10)])
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source, max_parallel_items=5, workers=1)
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
    schema = source.schema

    # same thing but select only odd
    source = DltSource(dlt.Schema("selectables"), "module", [resource])
    source = source.with_resources(resource.name)
    source.selected_resources[resource.name].bind(10).select_tables("odd_table")
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source, max_parallel_items=5, workers=1)
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
