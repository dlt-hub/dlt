import dlt
from dlt.common import json
from dlt.common.storages import NormalizeStorageConfiguration
from dlt.extract.extract import ExtractorStorage, extract
from dlt.extract.source import DltResource, DltSource

from tests.utils import clean_test_storage
from tests.extract.utils import expect_extracted_file


def test_extract_select_tables() -> None:

    def expect_tables(resource: DltResource) -> dlt.Schema:
        # delete files
        clean_test_storage()
        source = DltSource("selectables", "module", dlt.Schema("selectables"), [resource(10)])
        schema = source.discover_schema()

        storage = ExtractorStorage(NormalizeStorageConfiguration())
        extract_id = storage.create_extract_id()
        schema_update = extract(extract_id, source, storage)
        # odd and even tables
        assert len(schema_update) == 2
        assert "odd_table" in schema_update
        assert "even_table" in schema_update
        for partials in schema_update.values():
            assert len(partials) == 1
        # you must commit the files
        assert len(storage.list_files_to_normalize_sorted()) == 0
        storage.commit_extract_files(extract_id)
        # check resulting files
        assert len(storage.list_files_to_normalize_sorted()) == 2
        expect_extracted_file(storage, "selectables", "odd_table", json.dumps([1,3,5,7,9]))
        expect_extracted_file(storage, "selectables", "even_table", json.dumps([0,2,4,6,8]))


        # delete files
        clean_test_storage()
        storage = ExtractorStorage(NormalizeStorageConfiguration())
        # same thing but select only odd
        source = DltSource("selectables", "module", dlt.Schema("selectables"), [resource])
        source = source.with_resources(resource.name)
        source.selected_resources[resource.name].bind(10).select_tables("odd_table")
        extract_id = storage.create_extract_id()
        schema_update = extract(extract_id, source, storage)
        assert len(schema_update) == 1
        assert "odd_table" in schema_update
        for partials in schema_update.values():
            assert len(partials) == 1
        storage.commit_extract_files(extract_id)
        assert len(storage.list_files_to_normalize_sorted()) == 1
        expect_extracted_file(storage, "selectables", "odd_table", json.dumps([1,3,5,7,9]))

        return schema

    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    @dlt.resource
    def table_with_name_selectable(_range):
        for i in range(_range):
            yield dlt.mark.with_table_name(i, n_f(i))

    schema = expect_tables(table_with_name_selectable)
    # TODO: this one should not be there but we cannot remove it really, except explicit flag
    assert "table_with_name_selectable" in schema.tables

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
            yield list(range(_range))

    schema = expect_tables(table_name_with_lambda)
    assert "table_name_with_lambda" not in schema.tables
