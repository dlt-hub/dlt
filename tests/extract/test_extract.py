import pytest
from itertools import zip_longest

import dlt
from dlt.common import json
from dlt.common.configuration.specs import NormalizeVolumeConfiguration
from dlt.extract.extract import ExtractorStorage, extract
from dlt.extract.source import DltResource, DltSource

from tests.utils import autouse_test_storage, clean_test_storage


def test_extract_select_tables() -> None:

    def expect_tables(resource: DltResource) -> dlt.Schema:
        # delete files
        clean_test_storage()
        source = DltSource("selectables", dlt.Schema("selectables"), [resource(10)])
        schema = source.discover_schema()

        storage = ExtractorStorage(NormalizeVolumeConfiguration())
        schema_update = extract(source, storage)
        # odd and even tables
        assert len(schema_update) == 2
        assert "odd_table" in schema_update
        assert "even_table" in schema_update
        for partials in schema_update.values():
            assert len(partials) == 1
        # check resulting files
        assert len(storage.list_files_to_normalize_sorted()) == 2
        expect_extracted_file(storage, "selectables", "odd_table", json.dumps([1,3,5,7,9]))
        expect_extracted_file(storage, "selectables", "even_table", json.dumps([0,2,4,6,8]))


        # delete files
        clean_test_storage()
        # same thing but select only odd
        source = DltSource("selectables", dlt.Schema("selectables"), [resource])
        source.with_resources(resource.name).selected_resources[resource.name](10).select_tables("odd_table")
        storage = ExtractorStorage(NormalizeVolumeConfiguration())
        schema_update = extract(source, storage)
        assert len(schema_update) == 1
        assert "odd_table" in schema_update
        for partials in schema_update.values():
            assert len(partials) == 1
        assert len(storage.list_files_to_normalize_sorted()) == 1
        expect_extracted_file(storage, "selectables", "odd_table", json.dumps([1,3,5,7,9]))

        return schema

    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    @dlt.resource
    def table_with_name_selectable(_range):
        for i in range(_range):
            yield dlt.with_table_name(i, n_f(i))

    schema = expect_tables(table_with_name_selectable)
    # TODO: this one should not be there but we cannot remove it really, except explicit flag
    assert "table_with_name_selectable" in schema._schema_tables

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
            yield list(range(_range))

    schema = expect_tables(table_name_with_lambda)
    assert "table_name_with_lambda" not in schema._schema_tables


def expect_extracted_file(storage: ExtractorStorage, schema_name: str, table_name: str, content: str) -> None:
    files = storage.list_files_to_normalize_sorted()
    gen = (file for file in files if storage.get_schema_name(file) == schema_name and storage.parse_normalize_file_name(file).table_name == table_name)
    file = next(gen, None)
    assert file is not None
    # only one file expected
    with pytest.raises(StopIteration):
        next(gen)
    # load file and parse line by line
    file_content: str = storage.storage.load(file)
    for line, file_line in zip_longest(content.splitlines(), file_content.splitlines()):
        assert line == file_line
