import pytest
from itertools import zip_longest

from dlt.extract.extract import ExtractorStorage


def expect_extracted_file(storage: ExtractorStorage, schema_name: str, table_name: str, content: str) -> None:
    files = storage.list_files_to_normalize_sorted()
    gen = (file for file in files if storage.get_schema_name(file) == schema_name and storage.parse_normalize_file_name(file).table_name == table_name)
    file = next(gen, None)
    if file is None:
        raise FileNotFoundError(storage.build_extracted_file_stem(schema_name, table_name, "***"))
    assert file is not None
    # only one file expected
    with pytest.raises(StopIteration):
        next(gen)
    # load file and parse line by line
    file_content: str = storage.storage.load(file)
    if content == "***":
        return
    for line, file_line in zip_longest(content.splitlines(), file_content.splitlines()):
        assert line == file_line
