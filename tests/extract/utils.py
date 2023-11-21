from typing import Any, Optional, List
import pytest
from itertools import zip_longest

from dlt.common.typing import TDataItem, TDataItems

from dlt.extract.extract import ExtractorStorage
from dlt.extract.typing import ItemTransform

from tests.utils import TDataItemFormat


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


class AssertItems(ItemTransform[TDataItem]):
     def __init__(self, expected_items: Any, item_type: TDataItemFormat = "json") -> None:
         self.expected_items = expected_items
         self.item_type = item_type

     def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        assert data_item_to_list(self.item_type, item) == self.expected_items
        return item


def data_item_to_list(from_type: TDataItemFormat, values: List[TDataItem]):
    if from_type in ["arrow", "arrow-batch"]:
        return values[0].to_pylist()
    elif from_type == "pandas":
        return values[0].to_dict("records")
    return values
