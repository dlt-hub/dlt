from typing import Any, Optional, List
import pytest
from itertools import zip_longest

from dlt.common.storages import PackageStorage, ParsedLoadJobFileName
from dlt.common.typing import TDataItem, TDataItems

from dlt.extract.extract import ExtractStorage
from dlt.extract.items import ItemTransform

from tests.utils import TestDataItemFormat


def expect_extracted_file(
    storage: ExtractStorage,
    schema_name: str,
    table_name: str,
    content: str,
    expected_files: int = 1,
) -> None:
    load_ids = storage.extracted_packages.list_packages()
    gen = (
        file
        for load_id in load_ids
        for file in storage.extracted_packages.list_new_jobs(load_id)
        if storage.extracted_packages.schema_name(load_id) == schema_name
        and ParsedLoadJobFileName.parse(file).table_name == table_name
    )
    file = next(gen, None)
    if file is None:
        raise FileNotFoundError(
            PackageStorage.build_job_file_name(table_name, schema_name, validate_components=False)
        )
    assert file is not None
    # get remaining file names
    remaining_files = list(gen)
    assert (
        len(remaining_files) + 1 == expected_files
    ), f"Expected {expected_files} files for table {schema_name}:{table_name}"
    # load first file and parse line by line
    file_content: str = storage.extracted_packages.storage.load(file)
    if content == "***":
        return
    for line, file_line in zip_longest(content.splitlines(), file_content.splitlines()):
        assert line == file_line


class AssertItems(ItemTransform[TDataItem]):
    placement_affinity = 2.0  # even more sticky than incremental so gathers data after it

    def __init__(self, expected_items: Any, item_type: TestDataItemFormat = "object") -> None:
        self.expected_items = expected_items
        self.item_type = item_type

    def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        assert data_item_to_list(self.item_type, item) == self.expected_items
        return item


def data_item_to_list(from_type: TestDataItemFormat, values: List[TDataItem]):
    if from_type in ["arrow-table", "arrow-batch"]:
        return values[0].to_pylist()
    elif from_type == "pandas":
        return values[0].to_dict("records")
    return values
