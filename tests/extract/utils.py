from typing import Any, Optional, List, Union, Literal, get_args
import pytest
from itertools import zip_longest, chain

from dlt.common.typing import TDataItem, TDataItems, TAny

from dlt.extract.extract import ExtractorStorage
from dlt.extract.typing import ItemTransform, ItemTransformFunc
from tests.cases import TArrowFormat

import pandas as pd
from dlt.common.libs.pyarrow import pyarrow as pa


TItemFormat = Literal["json", "pandas", "arrow"]

ALL_ITEM_FORMATS = get_args(TItemFormat)


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
     def __init__(self, expected_items: Any, item_type: TItemFormat = "json") -> None:
         self.expected_items = expected_items
         self.item_type = item_type

     def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
        assert data_item_to_list(self.item_type, item) == self.expected_items
        return item


def data_to_item_format(item_format: TItemFormat, data: List[TDataItem]):
    """Return the given data in the form of pandas, arrow table or json items"""
    if item_format == "json":
        return data
    # Make dataframe from the data
    df = pd.DataFrame(data)
    if item_format == "pandas":
        return [df]
    elif item_format == "arrow":
        return [pa.Table.from_pandas(df)]
    else:
        raise ValueError(f"Unknown item format: {item_format}")


def data_item_to_list(from_type: TItemFormat, values: List[TDataItem]):
    if from_type == "arrow":
        return values[0].to_pylist()
    elif from_type == "pandas":
        return values[0].to_dict("records")
    return values
