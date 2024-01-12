from typing import List, Tuple

import dlt
import pytest

from copy import deepcopy
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
from dlt.common.data_writers.writers import TLoaderFileFormat

from tests.load.utils import (
    TABLE_ROW_ALL_DATA_TYPES,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    assert_all_data_types_row,
    delete_dataset,
)

SUPPORTED_LOADER_FORMATS = ["parquet", "jsonl", "insert_values"]


def _run_through_sink(
    items: TDataItems,
    loader_file_format: TLoaderFileFormat,
    columns=None,
    filter_dlt_tables: bool = True,
) -> List[Tuple[TDataItems, TTableSchema]]:
    """
    runs a list of items through the sink destination and returns colleceted calls
    """
    calls: List[Tuple[TDataItems, TTableSchema]] = []

    @dlt.sink(loader_file_format=loader_file_format, batch_size=1)
    def test_sink(items: TDataItems, table: TTableSchema) -> None:
        nonlocal calls
        if table["name"].startswith("_dlt") and filter_dlt_tables:
            return
        calls.append((items, table))

    @dlt.resource(columns=columns, table_name="items")
    def items_resource() -> TDataItems:
        nonlocal items
        yield items

    p = dlt.pipeline("sink_test", destination=test_sink, full_refresh=True)
    p.run([items_resource()])

    return calls


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
def test_all_datatypes(loader_file_format: TLoaderFileFormat) -> None:
    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    sink_calls = _run_through_sink(data_types, loader_file_format, columns=column_schemas)

    # inspect result
    assert len(sink_calls) == 1

    item = sink_calls[0][0]
    # filter out _dlt columns
    item = {k: v for k, v in item.items() if not k.startswith("_dlt")}

    # null values are not saved in jsonl (TODO: is this correct?)
    if loader_file_format == "jsonl":
        data_types = {k: v for k, v in data_types.items() if v is not None}

    # check keys are the same
    assert set(item.keys()) == set(data_types.keys())

    # TODO: check actual types
    # assert_all_data_types_row


@pytest.mark.parametrize("loader_file_format", SUPPORTED_LOADER_FORMATS)
def test_batch_size(loader_file_format: TLoaderFileFormat) -> None:
    pass
