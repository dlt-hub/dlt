import pytest

import pandas as pd
import pyarrow as pa
from typing import Any, Union

import dlt
from dlt.common import Decimal
from dlt.common.utils import uniq_id


# Create mock dataframe with string, float, int, datetime (w timezone), time, date, binary, decimal (of varying scale/precision) and json columns

def make_df():
    df = pd.DataFrame(
        {
            "string": ["a", "b", "c"],
            "float": [1.0, 2.0, 3.0],
            "int": [1, 2, 3],
            "datetime": pd.date_range("2021-01-01", periods=3, tz="UTC"),
            "time": pd.date_range("2021-01-01", periods=3, tz="UTC").time,
            "date": pd.date_range("2021-01-01", periods=3, tz="UTC").date,
            "binary": [b"a", b"b", b"c"],
            "decimal": [Decimal("1.0"), Decimal("2.0"), Decimal("3.0")],
            "json": [{"a": 1}, {"b": 2}, {"c": 3}],
        }
    )
    return df


def make_arrow_table():
    df = make_df()
    return pa.Table.from_pandas(df)

def make_arrow_record_batch():
    df = make_df()
    return pa.RecordBatch.from_pandas(df)



def make_data_item(item_type: str) -> Any:
    if item_type == "pandas":
        return make_df()
    elif item_type == "table":
        return make_arrow_table()
    elif item_type == "record_batch":
        return make_arrow_record_batch()
    raise ValueError("Unknown item type: " + item_type)


def item_to_arrow_table(item: Union[pa.Table, pa.RecordBatch, pd.DataFrame]) -> pa.Table:
    if isinstance(item, pa.Table):
        return item
    elif isinstance(item, pa.RecordBatch):
        return pa.Table.from_batches([item])
    elif isinstance(item, pd.DataFrame):
        return pa.Table.from_pandas(item)
    raise ValueError("Unknown item type: " + str(type(item)))


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_extract_and_normalize(item_type: str):
    item = make_data_item(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="filesystem")

    @dlt.resource
    def some_data():
        yield item


    pipeline.extract(some_data())
    pipeline.normalize()


@pytest.mark.parametrize("item_type", ["pandas", "table"])
def test_extract_with_incremental(item_type: str):
    item = make_data_item(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="filesystem")

    @dlt.resource
    def some_data(incremental = dlt.sources.incremental("datetime")):
        yield item

    pipeline.extract(some_data())
    pipeline.normalize()
