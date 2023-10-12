import pytest

import pandas as pd
from typing import Any, Union

import dlt
from dlt.common import Decimal
from dlt.common.utils import uniq_id
from dlt.common.exceptions import TerminalValueError
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.cases import arrow_table_all_data_types, TArrowFormat


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_extract_and_normalize(item_type: TArrowFormat):
    item, records = arrow_table_all_data_types(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="filesystem")

    @dlt.resource
    def some_data():
        yield item


    pipeline.extract(some_data())
    info = pipeline.normalize()

    assert info.row_counts['some_data'] == len(records)
    # TODO: Find the load id dir, check the normalized files are matching


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_normalize_unsupported_loader_format(item_type: TArrowFormat):
    item, _ = arrow_table_all_data_types(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="dummy")

    @dlt.resource
    def some_data():
        yield item

    pipeline.extract(some_data())
    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.normalize()

    assert "The destination doesn't support direct loading of arrow tables" in str(py_ex.value)


@pytest.mark.parametrize("item_type", ["pandas", "table"])
def test_extract_with_incremental(item_type: TArrowFormat):
    # TODO: Add complete incremental tests or include with existing tests in extract/incremental
    item, _ = arrow_table_all_data_types(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="filesystem")

    @dlt.resource
    def some_data(incremental = dlt.sources.incremental("datetime")):
        yield item

    pipeline.extract(some_data())
    pipeline.normalize()
