import pytest

import pandas as pd
from typing import Any, Union
import pyarrow as pa

import dlt
from dlt.common import Decimal
from dlt.common.utils import uniq_id
from dlt.common.exceptions import TerminalValueError
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.cases import arrow_table_all_data_types, TArrowFormat
from dlt.common.storages import LoadStorage



@pytest.mark.parametrize(
    ("item_type", "is_list"), [("pandas", False), ("table", False), ("record_batch", False), ("pandas", True), ("table", True), ("record_batch", True)]
)
def test_extract_and_normalize(item_type: TArrowFormat, is_list: bool):
    item, records = arrow_table_all_data_types(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="filesystem")

    @dlt.resource
    def some_data():
        if is_list:
            yield [item]
        else:
            yield item


    pipeline.extract(some_data())
    norm_storage = pipeline._get_normalize_storage()
    extract_files = [fn for fn in norm_storage.list_files_to_normalize_sorted() if fn.endswith(".parquet")]

    assert len(extract_files) == 1

    with norm_storage.storage.open_file(extract_files[0], 'rb') as f:
        extracted_bytes = f.read()

    info = pipeline.normalize()

    assert info.row_counts['some_data'] == len(records)

    load_id = pipeline.list_normalized_load_packages()[0]
    storage = pipeline._get_load_storage()
    jobs = storage.list_new_jobs(load_id)
    with storage.storage.open_file(jobs[0], 'rb') as f:
        normalized_bytes = f.read()

        # Normalized is linked/copied exactly and should be the same as the extracted file
        assert normalized_bytes == extracted_bytes

        f.seek(0)
        pq = pa.parquet.ParquetFile(f)
        tbl = pq.read()

        # Make the dataframes comparable exactly
        df_tbl = pa.Table.from_pandas(pd.DataFrame(records)).to_pandas()
        # Data is identical to the original dataframe
        assert (tbl.to_pandas() == df_tbl).all().all()

    schema = pipeline.default_schema

    # Check schema detection
    schema_columns = schema.tables['some_data']['columns']
    assert set(df_tbl.columns) == set(schema_columns)
    assert schema_columns['date']['data_type'] == 'date'
    assert schema_columns['int']['data_type'] == 'bigint'
    assert schema_columns['float']['data_type'] == 'double'
    assert schema_columns['decimal']['data_type'] == 'decimal'
    assert schema_columns['time']['data_type'] == 'time'
    assert schema_columns['binary']['data_type'] == 'binary'
    assert schema_columns['string']['data_type'] == 'text'
    assert schema_columns['json']['data_type'] == 'complex'


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


@pytest.mark.parametrize("item_type", ["table", "record_batch"])
def test_add_map(item_type: TArrowFormat):
    item, _ = arrow_table_all_data_types(item_type)

    @dlt.resource
    def some_data():
        yield item

    def map_func(item):
        return item.filter(pa.compute.equal(item['int'], 1))

    # Add map that filters the table
    some_data.add_map(map_func)

    result = list(some_data())

    assert len(result) == 1
    assert result[0]['int'][0].as_py() == 1
