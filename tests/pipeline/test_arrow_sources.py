import os
import pytest

import pandas as pd
import numpy as np
import os
import io
import pyarrow as pa

import dlt
from dlt.common import json, Decimal
from dlt.common.utils import uniq_id
from dlt.common.libs.pyarrow import NameNormalizationClash

from dlt.pipeline.exceptions import PipelineStepFailed

from tests.cases import arrow_format_from_pandas, arrow_table_all_data_types, TArrowFormat
from tests.utils import preserve_environ


@pytest.mark.parametrize(
    ("item_type", "is_list"),
    [
        ("pandas", False),
        ("table", False),
        ("record_batch", False),
        ("pandas", True),
        ("table", True),
        ("record_batch", True),
    ],
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
    extract_files = [
        fn for fn in norm_storage.list_files_to_normalize_sorted() if fn.endswith(".parquet")
    ]

    assert len(extract_files) == 1

    with norm_storage.extracted_packages.storage.open_file(extract_files[0], "rb") as f:
        extracted_bytes = f.read()

    info = pipeline.normalize()

    assert info.row_counts["some_data"] == len(records)

    load_id = pipeline.list_normalized_load_packages()[0]
    storage = pipeline._get_load_storage()
    jobs = storage.normalized_packages.list_new_jobs(load_id)
    job = [j for j in jobs if "some_data" in j][0]
    with storage.normalized_packages.storage.open_file(job, "rb") as f:
        normalized_bytes = f.read()

        # Normalized is linked/copied exactly and should be the same as the extracted file
        assert normalized_bytes == extracted_bytes

        f.seek(0)
        pq = pa.parquet.ParquetFile(f)
        tbl = pq.read()

        # To make tables comparable exactly write the expected data to parquet and read it back
        # The spark parquet writer loses timezone info
        tbl_expected = pa.Table.from_pandas(pd.DataFrame(records))
        with io.BytesIO() as f:
            pa.parquet.write_table(tbl_expected, f, flavor="spark")
            f.seek(0)
            tbl_expected = pa.parquet.read_table(f)
        df_tbl = tbl_expected.to_pandas(ignore_metadata=True)
        # Data is identical to the original dataframe
        df_result = tbl.to_pandas(ignore_metadata=True)
        assert df_result.equals(df_tbl)

    schema = pipeline.default_schema

    # Check schema detection
    schema_columns = schema.tables["some_data"]["columns"]
    assert set(df_tbl.columns) == set(schema_columns)
    assert schema_columns["date"]["data_type"] == "date"
    assert schema_columns["int"]["data_type"] == "bigint"
    assert schema_columns["float"]["data_type"] == "double"
    assert schema_columns["decimal"]["data_type"] == "decimal"
    assert schema_columns["time"]["data_type"] == "time"
    assert schema_columns["binary"]["data_type"] == "binary"
    assert schema_columns["string"]["data_type"] == "text"
    assert schema_columns["json"]["data_type"] == "complex"


@pytest.mark.parametrize(
    ("item_type", "is_list"),
    [
        ("pandas", False),
        ("table", False),
        ("record_batch", False),
        ("pandas", True),
        ("table", True),
        ("record_batch", True),
    ],
)
def test_normalize_jsonl(item_type: TArrowFormat, is_list: bool):
    os.environ["DUMMY__LOADER_FILE_FORMAT"] = "jsonl"

    item, records = arrow_table_all_data_types(item_type)

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="dummy")

    @dlt.resource
    def some_data():
        if is_list:
            yield [item]
        else:
            yield item

    pipeline.extract(some_data())
    pipeline.normalize()

    load_id = pipeline.list_normalized_load_packages()[0]
    storage = pipeline._get_load_storage()
    jobs = storage.normalized_packages.list_new_jobs(load_id)
    job = [j for j in jobs if "some_data" in j][0]
    with storage.normalized_packages.storage.open_file(job, "r") as f:
        result = [json.loads(line) for line in f]
        for row in result:
            row["decimal"] = Decimal(row["decimal"])

    for record in records:
        record["datetime"] = record["datetime"].replace(tzinfo=None)

    expected = json.loads(json.dumps(records))
    for record in expected:
        record["decimal"] = Decimal(record["decimal"])
    assert result == expected


@pytest.mark.parametrize("item_type", ["table", "record_batch"])
def test_add_map(item_type: TArrowFormat):
    item, records = arrow_table_all_data_types(item_type, num_rows=200)

    @dlt.resource
    def some_data():
        yield item

    def map_func(item):
        return item.filter(pa.compute.greater(item["int"], 80))

    # Add map that filters the table
    some_data.add_map(map_func)

    result = list(some_data())
    assert len(result) == 1
    result_tbl = result[0]

    assert len(result_tbl) < len(item)
    assert pa.compute.all(pa.compute.greater(result_tbl["int"], 80)).as_py()


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_extract_normalize_file_rotation(item_type: TArrowFormat) -> None:
    # do not extract state
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # use parquet for dummy
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item, rows = arrow_table_all_data_types(item_type)

    @dlt.resource
    def data_frames():
        for _ in range(10):
            yield item

    # get buffer written and file rotated with each yielded frame
    os.environ[f"SOURCES__{pipeline_name.upper()}__DATA_WRITER__BUFFER_MAX_ITEMS"] = str(len(rows))
    os.environ[f"SOURCES__{pipeline_name.upper()}__DATA_WRITER__FILE_MAX_ITEMS"] = str(len(rows))

    pipeline.extract(data_frames())
    # ten parquet files
    assert len(pipeline.list_extracted_resources()) == 10
    info = pipeline.normalize(workers=3)
    # with 10 * num rows
    assert info.row_counts["data_frames"] == 10 * len(rows)
    load_id = pipeline.list_normalized_load_packages()[0]
    # 10 jobs on parquet files
    assert len(pipeline.get_load_package_info(load_id).jobs["new_jobs"]) == 10


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_arrow_clashing_names(item_type: TArrowFormat) -> None:
    # # use parquet for dummy
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"
    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item, _ = arrow_table_all_data_types(item_type, include_name_clash=True)

    @dlt.resource
    def data_frames():
        for _ in range(10):
            yield item

    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.extract(data_frames())
    assert isinstance(py_ex.value.__context__, NameNormalizationClash)


@pytest.mark.parametrize("item_type", ["table", "record_batch"])
def test_load_arrow_vary_schema(item_type: TArrowFormat) -> None:
    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

    item, _ = arrow_table_all_data_types(item_type, include_not_normalized_name=False)
    pipeline.run(item, table_name="data").raise_on_failed_jobs()

    item, _ = arrow_table_all_data_types(item_type, include_not_normalized_name=False)
    # remove int column
    try:
        item = item.drop("int")
    except AttributeError:
        names = item.schema.names
        names.remove("int")
        item = item.select(names)
    pipeline.run(item, table_name="data").raise_on_failed_jobs()


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_arrow_as_data_loading(item_type: TArrowFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    item, rows = arrow_table_all_data_types(item_type)

    item_resource = dlt.resource(item, name="item")
    assert id(item) == id(list(item_resource)[0])

    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    pipeline.extract(item, table_name="items")
    assert len(pipeline.list_extracted_resources()) == 1
    info = pipeline.normalize()
    assert info.row_counts["items"] == len(rows)


@pytest.mark.parametrize("item_type", ["table"])  # , "pandas", "record_batch"
def test_normalize_with_dlt_columns(item_type: TArrowFormat):
    item, records = arrow_table_all_data_types(item_type, num_rows=5432)
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "True"
    # Test with buffer smaller than the number of batches to be written
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "100"
    os.environ["DATA_WRITER__ROW_GROUP_SIZE"] = "100"

    @dlt.resource
    def some_data():
        yield item

    pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="duckdb")

    pipeline.extract(some_data())
    pipeline.normalize(loader_file_format="parquet")

    load_id = pipeline.list_normalized_load_packages()[0]
    storage = pipeline._get_load_storage()
    jobs = storage.normalized_packages.list_new_jobs(load_id)
    job = [j for j in jobs if "some_data" in j][0]
    with storage.normalized_packages.storage.open_file(job, "rb") as f:
        tbl = pa.parquet.read_table(f)

        assert len(tbl) == 5432

        # Test one column matches source data
        assert tbl["string"].to_pylist() == [r["string"] for r in records]

        assert pa.compute.all(pa.compute.equal(tbl["_dlt_load_id"], load_id)).as_py()

        all_ids = tbl["_dlt_id"].to_pylist()
        assert len(all_ids[0]) >= 14

        # All ids are unique
        assert len(all_ids) == len(set(all_ids))

    # _dlt_id and _dlt_load_id are added to pipeline schema
    schema = pipeline.default_schema
    assert schema.tables["some_data"]["columns"]["_dlt_id"]["data_type"] == "text"
    assert schema.tables["some_data"]["columns"]["_dlt_load_id"]["data_type"] == "text"

    pipeline.load().raise_on_failed_jobs()

    # should be able to load again
    pipeline.run(some_data()).raise_on_failed_jobs()

    # should be able to load arrow without a column
    try:
        item = item.drop("int")
    except AttributeError:
        names = item.schema.names
        names.remove("int")
        item = item.select(names)
    pipeline.run(item, table_name="some_data").raise_on_failed_jobs()

    # should be able to load arrow with a new column
    # TODO: uncomment when load_id fixed in normalizer
    # item, records = arrow_table_all_data_types(item_type, num_rows=200)
    # item = item.append_column("static_int", [[0] * 200])
    # pipeline.run(item, table_name="some_data").raise_on_failed_jobs()

    # schema = pipeline.default_schema
    # assert schema.tables['some_data']['columns']['static_int']['data_type'] == 'bigint'


@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_empty_arrow(item_type: TArrowFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    # always return pandas
    item, _ = arrow_table_all_data_types("pandas", num_rows=1)
    item_resource = dlt.resource(item, name="items", write_disposition="replace")

    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    # E & L
    info = pipeline.extract(item_resource)
    load_id = info.loads_ids[0]
    assert info.metrics[load_id][0]["table_metrics"]["items"].items_count == 1
    assert len(pipeline.list_extracted_resources()) == 1
    norm_info = pipeline.normalize()
    assert norm_info.row_counts["items"] == 1

    # load 0 elements to replace
    empty_df = pd.DataFrame(columns=item.columns)

    item_resource = dlt.resource(
        arrow_format_from_pandas(empty_df, item_type), name="items", write_disposition="replace"
    )
    info = pipeline.extract(item_resource)
    load_id = info.loads_ids[0]
    assert info.metrics[load_id][0]["table_metrics"]["items"].items_count == 0
    assert len(pipeline.list_extracted_resources()) == 1
    norm_info = pipeline.normalize()
    assert norm_info.row_counts["items"] == 0
