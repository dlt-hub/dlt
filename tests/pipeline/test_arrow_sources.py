import os
from typing import Any
import pytest
import pandas as pd
import pyarrow as pa

import dlt
from dlt.common import json, Decimal
from dlt.common.utils import uniq_id
from dlt.common.libs.pyarrow import (
    NameNormalizationCollision,
    remove_columns,
    normalize_py_arrow_item,
)

from dlt.pipeline.exceptions import PipelineStepFailed

from tests.cases import (
    arrow_table_all_data_types,
    prepare_shuffled_tables,
)
from tests.pipeline.utils import assert_only_table_columns, load_tables_to_dicts
from tests.utils import (
    TPythonTableFormat,
    arrow_item_from_pandas,
    arrow_item_from_table,
)


@pytest.mark.parametrize(
    ("item_type", "is_list"),
    [
        ("pandas", False),
        ("arrow-table", False),
        ("arrow-batch", False),
        ("pandas", True),
        ("arrow-table", True),
        ("arrow-batch", True),
    ],
)
def test_extract_and_normalize(item_type: TPythonTableFormat, is_list: bool):
    item, records, data = arrow_table_all_data_types(item_type)

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

    info = pipeline.normalize(loader_file_format="parquet")

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
        with pa.parquet.ParquetFile(f) as pq:
            tbl = pq.read()

        # use original data to create data frame to preserve timestamp precision, timezones etc.
        tbl_expected = pa.Table.from_pandas(pd.DataFrame(data))
        # null is removed by dlt
        tbl_expected = remove_columns(tbl_expected, ["null"])
        # we want to normalize column names
        tbl_expected = normalize_py_arrow_item(
            tbl_expected,
            pipeline.default_schema.get_table_columns("some_data"),
            pipeline.default_schema.naming,
            None,
        )
        assert tbl_expected.schema.equals(tbl.schema)

        df_tbl = tbl_expected.to_pandas(ignore_metadata=False)
        # Data is identical to the original dataframe
        df_result = tbl.to_pandas(ignore_metadata=False)
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
    assert schema_columns["json"]["data_type"] == "json"


@pytest.mark.parametrize(
    ("item_type", "is_list"),
    [
        ("pandas", False),
        ("arrow-table", False),
        ("arrow-batch", False),
        ("pandas", True),
        ("arrow-table", True),
        ("arrow-batch", True),
    ],
)
def test_normalize_jsonl(item_type: TPythonTableFormat, is_list: bool):
    os.environ["DUMMY__LOADER_FILE_FORMAT"] = "jsonl"

    item, records, _ = arrow_table_all_data_types(item_type, tz="Europe/Berlin")

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

    expected = json.loads(json.dumps(records))
    assert len(result) == len(expected)
    for res_item, exp_item in zip(result, expected):
        res_item["decimal"] = Decimal(res_item["decimal"])
        exp_item["decimal"] = Decimal(exp_item["decimal"])
        assert res_item == exp_item


@pytest.mark.parametrize("item_type", ["arrow-table", "arrow-batch"])
def test_add_map(item_type: TPythonTableFormat):
    item, _, _ = arrow_table_all_data_types(item_type, num_rows=200)

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


@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_extract_normalize_file_rotation(item_type: TPythonTableFormat) -> None:
    # do not extract state
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # use parquet for dummy
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item, rows, _ = arrow_table_all_data_types(item_type)

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


@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_arrow_clashing_names(item_type: TPythonTableFormat) -> None:
    # # use parquet for dummy
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"
    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")

    item, _, _ = arrow_table_all_data_types(item_type, include_name_clash=True)

    @dlt.resource
    def data_frames():
        for _ in range(10):
            yield item

    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.extract(data_frames())
    assert isinstance(py_ex.value.__context__, NameNormalizationCollision)


@pytest.mark.parametrize("item_type", ["arrow-table", "arrow-batch"])
def test_load_arrow_vary_schema(item_type: TPythonTableFormat) -> None:
    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

    item, _, _ = arrow_table_all_data_types(item_type, include_not_normalized_name=False)
    pipeline.run(item, table_name="data")

    item, _, _ = arrow_table_all_data_types(item_type, include_not_normalized_name=False)
    # remove int column
    try:
        item = item.drop("int")
    except AttributeError:
        names = item.schema.names
        names.remove("int")
        item = item.select(names)
    pipeline.run(item, table_name="data")


@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_arrow_as_data_loading(item_type: TPythonTableFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    item, rows, _ = arrow_table_all_data_types(item_type)

    item_resource = dlt.resource(item, name="item")
    assert id(item) == id(list(item_resource)[0])

    pipeline_name = "arrow_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    pipeline.extract(item, table_name="items")
    assert len(pipeline.list_extracted_resources()) == 1
    info = pipeline.normalize()
    assert info.row_counts["items"] == len(rows)


@pytest.mark.parametrize("item_type", ["arrow-table"])  # , "pandas", "arrow-batch"
def test_normalize_with_dlt_columns(item_type: TPythonTableFormat):
    item, records, _ = arrow_table_all_data_types(item_type, num_rows=5432)
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

    pipeline.load()

    # should be able to load again
    pipeline.run(some_data())

    # should be able to load arrow without a column
    try:
        item = item.drop("int")
    except AttributeError:
        names = item.schema.names
        names.remove("int")
        item = item.select(names)
    pipeline.run(item, table_name="some_data")

    # should be able to load arrow with a new column
    item, records, _ = arrow_table_all_data_types(item_type, num_rows=200)
    item = item.append_column("static_int", [[0] * 200])
    pipeline.run(item, table_name="some_data")

    schema = pipeline.default_schema
    assert schema.tables["some_data"]["columns"]["static_int"]["data_type"] == "bigint"


@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_normalize_reorder_columns_separate_packages(item_type: TPythonTableFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    table, shuffled_table, shuffled_removed_column = prepare_shuffled_tables()

    def _to_item(table: Any) -> Any:
        return arrow_item_from_table(table, item_type)

    pipeline_name = "arrow_" + uniq_id()
    # all arrows will be written to the same table in the destination
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
    storage = pipeline._get_normalize_storage()
    extract_info = pipeline.extract(_to_item(shuffled_removed_column), table_name="table")
    job_file = extract_info.load_packages[0].jobs["new_jobs"][0].file_path
    with storage.extracted_packages.storage.open_file(job_file, "rb") as f:
        actual_tbl_no_binary = pa.parquet.read_table(f)
        # schema must be same
        assert actual_tbl_no_binary.schema.names == shuffled_removed_column.schema.names
        assert actual_tbl_no_binary.schema.equals(shuffled_removed_column.schema)
    # print(pipeline.default_schema.to_pretty_yaml())

    extract_info = pipeline.extract(_to_item(shuffled_table), table_name="table")
    job_file = extract_info.load_packages[0].jobs["new_jobs"][0].file_path
    with storage.extracted_packages.storage.open_file(job_file, "rb") as f:
        actual_tbl_shuffled = pa.parquet.read_table(f)
        # shuffled has additional "binary column" which must be added at the end
        shuffled_names = list(shuffled_table.schema.names)
        shuffled_names.remove("binary")
        shuffled_names.append("binary")
        assert actual_tbl_shuffled.schema.names == shuffled_names

    extract_info = pipeline.extract(_to_item(table), table_name="table")
    job_file = extract_info.load_packages[0].jobs["new_jobs"][0].file_path
    with storage.extracted_packages.storage.open_file(job_file, "rb") as f:
        actual_tbl = pa.parquet.read_table(f)
        # orig table must be ordered exactly as shuffled table
        assert actual_tbl.schema.names == shuffled_names
        assert actual_tbl.schema.equals(actual_tbl_shuffled.schema)

    # now normalize everything to parquet
    normalize_info = pipeline.normalize(loader_file_format="parquet")
    print(normalize_info.asstr(verbosity=2))
    # we should have 3 load packages
    assert len(normalize_info.load_packages) == 3
    assert normalize_info.row_counts["table"] == 5432 * 3

    # load to duckdb
    pipeline.load()


@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_normalize_reorder_columns_single_package(item_type: TPythonTableFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # we do not want to rotate buffer
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "100000"
    table, shuffled_table, shuffled_removed_column = prepare_shuffled_tables()

    def _to_item(table: Any) -> Any:
        return arrow_item_from_table(table, item_type)

    pipeline_name = "arrow_" + uniq_id()
    # all arrows will be written to the same table in the destination
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

    # extract arrows one by one
    extract_info = pipeline.extract(
        [_to_item(shuffled_removed_column), _to_item(shuffled_table), _to_item(table)],
        table_name="table",
    )
    assert len(extract_info.load_packages) == 1
    # there was a schema change (binary column was added)
    assert len(extract_info.load_packages[0].jobs["new_jobs"]) == 2

    normalize_info = pipeline.normalize(loader_file_format="parquet")
    assert len(normalize_info.load_packages) == 1
    assert normalize_info.row_counts["table"] == 5432 * 3
    # we have 2 jobs: one was imported and second one had to be normalized
    assert len(normalize_info.load_packages[0].jobs["new_jobs"]) == 2
    load_storage = pipeline._get_load_storage()
    for new_job in normalize_info.load_packages[0].jobs["new_jobs"]:
        # all jobs must have the destination schemas
        with load_storage.normalized_packages.storage.open_file(new_job.file_path, "rb") as f:
            actual_tbl = pa.parquet.read_table(f)
            shuffled_names = list(shuffled_table.schema.names)
            # binary must be at the end
            shuffled_names.remove("binary")
            shuffled_names.append("binary")
            assert actual_tbl.schema.names == shuffled_names

    pipeline.load()


@pytest.mark.parametrize("item_type", ["arrow-table", "pandas", "arrow-batch"])
def test_normalize_reorder_columns_single_batch(item_type: TPythonTableFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # we do not want to rotate buffer
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "100000"
    table, shuffled_table, shuffled_removed_column = prepare_shuffled_tables()

    def _to_item(table: Any) -> Any:
        return arrow_item_from_table(table, item_type)

    pipeline_name = "arrow_" + uniq_id()
    # all arrows will be written to the same table in the destination
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

    # extract arrows in a single batch. this should unify the schema and generate just a single file
    # that can be directly imported
    extract_info = pipeline.extract(
        [[_to_item(shuffled_removed_column), _to_item(shuffled_table), _to_item(table)]],
        table_name="table",
    )
    assert len(extract_info.load_packages) == 1
    # all arrow tables got normalized to the same schema so no rotation
    assert len(extract_info.load_packages[0].jobs["new_jobs"]) == 1

    shuffled_names = list(shuffled_table.schema.names)
    # binary must be at the end
    shuffled_names.remove("binary")
    shuffled_names.append("binary")

    storage = pipeline._get_normalize_storage()
    job_file = extract_info.load_packages[0].jobs["new_jobs"][0].file_path
    with storage.extracted_packages.storage.open_file(job_file, "rb") as f:
        actual_tbl = pa.parquet.read_table(f)
        # must be exactly shuffled_schema like in all other cases
        assert actual_tbl.schema.names == shuffled_names

    normalize_info = pipeline.normalize(loader_file_format="parquet")
    assert len(normalize_info.load_packages) == 1
    assert normalize_info.row_counts["table"] == 5432 * 3
    # one job below that was imported without normalization
    assert len(normalize_info.load_packages[0].jobs["new_jobs"]) == 1
    load_storage = pipeline._get_load_storage()
    for new_job in normalize_info.load_packages[0].jobs["new_jobs"]:
        # all jobs must have the destination schemas
        with load_storage.normalized_packages.storage.open_file(new_job.file_path, "rb") as f:
            actual_tbl = pa.parquet.read_table(f)
            assert len(actual_tbl) == 5432 * 3
            assert actual_tbl.schema.names == shuffled_names

    pipeline.load()


@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_empty_arrow(item_type: TPythonTableFormat) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    # always return pandas
    item, _, _ = arrow_table_all_data_types("pandas", num_rows=1)
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
        arrow_item_from_pandas(empty_df, item_type), name="items", write_disposition="replace"
    )
    info = pipeline.extract(item_resource)
    load_id = info.loads_ids[0]
    assert info.metrics[load_id][0]["table_metrics"]["items"].items_count == 0
    assert len(pipeline.list_extracted_resources()) == 1
    norm_info = pipeline.normalize()
    assert norm_info.row_counts["items"] == 0


def test_import_file_with_arrow_schema() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_jsonl_import",
        destination="duckdb",
        dev_mode=True,
    )

    # Define the schema based on the CSV input
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("description", pa.string()),
            ("ordered_at", pa.date32()),
            ("price", pa.float64()),
        ]
    )

    # Create empty arrays for each field
    empty_arrays = [
        pa.array([], type=pa.int64()),
        pa.array([], type=pa.string()),
        pa.array([], type=pa.string()),
        pa.array([], type=pa.date32()),
        pa.array([], type=pa.float64()),
    ]

    # Create an empty table with the defined schema
    empty_table = pa.Table.from_arrays(empty_arrays, schema=schema)

    # columns should be created from empty table
    import_file = "tests/load/cases/loading/header.jsonl"
    pipeline.run(
        [dlt.mark.with_file_import(import_file, "jsonl", 2, hints=empty_table)],
        table_name="no_header",
    )

    assert_only_table_columns(pipeline, "no_header", schema.names)
    rows = load_tables_to_dicts(pipeline, "no_header")
    assert len(rows["no_header"]) == 2


@pytest.mark.parametrize("item_type", ["pandas", "arrow-table", "arrow-batch"])
def test_extract_adds_dlt_load_id(item_type: TPythonTableFormat) -> None:
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"
    os.environ["DESTINATION__LOADER_FILE_FORMAT"] = "parquet"

    item, _, _ = arrow_table_all_data_types(item_type, num_rows=5432)

    @dlt.resource
    def some_data():
        yield item

    pipeline: dlt.Pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="duckdb")
    info = pipeline.extract(some_data())

    load_id = info.loads_ids[0]
    jobs = info.load_packages[0].jobs["new_jobs"]
    extracted_file = [job for job in jobs if "some_data" in job.file_path][0].file_path

    with pa.parquet.ParquetFile(extracted_file) as pq:
        tbl = pq.read()
        assert len(tbl) == 5432

        # Extracted file has _dlt_load_id
        assert pa.compute.all(pa.compute.equal(tbl["_dlt_load_id"], load_id)).as_py()

        # Load ID in both schema and arrow tbl should be the last column
        assert tbl.schema.names[-1] == "_dlt_load_id"
        cols = list(pipeline.default_schema.tables["some_data"]["columns"])
        assert cols[-1] == "_dlt_load_id"


def test_extract_json_normalize_parquet_adds_dlt_load_id():
    """Extract jsonl data that gets written to parquet in normalizer. Check that _dlt_load_id is added."""
    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "True"

    rows, _, _ = arrow_table_all_data_types("object", num_rows=1001)

    @dlt.resource
    def some_data():
        yield rows

    pipeline: dlt.Pipeline = dlt.pipeline("arrow_" + uniq_id(), destination="duckdb")

    pipeline.extract(some_data())
    n_info = pipeline.normalize(loader_file_format="parquet")

    load_id = n_info.loads_ids[0]
    jobs = n_info.load_packages[0].jobs["new_jobs"]
    normalized_file = [job for job in jobs if "some_data" in job.file_path][0].file_path

    with pa.parquet.ParquetFile(normalized_file) as pq:
        tbl = pq.read()
        assert len(tbl) == 1001

        # Normalized file has _dlt_load_id
        assert pa.compute.all(pa.compute.equal(tbl["_dlt_load_id"], load_id)).as_py()
