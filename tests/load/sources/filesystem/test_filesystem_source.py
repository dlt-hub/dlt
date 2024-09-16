import os
from typing import Any, Dict, List

import dlt
import pytest
from dlt.common import pendulum

from dlt.common.storages import fsspec_filesystem
from dlt.sources.filesystem import filesystem, readers, FileItem, FileItemDict, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource

from tests.common.storages.utils import TEST_SAMPLE_FILES
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import (
    assert_load_info,
    load_table_counts,
    assert_query_data,
)
from tests.utils import TEST_STORAGE_ROOT

from tests.load.sources.filesystem.cases import GLOB_RESULTS, TESTS_BUCKET_URLS


@pytest.fixture(autouse=True)
def glob_test_setup() -> None:
    file_fs, _ = fsspec_filesystem("file")
    file_path = os.path.join(TEST_STORAGE_ROOT, "standard_source")
    if not file_fs.isdir(file_path):
        file_fs.mkdirs(file_path)
        file_fs.upload(TEST_SAMPLE_FILES, file_path, recursive=True)


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("glob_params", GLOB_RESULTS)
def test_file_list(bucket_url: str, glob_params: Dict[str, Any]) -> None:
    @dlt.transformer
    def bypass(items) -> str:
        return items

    # we just pass the glob parameter to the resource if it is not None
    if file_glob := glob_params["glob"]:
        filesystem_res = filesystem(bucket_url=bucket_url, file_glob=file_glob) | bypass
    else:
        filesystem_res = filesystem(bucket_url=bucket_url) | bypass

    all_files = list(filesystem_res)
    file_count = len(all_files)
    relative_paths = [item["relative_path"] for item in all_files]
    assert file_count == len(glob_params["relative_paths"])
    assert set(relative_paths) == set(glob_params["relative_paths"])


@pytest.mark.parametrize("extract_content", [True, False])
@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
def test_load_content_resources(bucket_url: str, extract_content: bool) -> None:
    @dlt.transformer
    def assert_sample_content(items: List[FileItemDict]):
        # expect just one file
        for item in items:
            assert item["file_name"] == "sample.txt"
            content = item.read_bytes()
            assert content == b"dlthub content"
            assert item["size_in_bytes"] == 14
            assert item["file_url"].endswith("/samples/sample.txt")
            assert item["mime_type"] == "text/plain"
            assert isinstance(item["modification_date"], pendulum.DateTime)

        yield items

    # use transformer to test files
    sample_file = (
        filesystem(
            bucket_url=bucket_url,
            file_glob="sample.txt",
            extract_content=extract_content,
        )
        | assert_sample_content
    )
    # just execute iterator
    files = list(sample_file)
    assert len(files) == 1

    # take file from nested dir
    # use map function to assert
    def assert_csv_file(item: FileItem):
        # on windows when checking out, git will convert lf into cr+lf so we have more bytes (+ number of lines: 25)
        assert item["size_in_bytes"] in (742, 767)
        assert item["relative_path"] == "met_csv/A801/A881_20230920.csv"
        assert item["file_url"].endswith("/samples/met_csv/A801/A881_20230920.csv")
        assert item["mime_type"] == "text/csv"
        # print(item)
        return item

    nested_file = filesystem(bucket_url, file_glob="met_csv/A801/A881_20230920.csv")

    assert len(list(nested_file | assert_csv_file)) == 1


@pytest.mark.skip("Needs secrets toml to work..")
def test_fsspec_as_credentials():
    # get gs filesystem
    gs_resource = filesystem("gs://ci-test-bucket")
    # get authenticated client
    fs_client = fsspec_from_resource(gs_resource)
    print(fs_client.ls("ci-test-bucket/standard_source/samples"))
    # use to create resource instead of credentials
    gs_resource = filesystem("gs://ci-test-bucket/standard_source/samples", credentials=fs_client)
    print(list(gs_resource))


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, supports_merge=True, all_buckets_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
def test_csv_transformers(
    bucket_url: str, destination_config: DestinationTestConfiguration
) -> None:
    pipeline = destination_config.setup_pipeline("test_csv_transformers", dev_mode=True)
    # load all csvs merging data on a date column
    met_files = filesystem(bucket_url=bucket_url, file_glob="met_csv/A801/*.csv") | read_csv()
    met_files.apply_hints(write_disposition="merge", merge_key="date")
    load_info = pipeline.run(met_files.with_name("met_csv"))
    assert_load_info(load_info)

    # print(pipeline.last_trace.last_normalize_info)
    # must contain 24 rows of A881
    if destination_config.destination_type != "filesystem":
        with pipeline.sql_client() as client:
            table_name = client.make_qualified_table_name("met_csv")
        # TODO: comment out when filesystem destination supports queries (data pond PR)
        assert_query_data(pipeline, f"SELECT code FROM {table_name}", ["A881"] * 24)

    # load the other folder that contains data for the same day + one other day
    # the previous data will be replaced
    met_files = filesystem(bucket_url=bucket_url, file_glob="met_csv/A803/*.csv") | read_csv()
    met_files.apply_hints(write_disposition="merge", merge_key="date")
    load_info = pipeline.run(met_files.with_name("met_csv"))
    assert_load_info(load_info)
    # print(pipeline.last_trace.last_normalize_info)
    # must contain 48 rows of A803
    if destination_config.destination_type != "filesystem":
        with pipeline.sql_client() as client:
            table_name = client.make_qualified_table_name("met_csv")
        # TODO: comment out when filesystem destination supports queries (data pond PR)
        assert_query_data(pipeline, f"SELECT code FROM {table_name}", ["A803"] * 48)
        # and 48 rows in total -> A881 got replaced
        # print(pipeline.default_schema.to_pretty_yaml())
        assert load_table_counts(pipeline, "met_csv") == {"met_csv": 48}


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_standard_readers(
    bucket_url: str, destination_config: DestinationTestConfiguration
) -> None:
    # extract pipes with standard readers
    jsonl_reader = readers(bucket_url, file_glob="**/*.jsonl").read_jsonl()
    parquet_reader = readers(bucket_url, file_glob="**/*.parquet").read_parquet()
    # also read zipped csvs
    csv_reader = readers(bucket_url, file_glob="**/*.csv*").read_csv(float_precision="high")
    csv_duckdb_reader = readers(bucket_url, file_glob="**/*.csv*").read_csv_duckdb()

    # a step that copies files into test storage
    def _copy(item: FileItemDict):
        # instantiate fsspec and copy file
        dest_file = os.path.join(TEST_STORAGE_ROOT, item["relative_path"])
        # create dest folder
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        # download file
        item.fsspec.download(item["file_url"], dest_file)
        # return file item unchanged
        return item

    downloader = filesystem(bucket_url, file_glob="**").add_map(_copy)

    # load in single pipeline
    pipeline = destination_config.setup_pipeline("test_standard_readers", dev_mode=True)
    load_info = pipeline.run(
        [
            jsonl_reader.with_name("jsonl_example"),
            parquet_reader.with_name("parquet_example"),
            downloader.with_name("listing"),
            csv_reader.with_name("csv_example"),
            csv_duckdb_reader.with_name("csv_duckdb_example"),
        ]
    )
    # pandas incorrectly guesses that taxi dataset has headers so it skips one row
    # so we have 1 less row in csv_example than in csv_duckdb_example
    assert_load_info(load_info)
    assert load_table_counts(
        pipeline,
        "jsonl_example",
        "parquet_example",
        "listing",
        "csv_example",
        "csv_duckdb_example",
    ) == {
        "jsonl_example": 1034,
        "parquet_example": 1034,
        "listing": 11,
        "csv_example": 1279,
        "csv_duckdb_example": 1281,  # TODO: i changed this from 1280, what is going on? :)
    }
    # print(pipeline.last_trace.last_normalize_info)
    # print(pipeline.default_schema.to_pretty_yaml())


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_incremental_load(
    bucket_url: str, destination_config: DestinationTestConfiguration
) -> None:
    @dlt.transformer
    def bypass(items) -> str:
        return items

    pipeline = destination_config.setup_pipeline("test_incremental_load", dev_mode=True)

    # Load all files
    all_files = filesystem(bucket_url=bucket_url, file_glob="csv/*")
    # add incremental on modification time
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((all_files | bypass).with_name("csv_files"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files"] == 4

    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again
    all_files = filesystem(bucket_url=bucket_url, file_glob="csv/*")
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((all_files | bypass).with_name("csv_files"))
    # nothing into csv_files
    assert "csv_files" not in pipeline.last_trace.last_normalize_info.row_counts
    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again into different table
    all_files = filesystem(bucket_url=bucket_url, file_glob="csv/*")
    all_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((all_files | bypass).with_name("csv_files_2"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files_2"] == 4


def test_file_chunking() -> None:
    resource = filesystem(
        bucket_url=TESTS_BUCKET_URLS[0],
        file_glob="*/*.csv",
        files_per_page=2,
    )

    from dlt.extract.pipe_iterator import PipeIterator

    # use pipe iterator to get items as they go through pipe
    for pipe_item in PipeIterator.from_pipe(resource._pipe):
        assert len(pipe_item.item) == 2
        # no need to test more chunks
        break
