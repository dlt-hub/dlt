import os
from typing import Any, Dict, List

from fsspec import AbstractFileSystem

import dlt
import pytest
from dlt.common import pendulum

from dlt.common.storages import fsspec_filesystem
from dlt.common.typing import TSortOrder
from dlt.extract.resource import DltResource
from dlt.sources.filesystem import filesystem, readers, FileItem, FileItemDict, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource

from tests.common.storages.utils import TEST_SAMPLE_FILES
from tests.common.configuration.utils import environment
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import (
    assert_load_info,
    load_table_counts,
    assert_query_column,
)
from tests.utils import TEST_STORAGE_ROOT, public_http_server
from tests.load.sources.filesystem.cases import GLOB_RESULTS, TESTS_BUCKET_URLS


@pytest.fixture(autouse=True)
def glob_test_setup() -> None:
    file_fs, _ = fsspec_filesystem("file")
    file_path = os.path.join(TEST_STORAGE_ROOT, "data", "standard_source")
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


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("skip_instance_cache", (True, False))
def test_fsspec_as_credentials(
    bucket_url: str, skip_instance_cache: bool, environment: Dict[str, str]
) -> None:
    if bucket_url.startswith(("sftp", "gdrive")):
        pytest.skip(f"We can't test {bucket_url} here")
    # enable or disable instance cache
    environment["SOURCES__FILESYSTEM__KWARGS"] = (
        '{"skip_instance_cache": %s}' % str(skip_instance_cache).lower()
    )

    def _assert_cached(fsspec_: AbstractFileSystem) -> None:
        # check if token (identify fspec instance) is in cache
        if skip_instance_cache:
            assert fsspec_._fs_token not in fsspec_._cache
        else:
            assert fsspec_._fs_token in fsspec_._cache

    gs_resource = filesystem(bucket_url)
    item: FileItemDict = None
    for item in gs_resource:
        _assert_cached(item.fsspec)
        file_url = item["file_url"]
        break
    assert item

    # get authenticated client
    fs_client = fsspec_from_resource(gs_resource)
    print(fs_client.ls(file_url))
    _assert_cached(fs_client)

    # use to create resource instead of credentials
    gs_resource = filesystem(bucket_url, credentials=fs_client)
    for item in gs_resource:
        _assert_cached(item.fsspec)
        break


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
        assert_query_column(pipeline, f"SELECT code FROM {table_name}", ["A881"] * 24)

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
        assert_query_column(pipeline, f"SELECT code FROM {table_name}", ["A803"] * 48)
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
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("incremental_method", ("signature", "apply"))
@pytest.mark.parametrize("row_order", ("asc", "desc", None))
def test_incremental_load(
    bucket_url: str,
    destination_config: DestinationTestConfiguration,
    incremental_method: str,
    row_order: TSortOrder,
) -> None:
    @dlt.transformer
    def bypass(items) -> str:
        return items

    pipeline = destination_config.setup_pipeline("test_incremental_load", dev_mode=True)

    def _get_files() -> DltResource:
        incremental_ = dlt.sources.incremental[pendulum.DateTime](
            "modification_date", row_order=row_order
        )
        if incremental_method == "signature":
            fs_ = filesystem(bucket_url=bucket_url, file_glob="csv/*", incremental=incremental_)
        else:
            fs_ = filesystem(bucket_url=bucket_url, file_glob="csv/*")
            # add incremental on modification time
            fs_.apply_hints(incremental=incremental_)
        return fs_

    # Load all files
    all_files = _get_files()
    load_info = pipeline.run((all_files | bypass).with_name("csv_files"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files"] == 4

    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again
    all_files = _get_files()
    load_info = pipeline.run((all_files | bypass).with_name("csv_files"))
    # nothing into csv_files
    assert "csv_files" not in pipeline.last_trace.last_normalize_info.row_counts
    table_counts = load_table_counts(pipeline, "csv_files")
    assert table_counts["csv_files"] == 4

    # load again into different table
    all_files = _get_files()
    load_info = pipeline.run((all_files | bypass).with_name("csv_files_2"))
    assert_load_info(load_info)
    assert pipeline.last_trace.last_normalize_info.row_counts["csv_files_2"] == 4


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize("incremental_method", ("signature", "apply"))
@pytest.mark.parametrize("row_order", ("asc", "desc"))
def test_incremental_order(bucket_url: str, incremental_method: str, row_order: TSortOrder) -> None:
    pipeline = dlt.pipeline("test_incremental_order", destination="duckdb", dev_mode=True)

    def _get_files() -> dlt.sources.DltResource:
        incremental_ = dlt.sources.incremental[pendulum.DateTime](
            "file_name", row_order="asc", last_value_func=min if row_order == "desc" else max
        )  # max if row_order=="asc" else min)
        if incremental_method == "signature":
            fs_ = filesystem(
                bucket_url=bucket_url, file_glob="csv/*", incremental=incremental_, files_per_page=1
            )
        else:
            fs_ = filesystem(bucket_url=bucket_url, file_glob="csv/*", files_per_page=1)
            # add incremental on modification time
            fs_.apply_hints(incremental=incremental_)
        return fs_

    # all files sorted by name
    all_files = [file["file_name"] for file in filesystem(bucket_url=bucket_url, file_glob="csv/*")]
    all_files = sorted(all_files, reverse=row_order == "desc")

    # load files with limit until we have no data
    runs = 0
    while not pipeline.run(_get_files().with_name("files").add_limit(1)).is_empty:
        print(pipeline.last_trace.last_normalize_info)
        loaded_files = pipeline.dataset().files["file_name"].fetchall()
        runs += 1
        assert [t_[0] for t_ in loaded_files] == all_files[:runs]
    assert runs == 4


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
def test_partitioned_load(bucket_url: str) -> None:
    # list and sort all csv files for deterministic partitioning
    fs_ = filesystem(bucket_url=bucket_url, file_glob="**/*.csv")
    # we assume that file paths are named so files added later in time come at the end when sorted
    all_files = sorted([file["file_url"] for file in fs_])
    n = len(all_files)
    assert n >= 4

    pipeline = dlt.pipeline("test_partitioned_load", destination="duckdb", dev_mode=True)

    expected_loaded: List[str] = []
    total_loaded = 0

    # load each partition using initial_value and end_value
    for i in range(len(all_files) // 4 + 1):
        files_range = all_files[i * 4 : (i + 1) * 4]
        if not files_range:
            continue

        # close both ranges to load inclusively
        file_name_incremental = dlt.sources.incremental(
            "file_url",
            initial_value=files_range[0],
            end_value=files_range[-1],
            range_start="closed",
            range_end="closed",
        )
        file_resource = filesystem(
            bucket_url=bucket_url, file_glob="**/*.csv", incremental=file_name_incremental
        ).with_name("files")
        load_info = pipeline.run(file_resource)
        assert_load_info(load_info)

        # verify correct number of items loaded in this run
        expected_count = len(files_range)
        assert pipeline.last_trace.last_normalize_info.row_counts["files"] == expected_count
        print(pipeline.last_trace.last_normalize_info.row_counts)

        expected_loaded.extend(files_range)
        total_loaded += expected_count

    # verify that exactly the expected files were loaded overall
    loaded_urls = [t[0] for t in pipeline.dataset().files["file_url"].fetchall()]
    assert len(loaded_urls) == total_loaded
    assert set(loaded_urls) == set(all_files)

    # make sure incremental state is not created
    assert "sources" not in pipeline.state

    # note we could also extract max modification_time and use it for subsequent incremental loading
    file_name_incremental = dlt.sources.incremental(
        "file_url",
        initial_value=all_files[-1],
        range_start="open",
    )
    file_resource = filesystem(
        bucket_url=bucket_url, file_glob="**/*.csv", incremental=file_name_incremental
    ).with_name("files")
    # incremental state got loaded for a first time
    assert not pipeline.run(file_resource).is_empty
    # last item was skipped - nothing more to load
    assert "files" not in pipeline.last_trace.last_normalize_info.row_counts


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
