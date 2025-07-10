import pytest

from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.load.sources.filesystem.cases import  TESTS_BUCKET_URLS

from dlt.sources.filesystem.copy import copy_files
from dlt.sources.filesystem import filesystem

from tests.load.sources.filesystem.utils import glob_test_setup
from tests.load.sources.filesystem.cases import CSV_GLOB_RESULT


@pytest.mark.parametrize("bucket_url", TESTS_BUCKET_URLS)
@pytest.mark.parametrize(
    "destination_config",
    # NOTE: this setup will only load to local filesystem 
    # (jsonl file format is not used, but if we do not add it, every test will run twice with jsonl and parquet set)
    destinations_configs(local_filesystem_configs=True, with_file_format="jsonl"),
    ids=lambda x: x.name,
)
def test_copy_to_local(bucket_url: str, destination_config: DestinationTestConfiguration) -> None:

    glob_result = CSV_GLOB_RESULT
    p = destination_config.setup_pipeline("test_copy_to_local", dev_mode=True)
    copy_files(filesystem(bucket_url, file_glob=glob_result["glob"]).with_name("my_table"), p)

    # check all expected csv files are there
    all_files = p._fs_client().list_table_files("my_table")

    # we have the same amount of files
    assert len(all_files) == 7
    assert len(all_files) == len(glob_result["relative_paths"])

    # check that each local file is found unmodified in the destination
    source_files = {
        f["file_content"]
        for f in list(filesystem(bucket_url, file_glob=glob_result["glob"], extract_content=True))
    }
    destination_files = {p._fs_client().read_bytes(f) for f in all_files}
    assert source_files == destination_files


# TODO
def test_copy_with_incorrect_source_type() -> None:
    pass

# TODO
def test_foward_run_kwargs() -> None:
    pass


# TODO
def test_copy_unknown_file_format() -> None:
    pass

# TODO
def test_prevent_non_filesystem_destination() -> None:
    pass


# TODO
def test_copy_files_from_list() -> None:
    pass