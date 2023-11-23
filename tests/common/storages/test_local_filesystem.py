import os
import itertools
import pytest
import pathlib

from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import glob_files

from tests.common.storages.utils import assert_sample_files

TEST_SAMPLE_FILES = "tests/common/storages/samples"


@pytest.mark.parametrize(
    "bucket_url,load_content", itertools.product(["file:///", "/", ""], [True, False])
)
def test_filesystem_dict_local(bucket_url: str, load_content: bool) -> None:
    if bucket_url in [""]:
        # relative paths
        bucket_url = TEST_SAMPLE_FILES
    else:
        if bucket_url == "/":
            bucket_url = os.path.abspath(TEST_SAMPLE_FILES)
        else:
            bucket_url = pathlib.Path(TEST_SAMPLE_FILES).absolute().as_uri()

    config = FilesystemConfiguration(bucket_url=bucket_url)
    filesystem, _ = fsspec_from_config(config)
    # use glob to get data
    try:
        all_file_items = list(glob_files(filesystem, bucket_url))
        assert_sample_files(all_file_items, filesystem, config, load_content)
    except NotImplementedError as ex:
        pytest.skip("Skipping due to " + str(ex))
