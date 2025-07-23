import os
import pytest
from tests.utils import TEST_STORAGE_ROOT
from dlt.common.storages import fsspec_filesystem
from tests.common.storages.utils import TEST_SAMPLE_FILES


# upload test files to local filesystem
@pytest.fixture(autouse=True)
def glob_test_setup() -> None:
    file_fs, _ = fsspec_filesystem("file")
    file_path = os.path.join(TEST_STORAGE_ROOT, "data", "standard_source")
    if not file_fs.isdir(file_path):
        file_fs.mkdirs(file_path)
        file_fs.upload(TEST_SAMPLE_FILES, file_path, recursive=True)
