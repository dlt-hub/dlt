import os
import itertools
import pytest
import pathlib

from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import (
    FileItemDict,
    glob_files,
    register_implementation_in_fsspec,
)

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


def test_filesystem_decompress() -> None:
    config = FilesystemConfiguration(bucket_url=TEST_SAMPLE_FILES)
    filesystem, _ = fsspec_from_config(config)
    gzip_files = list(glob_files(filesystem, TEST_SAMPLE_FILES, "**/*.gz"))
    assert len(gzip_files) > 0
    for file in gzip_files:
        assert file["encoding"] == "gzip"
        assert file["file_name"].endswith(".gz")
        file_dict = FileItemDict(file, filesystem)
        # read as is (compressed gzip)
        with file_dict.open(compression="disable") as f:
            assert f.read() == file_dict.read_bytes()
        # read as uncompressed text
        with file_dict.open(mode="tr") as f:
            lines = f.readlines()
            assert len(lines) > 1
            assert lines[0].startswith('"1200864931","2015-07-01 00:00:13"')
        # read as uncompressed binary
        with file_dict.open(compression="enable") as f:
            assert f.read().startswith(b'"1200864931","2015-07-01 00:00:13"')


def test_register_implementation_in_fsspec() -> None:
    """Test registering a filesystem implementation with fsspec."""
    # ToDo make test more focused and safe with mock.  Just want a unit test.
    from fsspec.registry import known_implementations, available_protocols, register_implementation

    protocol = "dummyfs"
    previous_registration_existed = False

    # setup
    if protocol in known_implementations:
        backup = known_implementations.pop(protocol)
        previous_registration_existed = True

    assert (
        protocol not in known_implementations
    ), f"As a test precondition, {protocol} should not be registered."

    # do and test
    register_implementation_in_fsspec(protocol)
    assert protocol in available_protocols(), f"{protocol} should be registered."

    # teardown
    if previous_registration_existed:
        register_implementation(protocol, backup, clobber=True)
        assert (
            protocol in available_protocols()
        ), f"After teardown, {protocol} should not be registered, which was the original state."
    else:
        known_implementations.pop(protocol)
        assert (
            protocol not in known_implementations
        ), f"After teardown, {protocol} should not be registered, which was the original state."


def test_register_unsupported_raises() -> None:
    """Test registering an unsupported filesystem implementation with fsspec raises an error."""
    with pytest.raises(ValueError, match=r"Unknown protocol: 'unsupportedfs_t7Y8'"):
        register_implementation_in_fsspec("unsupportedfs_t7Y8")
