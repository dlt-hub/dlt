import os
import posixpath
from typing import Union
import pytest
from dlt.common.configuration.inject import with_config

from dlt.common import pendulum
from dlt.common.configuration.specs import AzureCredentials, AzureCredentialsWithoutDefaults
from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH, glob_files
from dlt.common.utils import uniq_id

from tests.utils import preserve_environ, autouse_test_storage
from tests.common.storages.utils import assert_sample_files


@with_config(spec=FilesystemConfiguration, sections=("destination", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def test_filesystem_configuration() -> None:
    config = FilesystemConfiguration(bucket_url="az://root")
    assert config.protocol == "az"
    # print(config.resolve_credentials_type())
    assert config.resolve_credentials_type() == Union[AzureCredentialsWithoutDefaults, AzureCredentials]
    # make sure that only bucket_url and credentials are there
    assert dict(config) == {'bucket_url': 'az://root', 'credentials': None}


def test_filesystem_instance(all_buckets_env: str) -> None:
    bucket_url = os.environ['DESTINATION__FILESYSTEM__BUCKET_URL']
    config = get_config()
    assert bucket_url.startswith(config.protocol)
    filesystem, url = fsspec_from_config(config)
    if config.protocol != "file":
        assert bucket_url.endswith(url)
    # do a few file ops
    now = pendulum.now()
    filename = "filesystem_common_" + uniq_id()
    file_url = posixpath.join(url, filename)
    try:
        filesystem.pipe(file_url, b"test bytes")
        files = filesystem.ls(url, detail=True)
        details = next(d for d in files if d["name"] == file_url)
        # print(details)
        # print(MTIME_DISPATCH[config.protocol](details))
        assert (MTIME_DISPATCH[config.protocol](details) - now).seconds < 60
    finally:
        filesystem.rm(file_url)


@pytest.mark.parametrize("load_content", (True, False))
def test_filesystem_dict(all_buckets_env: str, load_content: bool) -> None:
    bucket_url = os.environ['DESTINATION__FILESYSTEM__BUCKET_URL']
    config = get_config()
    if config.protocol in ["memory", "file"]:
        pytest.skip(f"{config.protocol} not supported in this test")
    glob_folder = "standard_source"
    filesystem, _ = fsspec_from_config(config)
    # use glob to get data
    try:
        all_file_items = list(glob_files(filesystem, posixpath.join(bucket_url, glob_folder, "samples")))
        assert_sample_files(all_file_items, filesystem, config, load_content)
    except NotImplementedError as ex:
        pytest.skip("Skipping due to " + str(ex))
