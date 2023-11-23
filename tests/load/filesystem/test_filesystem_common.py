import os
import posixpath
from typing import Union, Dict
import pytest
from dlt.common.configuration.inject import with_config

from dlt.common import pendulum
from dlt.common.configuration.specs import AzureCredentials, AzureCredentialsWithoutDefaults
from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH, glob_files
from dlt.common.utils import uniq_id

from tests.utils import preserve_environ, autouse_test_storage
from tests.common.configuration.utils import environment
from tests.common.storages.utils import assert_sample_files
from tests.load.utils import ALL_FILESYSTEM_DRIVERS


@with_config(spec=FilesystemConfiguration, sections=("destination", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def test_filesystem_configuration() -> None:
    config = FilesystemConfiguration(bucket_url="az://root")
    assert config.protocol == "az"
    # print(config.resolve_credentials_type())
    assert (
        config.resolve_credentials_type()
        == Union[AzureCredentialsWithoutDefaults, AzureCredentials]
    )
    # make sure that only bucket_url and credentials are there
    assert dict(config) == {"bucket_url": "az://root", "credentials": None}


def test_filesystem_instance(all_buckets_env: str) -> None:
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
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
def test_filesystem_dict(default_buckets_env: str, load_content: bool) -> None:
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
    config = get_config()
    if config.protocol in ["memory", "file"]:
        pytest.skip(f"{config.protocol} not supported in this test")
    glob_folder = "standard_source"
    filesystem, _ = fsspec_from_config(config)
    # use glob to get data
    try:
        all_file_items = list(
            glob_files(filesystem, posixpath.join(bucket_url, glob_folder, "samples"))
        )
        assert_sample_files(all_file_items, filesystem, config, load_content)
    except NotImplementedError as ex:
        pytest.skip("Skipping due to " + str(ex))


@pytest.mark.skipif("s3" not in ALL_FILESYSTEM_DRIVERS, reason="s3 destination not configured")
def test_filesystem_instance_from_s3_endpoint(environment: Dict[str, str]) -> None:
    """Test that fsspec instance is correctly configured when using endpoint URL.
    E.g. when using an S3 compatible service such as Cloudflare R2
    """
    from s3fs import S3FileSystem

    environment["DESTINATION__FILESYSTEM__BUCKET_URL"] = "s3://dummy-bucket"
    environment["CREDENTIALS__ENDPOINT_URL"] = "https://fake-s3-endpoint.example.com"
    environment["CREDENTIALS__AWS_ACCESS_KEY_ID"] = "fake-access-key"
    environment["CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = "fake-secret-key"

    config = get_config()

    filesystem, bucket_name = fsspec_from_config(config)

    assert isinstance(filesystem, S3FileSystem)
    assert filesystem.endpoint_url == "https://fake-s3-endpoint.example.com"
    assert bucket_name == "dummy-bucket"
    assert filesystem.key == "fake-access-key"
    assert filesystem.secret == "fake-secret-key"
