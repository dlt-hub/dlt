import os
import posixpath

from typing import Tuple, Union, Dict
from urllib.parse import urlparse
from fsspec import AbstractFileSystem, get_filesystem_class, register_implementation
from fsspec.core import filesystem as fs_filesystem
import pytest

from tenacity import retry, stop_after_attempt, wait_fixed

from dlt.common import logger
from dlt.common import json, pendulum
from dlt.common.configuration import resolve
from dlt.common.configuration.inject import with_config
from dlt.common.configuration.specs import AnyAzureCredentials
from dlt.common.storages import fsspec_from_config, FilesystemConfiguration
from dlt.common.storages.configuration import make_fsspec_url
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH, glob_files
from dlt.common.utils import custom_environ, uniq_id
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.configuration import (
    FilesystemDestinationClientConfiguration,
)
from dlt.destinations.impl.filesystem.typing import TExtraPlaceholders

from tests.common.configuration.utils import environment
from tests.common.storages.utils import TEST_SAMPLE_FILES, assert_sample_files
from tests.load.utils import ALL_FILESYSTEM_DRIVERS, AWS_BUCKET, WITH_GDRIVE_BUCKETS
from tests.utils import autouse_test_storage
from tests.load.filesystem.utils import self_signed_cert


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@with_config(spec=FilesystemConfiguration, sections=("destination", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def test_filesystem_configuration() -> None:
    config = FilesystemConfiguration(bucket_url="az://root")
    assert config.protocol == "az"
    # print(config.resolve_credentials_type())
    assert config.resolve_credentials_type() == AnyAzureCredentials
    assert dict(config) == {
        "read_only": False,
        "bucket_url": "az://root",
        "credentials": None,
        "client_kwargs": None,
        "max_state_files": 100,
        "kwargs": None,
        "deltalake_storage_options": None,
    }


@pytest.mark.parametrize("bucket_url", WITH_GDRIVE_BUCKETS)
def test_remote_url(bucket_url: str) -> None:
    # make absolute urls out of paths
    scheme = urlparse(bucket_url).scheme
    if not scheme:
        scheme = "file"
        bucket_url = FilesystemConfiguration.make_file_url(bucket_url)
    if scheme == "gdrive":
        from dlt.common.storages.fsspecs.google_drive import GoogleDriveFileSystem

        register_implementation("gdrive", GoogleDriveFileSystem, "GoogleDriveFileSystem")

    fs_class = get_filesystem_class(scheme)
    fs_path = fs_class._strip_protocol(bucket_url)
    # reconstitute url
    assert make_fsspec_url(scheme, fs_path, bucket_url) == bucket_url


def test_filesystem_instance(with_gdrive_buckets_env: str) -> None:
    @retry(stop=stop_after_attempt(10), wait=wait_fixed(1), reraise=True)
    def check_file_exists(filedir_: str, file_url_: str):
        try:
            files = filesystem.ls(filedir_, detail=True)
            details = next(d for d in files if d["name"] == file_url_)
            assert details["size"] == 10
        except Exception as ex:
            print(ex)
            raise

    def check_file_changed(file_url_: str):
        details = filesystem.info(file_url_)
        assert details["size"] == 11
        assert (MTIME_DISPATCH[config.protocol](details) - now).seconds < 160

    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
    config = get_config()
    # we do not add protocol to bucket_url (we need relative path)
    assert bucket_url.startswith(config.protocol) or config.is_local_filesystem
    filesystem, url = fsspec_from_config(config)
    # do a few file ops
    now = pendulum.now()
    filename = f"filesystem_common_{uniq_id()}"
    file_dir = posixpath.join(url, f"filesystem_common_dir_{uniq_id()}")
    file_url = posixpath.join(file_dir, filename)
    try:
        filesystem.mkdir(file_dir, create_parents=False)
        filesystem.pipe(file_url, b"test bytes")
        check_file_exists(file_dir, file_url)
        filesystem.pipe(file_url, b"test bytes2")
        check_file_changed(file_url)
    finally:
        filesystem.rm(file_url)
        # s3 does not create folder with mkdir
        if config.protocol != "s3":
            filesystem.rmdir(file_dir)
        assert not filesystem.exists(file_url)
        with pytest.raises(FileNotFoundError):
            filesystem.info(file_url)


@pytest.mark.parametrize("load_content", (True, False))
@pytest.mark.parametrize("glob_filter", ("**", "**/*.csv", "*.txt", "met_csv/A803/*.csv"))
def test_glob_files(with_gdrive_buckets_env: str, load_content: bool, glob_filter: str) -> None:
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
    bucket_url, config, filesystem = glob_test_setup(bucket_url, "standard_source/samples")
    # use glob to get data
    all_file_items = list(glob_files(filesystem, bucket_url, glob_filter))
    # assert len(all_file_items) == 0
    assert_sample_files(all_file_items, filesystem, config, load_content, glob_filter)


def test_glob_overlapping_path_files(with_gdrive_buckets_env: str) -> None:
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]
    # "standard_source/sample" overlaps with a real existing "standard_source/samples". walk operation on azure
    # will return all files from "standard_source/samples" and report the wrong "standard_source/sample" path to the user
    # here we test we do not have this problem with out glob
    bucket_url, config, filesystem = glob_test_setup(bucket_url, "standard_source/sample")
    if config.protocol in ["file"]:
        pytest.skip(f"{config.protocol} not supported in this test")
    # use glob to get data
    all_file_items = list(glob_files(filesystem, bucket_url))
    assert len(all_file_items) == 0


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


def test_filesystem_configuration_with_additional_arguments() -> None:
    config = FilesystemConfiguration(
        bucket_url="az://root",
        kwargs={"use_ssl": True},
        client_kwargs={"verify": "public.crt"},
        deltalake_storage_options={"AWS_S3_LOCKING_PROVIDER": "dynamodb"},
    )
    assert dict(config) == {
        "read_only": False,
        "bucket_url": "az://root",
        "credentials": None,
        "max_state_files": 100,
        "kwargs": {"use_ssl": True},
        "client_kwargs": {"verify": "public.crt"},
        "deltalake_storage_options": {"AWS_S3_LOCKING_PROVIDER": "dynamodb"},
    }


@pytest.mark.skipif("s3" not in ALL_FILESYSTEM_DRIVERS, reason="s3 destination not configured")
def test_kwargs_propagate_to_s3_instance(default_buckets_env: str) -> None:
    os.environ["DESTINATION__FILESYSTEM__KWARGS"] = '{"use_ssl": false}'
    os.environ["DESTINATION__FILESYSTEM__CLIENT_KWARGS"] = '{"verify": false, "foo": "bar"}'

    config = get_config()

    if config.protocol != "s3":
        pytest.skip(f"Not configured to use {config.protocol} protocol.")

    filesystem, _ = fsspec_from_config(config)

    assert hasattr(filesystem, "kwargs")
    assert hasattr(filesystem, "client_kwargs")
    assert not filesystem.use_ssl
    assert ("verify", False) in filesystem.client_kwargs.items()
    assert ("foo", "bar") in filesystem.client_kwargs.items()


@pytest.mark.skipif("s3" not in ALL_FILESYSTEM_DRIVERS, reason="s3 destination not configured")
def test_s3_wrong_client_certificate(default_buckets_env: str, self_signed_cert: str) -> None:
    """Test whether filesystem raises an SSLError when trying to establish
    a connection with the wrong client certificate."""
    os.environ["DESTINATION__FILESYSTEM__KWARGS"] = '{"use_ssl": true}'
    os.environ["DESTINATION__FILESYSTEM__CLIENT_KWARGS"] = f'{{"verify": "{self_signed_cert}"}}'

    config = get_config()

    if config.protocol != "s3":
        pytest.skip(f"Not configured to use {config.protocol} protocol.")

    filesystem, _ = fsspec_from_config(config)

    from botocore.exceptions import SSLError

    with pytest.raises(SSLError, match="SSL: CERTIFICATE_VERIFY_FAILED"):
        print(filesystem.ls("", detail=False))


def test_filesystem_destination_config_reports_unused_placeholders(mocker) -> None:
    with custom_environ({"DATASET_NAME": "BOBO"}):
        extra_placeholders: TExtraPlaceholders = {
            "value": 1,
            "otters": "lab",
            "dlt": "labs",
            "dlthub": "platform",
            "x": "files",
        }
        logger_spy = mocker.spy(logger, "info")
        resolve.resolve_configuration(
            FilesystemDestinationClientConfiguration(
                bucket_url="file:///tmp/dirbobo",
                layout="{schema_name}/{table_name}/{otters}-x-{x}/{load_id}.{file_id}.{timestamp}.{ext}",
                extra_placeholders=extra_placeholders,
            )
        )
        logger_spy.assert_called_once_with("Found unused layout placeholders: value, dlt, dlthub")


def test_filesystem_destination_passed_parameters_override_config_values() -> None:
    config_current_datetime = "2024-04-11T00:00:00Z"
    config_extra_placeholders = {"placeholder_x": "x", "placeholder_y": "y"}
    with custom_environ(
        {
            "DESTINATION__FILESYSTEM__BUCKET_URL": "file:///tmp/dirbobo",
            "DESTINATION__FILESYSTEM__CURRENT_DATETIME": config_current_datetime,
            "DESTINATION__FILESYSTEM__EXTRA_PLACEHOLDERS": json.dumps(config_extra_placeholders),
        }
    ):
        extra_placeholders: TExtraPlaceholders = {
            "new_value": 1,
            "dlt": "labs",
            "dlthub": "platform",
        }
        now = pendulum.now()
        config_now = pendulum.parse(config_current_datetime)

        # Check with custom datetime and extra placeholders
        # both should override config values
        filesystem_destination = filesystem(
            extra_placeholders=extra_placeholders, current_datetime=now
        )
        filesystem_config = FilesystemDestinationClientConfiguration()._bind_dataset_name(
            dataset_name="dummy_dataset"
        )
        bound_config = filesystem_destination.configuration(filesystem_config)
        assert bound_config.current_datetime == now
        assert bound_config.extra_placeholders == extra_placeholders

        # Check only passing one parameter
        filesystem_destination = filesystem(extra_placeholders=extra_placeholders)
        filesystem_config = FilesystemDestinationClientConfiguration()._bind_dataset_name(
            dataset_name="dummy_dataset"
        )
        bound_config = filesystem_destination.configuration(filesystem_config)
        assert bound_config.current_datetime == config_now
        assert bound_config.extra_placeholders == extra_placeholders

        filesystem_destination = filesystem()
        filesystem_config = FilesystemDestinationClientConfiguration()._bind_dataset_name(
            dataset_name="dummy_dataset"
        )
        bound_config = filesystem_destination.configuration(filesystem_config)
        assert bound_config.current_datetime == config_now
        assert bound_config.extra_placeholders == config_extra_placeholders


def glob_test_setup(
    bucket_url: str, glob_folder: str
) -> Tuple[str, FilesystemConfiguration, AbstractFileSystem]:
    config = get_config()
    # enable caches
    config.read_only = True

    # may contain query string
    filesystem, fs_path = fsspec_from_config(config)
    bucket_url = make_fsspec_url(config.protocol, posixpath.join(fs_path, glob_folder), bucket_url)
    if config.protocol == "memory":
        mem_path = os.path.join("/m", "standard_source")
        if not filesystem.isdir(mem_path):
            filesystem.mkdirs(mem_path)
            filesystem.upload(TEST_SAMPLE_FILES, mem_path, recursive=True)
    if config.protocol == "file":
        file_path = os.path.join("_storage", "standard_source")
        if not filesystem.isdir(file_path):
            filesystem.mkdirs(file_path)
            filesystem.upload(TEST_SAMPLE_FILES, file_path, recursive=True)
    return bucket_url, config, filesystem
