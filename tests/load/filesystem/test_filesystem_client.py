import posixpath
import os
from unittest import mock
from pathlib import Path
from urllib.parse import urlparse

import pytest

from dlt.common.configuration.specs.azure_credentials import AzureCredentials
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    extract_inner_hint,
)
from dlt.common.schema.schema import Schema
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.utils import digest128, uniq_id
from dlt.common.storages import FileStorage, ParsedLoadJobFileName

from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import (
    FilesystemDestinationClientConfiguration,
    INIT_FILE_NAME,
)

from dlt.destinations.path_utils import create_path, prepare_datetime_params
from tests.load.filesystem.utils import perform_load
from tests.utils import clean_test_storage, init_test_logging
from tests.load.utils import TEST_FILE_LAYOUTS

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl",
]


@pytest.mark.parametrize(
    "url, exp",
    (
        (None, ""),
        ("/path/path2", digest128("")),
        ("s3://cool", digest128("s3://cool")),
        ("s3://cool.domain/path/path2", digest128("s3://cool.domain")),
    ),
)
def test_filesystem_destination_configuration(url, exp) -> None:
    assert FilesystemDestinationClientConfiguration(bucket_url=url).fingerprint() == exp


def test_filesystem_factory_buckets(with_gdrive_buckets_env: str) -> None:
    proto = urlparse(with_gdrive_buckets_env).scheme
    credentials_type = extract_inner_hint(
        FilesystemConfiguration.PROTOCOL_CREDENTIALS.get(proto, CredentialsConfiguration)
    )

    # test factory figuring out the right credentials
    filesystem_ = filesystem(with_gdrive_buckets_env)
    client = filesystem_.client(
        Schema("test"),
        initial_config=FilesystemDestinationClientConfiguration()._bind_dataset_name("test"),
    )
    assert client.config.protocol == proto or "file"
    assert isinstance(client.config.credentials, credentials_type)
    assert issubclass(client.config.credentials_type(client.config), credentials_type)
    assert filesystem_.capabilities()

    # factory gets initial credentials
    filesystem_ = filesystem(with_gdrive_buckets_env, credentials=credentials_type())
    client = filesystem_.client(
        Schema("test"),
        initial_config=FilesystemDestinationClientConfiguration()._bind_dataset_name("test"),
    )
    assert isinstance(client.config.credentials, credentials_type)


@pytest.mark.parametrize("write_disposition", ("replace", "append", "merge"))
@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_successful_load(write_disposition: str, layout: str, with_gdrive_buckets_env: str) -> None:
    """Test load is successful with an empty destination dataset"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    dataset_name = "test_" + uniq_id()
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition=write_disposition,
    ) as load_info:
        client, jobs, _, load_id = load_info
        layout = client.config.layout
        dataset_path = posixpath.join(client.bucket_path, client.config.dataset_name)

        # Assert dataset dir exists
        assert client.fs_client.isdir(dataset_path)

        # Sanity check, there are jobs
        assert jobs
        for job in jobs:
            assert job.state() == "completed"
            job_info = ParsedLoadJobFileName.parse(job.file_name())
            destination_path = posixpath.join(
                dataset_path,
                layout.format(
                    schema_name=client.schema.name,
                    table_name=job_info.table_name,
                    load_id=load_id,
                    file_id=job_info.file_id,
                    ext=job_info.file_format,
                    **prepare_datetime_params(load_package_timestamp=timestamp),
                ),
            )

            # File is created with correct filename and path
            assert client.fs_client.isfile(destination_path)


@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_replace_write_disposition(layout: str, default_buckets_env: str) -> None:
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    # state is typed now
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition="replace",
    ) as load_info:
        client, _, root_path, load_id1 = load_info
        layout = client.config.layout
        # this path will be kept after replace
        job_2_load_1_path = Path(
            posixpath.join(
                root_path,
                create_path(
                    layout,
                    NORMALIZED_FILES[1],
                    client.schema.name,
                    load_id1,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                ),
            )
        )

        with perform_load(
            dataset_name, [NORMALIZED_FILES[0]], write_disposition="replace"
        ) as load_info:
            client, _, root_path, load_id2 = load_info

            # this one we expect to be replaced with
            job_1_load_2_path = Path(
                posixpath.join(
                    root_path,
                    create_path(
                        layout,
                        NORMALIZED_FILES[0],
                        client.schema.name,
                        load_id2,
                        load_package_timestamp=timestamp,
                        extra_placeholders=client.config.extra_placeholders,
                    ),
                )
            )

            # First file from load1 remains, second file is replaced by load2
            # assert that only these two files are in the destination folder
            is_sftp = urlparse(default_buckets_env).scheme == "sftp"
            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, **({"refresh": True} if not is_sftp else {})
            ):
                # remove internal paths
                if "_dlt" in basedir:
                    continue
                for f in files:
                    if f == INIT_FILE_NAME:
                        continue
                    paths.append(Path(posixpath.join(basedir, f)))

            ls = set(paths)
            assert ls == {job_2_load_1_path, job_1_load_2_path}


@pytest.mark.parametrize("layout", TEST_FILE_LAYOUTS)
def test_append_write_disposition(layout: str, default_buckets_env: str) -> None:
    """Run load twice with append write_disposition and assert that there are two copies of each file in destination"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)
    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    # also we would like to have reliable timestamp for this test so we patch it
    timestamp = ensure_pendulum_datetime("2024-04-05T09:16:59.942779Z")
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), perform_load(
        dataset_name,
        NORMALIZED_FILES,
        write_disposition="append",
    ) as load_info:
        client, jobs1, root_path, load_id1 = load_info
        with perform_load(dataset_name, NORMALIZED_FILES, write_disposition="append") as load_info:
            client, jobs2, root_path, load_id2 = load_info
            layout = client.config.layout
            expected_files = [
                create_path(
                    layout,
                    job.file_name(),
                    client.schema.name,
                    load_id1,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                )
                for job in jobs1
            ] + [
                create_path(
                    layout,
                    job.file_name(),
                    client.schema.name,
                    load_id2,
                    load_package_timestamp=timestamp,
                    extra_placeholders=client.config.extra_placeholders,
                )
                for job in jobs2
            ]
            expected_files = sorted([Path(posixpath.join(root_path, fn)) for fn in expected_files])  # type: ignore[misc]

            is_sftp = urlparse(default_buckets_env).scheme == "sftp"
            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, **({"refresh": True} if not is_sftp else {})
            ):
                # remove internal paths
                if "_dlt" in basedir:
                    continue
                for f in files:
                    if f == INIT_FILE_NAME:
                        continue
                    paths.append(Path(posixpath.join(basedir, f)))
            assert list(sorted(paths)) == expected_files
