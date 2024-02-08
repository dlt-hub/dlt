import posixpath
import os

import pytest

from dlt.common.utils import digest128, uniq_id
from dlt.common.storages import FileStorage, ParsedLoadJobFileName

from dlt.destinations.impl.filesystem.filesystem import (
    LoadFilesystemJob,
    FilesystemDestinationClientConfiguration,
)

from tests.load.filesystem.utils import perform_load
from tests.utils import clean_test_storage, init_test_logging
from tests.utils import preserve_environ, autouse_test_storage


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

ALL_LAYOUTS = (
    None,
    "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",  # new default layout with schema
    "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}",  # classic layout
    "{table_name}88{load_id}-u-{file_id}.{ext}",  # default layout with strange separators
)


def test_filesystem_destination_configuration() -> None:
    assert FilesystemDestinationClientConfiguration().fingerprint() == ""
    assert FilesystemDestinationClientConfiguration(
        bucket_url="s3://cool"
    ).fingerprint() == digest128("s3://cool")


@pytest.mark.parametrize("write_disposition", ("replace", "append", "merge"))
@pytest.mark.parametrize("layout", ALL_LAYOUTS)
def test_successful_load(write_disposition: str, layout: str, default_buckets_env: str) -> None:
    """Test load is successful with an empty destination dataset"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    dataset_name = "test_" + uniq_id()

    with perform_load(
        dataset_name, NORMALIZED_FILES, write_disposition=write_disposition
    ) as load_info:
        client, jobs, _, load_id = load_info
        layout = client.config.layout
        dataset_path = posixpath.join(client.fs_path, client.config.dataset_name)

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
                ),
            )

            # File is created with correct filename and path
            assert client.fs_client.isfile(destination_path)


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
def test_replace_write_disposition(layout: str, default_buckets_env: str) -> None:
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)
    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    with perform_load(dataset_name, NORMALIZED_FILES, write_disposition="replace") as load_info:
        client, _, root_path, load_id1 = load_info
        layout = client.config.layout

        # this path will be kept after replace
        job_2_load_1_path = posixpath.join(
            root_path,
            LoadFilesystemJob.make_destination_filename(
                layout, NORMALIZED_FILES[1], client.schema.name, load_id1
            ),
        )

        with perform_load(
            dataset_name, [NORMALIZED_FILES[0]], write_disposition="replace"
        ) as load_info:
            client, _, root_path, load_id2 = load_info

            # this one we expect to be replaced with
            job_1_load_2_path = posixpath.join(
                root_path,
                LoadFilesystemJob.make_destination_filename(
                    layout, NORMALIZED_FILES[0], client.schema.name, load_id2
                ),
            )

            # First file from load1 remains, second file is replaced by load2
            # assert that only these two files are in the destination folder
            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, refresh=True
            ):
                for f in files:
                    paths.append(posixpath.join(basedir, f))
            ls = set(paths)
            assert ls == {job_2_load_1_path, job_1_load_2_path}


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
def test_append_write_disposition(layout: str, default_buckets_env: str) -> None:
    """Run load twice with append write_disposition and assert that there are two copies of each file in destination"""
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)
    dataset_name = "test_" + uniq_id()
    # NOTE: context manager will delete the dataset at the end so keep it open until the end
    with perform_load(dataset_name, NORMALIZED_FILES, write_disposition="append") as load_info:
        client, jobs1, root_path, load_id1 = load_info
        with perform_load(dataset_name, NORMALIZED_FILES, write_disposition="append") as load_info:
            client, jobs2, root_path, load_id2 = load_info
            layout = client.config.layout
            expected_files = [
                LoadFilesystemJob.make_destination_filename(
                    layout, job.file_name(), client.schema.name, load_id1
                )
                for job in jobs1
            ] + [
                LoadFilesystemJob.make_destination_filename(
                    layout, job.file_name(), client.schema.name, load_id2
                )
                for job in jobs2
            ]
            expected_files = sorted([posixpath.join(root_path, fn) for fn in expected_files])

            paths = []
            for basedir, _dirs, files in client.fs_client.walk(
                client.dataset_path, detail=False, refresh=True
            ):
                for f in files:
                    paths.append(posixpath.join(basedir, f))
            assert list(sorted(paths)) == expected_files
