import posixpath
from typing import Sequence, Tuple, List
import os

import pytest
from dlt.common.utils import digest128
from dlt.common.storages import LoadStorage, FileStorage
from dlt.common.destination.reference import LoadJob

from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob, FilesystemClientConfiguration
from dlt.load import Load
from dlt.destinations.job_impl import EmptyLoadJob

from tests.utils import clean_test_storage, init_test_logging, preserve_environ
from tests.load.filesystem.utils import get_client_instance, setup_loader
from tests.load.utils import prepare_load_package


@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl"
]


def test_filesystem_configuration() -> None:
    assert FilesystemClientConfiguration().fingerprint() == ""
    assert FilesystemClientConfiguration(bucket_url="s3://cool").fingerprint() == digest128("s3://cool")


@pytest.mark.parametrize('write_disposition', ('replace', 'append', 'merge'))
@pytest.mark.parametrize('layout',
                         ("{schema_name}/{table_name}/{load_id}.{file_id}.{ext}", # new default layout
                          "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}", # classic layout
                          "{table_name}88{load_id}-u-{file_id}.{ext}" # some strange layout
                          ))
def test_successful_load(write_disposition: str, layout: str, all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    """Test load is successful with an empty destination dataset"""

    os.environ['DESTINATION__FILESYSTEM__LAYOUT'] = layout

    client = filesystem_client
    jobs, _, load_id = perform_load(client, NORMALIZED_FILES, write_disposition=write_disposition)

    dataset_path = posixpath.join(client.fs_path, client.config.dataset_name)

    # Assert dataset dir exists
    assert client.fs_client.isdir(dataset_path)

    # Sanity check, there are jobs
    assert jobs
    for job in jobs:
        assert job.state() == 'completed'
        job_info = LoadStorage.parse_job_file_name(job.file_name())
        destination_path = posixpath.join(
            dataset_path,
            layout.format(schema_name=client.schema.name, table_name=job_info.table_name, load_id=load_id, file_id=job_info.file_id, ext=job_info.file_format)
        )

        # File is created with correct filename and path
        assert client.fs_client.isfile(destination_path)


def test_replace_write_disposition(all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    client = filesystem_client
    _, root_path, load_id1 = perform_load(client, NORMALIZED_FILES, write_disposition='replace')
    layout = filesystem_client.config.layout

    # this path will be kept after replace
    job_2_load_1_path = posixpath.join(
        root_path,
        LoadFilesystemJob.make_destination_filename(layout, NORMALIZED_FILES[1], client.schema.name, load_id1)
    )

    _, root_path, load_id2 = perform_load(client, [NORMALIZED_FILES[0]], write_disposition='replace')

    # this one we expect to be replaced with
    job_1_load_2_path = posixpath.join(
        root_path,
        LoadFilesystemJob.make_destination_filename(layout, NORMALIZED_FILES[0], client.schema.name, load_id2)
    )

    # First file from load1 remains, second file is replaced by load2
    # assert that only these two files are in the destination folder
    paths = []
    for basedir, _dirs, files  in client.fs_client.walk(client.dataset_path, detail=False, refresh=True):
        for f in files:
            paths.append(posixpath.join(basedir, f))
    ls = set(paths)
    assert ls == {job_2_load_1_path, job_1_load_2_path}


def test_append_write_disposition(all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    """Run load twice with append write_disposition and assert that there are two copies of each file in destination"""
    client = filesystem_client
    layout = filesystem_client.config.layout
    jobs1, root_path, load_id1 = perform_load(client, NORMALIZED_FILES, write_disposition='append')

    jobs2, root_path, load_id2 = perform_load(client, NORMALIZED_FILES, write_disposition='append')

    expected_files = [
        LoadFilesystemJob.make_destination_filename(layout, job.file_name(), client.schema.name, load_id1) for job in jobs1
    ] + [
        LoadFilesystemJob.make_destination_filename(layout, job.file_name(), client.schema.name, load_id2) for job in jobs2
    ]
    expected_files = sorted([posixpath.join(root_path, fn) for fn in expected_files])

    paths = []
    for basedir, _dirs, files  in client.fs_client.walk(client.dataset_path, detail=False, refresh=True):
        for f in files:
            paths.append(posixpath.join(basedir, f))
    assert list(sorted(paths)) == expected_files


def perform_load(
    client: FilesystemClient, cases: Sequence[str], write_disposition: str='append'
) -> Tuple[List[LoadJob], str, str]:
    dataset_name = client.config.dataset_name
    load = setup_loader(dataset_name)
    load_id, schema = prepare_load_package(load.load_storage, cases, write_disposition)

    client.schema = schema

    # for the replace disposition in the loader we truncate the tables, so do this here
    truncate_tables = []
    if write_disposition == 'replace':
        for item in cases:
            parts = item.split('.')
            truncate_tables.append(parts[0])

    client.initialize_storage(truncate_tables=truncate_tables)
    root_path = posixpath.join(client.fs_path, client.config.dataset_name)

    files = load.load_storage.list_new_jobs(load_id)
    jobs = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        # job execution failed
        if isinstance(job, EmptyLoadJob):
            raise RuntimeError(job.exception())
        jobs.append(job)

    return jobs, root_path, load_id
