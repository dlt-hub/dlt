from typing import Sequence, Tuple, cast, List
import shutil
from pathlib import Path
import json

import pytest

from dlt.common.utils import uniq_id
from dlt.common.storages import LoadStorage, FileStorage
from dlt.common.schema import Schema
from dlt.common.destination.reference import DestinationReference, LoadJob
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.destinations import filesystem
from dlt.load import Load

from tests.utils import clean_test_storage, init_test_logging, TEST_DICT_CONFIG_PROVIDER, preserve_environ
from tests.load.filesystem.utils import get_client, setup_loader
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

@pytest.mark.parametrize('write_disposition', ('replace', 'append', 'merge'))
def test_succesful_load(write_disposition: str, all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    """Test load is successful with an empty destination dataset"""
    client = filesystem_client
    dataset_name = client.config.dataset_name
    jobs, _, load_id = perform_load(client, NORMALIZED_FILES, write_disposition=write_disposition)

    dataset_path = Path(client.fs_path).joinpath(client.config.dataset_name)

    # Assert dataset dir exists
    assert client.fs_client.isdir(str(dataset_path))

    # Sanity check, there are jobs
    assert jobs
    for job in jobs:
        assert job.state() == 'completed'
        job_info = LoadStorage.parse_job_file_name(job.file_name())
        destination_path = dataset_path.joinpath(f"{client.schema.name}.{job_info.table_name}.{load_id}.{job_info.file_id}.{job_info.file_format}")

        # File is created with correct filename and path
        assert client.fs_client.isfile(destination_path)


def test_replace_write_disposition(all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    client = filesystem_client
    jobs1, root_path, load_id1 = perform_load(client, NORMALIZED_FILES, write_disposition='replace')

    job_1_paths = []

    for job in jobs1:
        dest_fn = LoadFilesystemJob.make_destination_filename(job.file_name(), client.schema.name, load_id1)
        job_1_paths.append(root_path.joinpath(dest_fn))

    jobs2, root_path, load_id2 = perform_load(client, [NORMALIZED_FILES[0]], write_disposition='replace')

    job_2_path = root_path.joinpath(LoadFilesystemJob.make_destination_filename(jobs2[0].file_name(), client.schema.name, load_id2))

    # First file from load1 remains, second file is replaced by load2
    # assert that only these two files are in the destination folder
    ls = set(client.fs_client.ls(root_path))
    assert ls == {str(job_1_paths[0]), str(job_2_path)}


def test_append_write_disposition(all_buckets_env: str, filesystem_client: FilesystemClient) -> None:
    """Run load twice with append write_disposition and assert that there are two copies of each file in destination"""
    client = filesystem_client
    jobs1, root_path, load_id1 = perform_load(client, NORMALIZED_FILES, write_disposition='append')

    jobs2, root_path, load_id2 = perform_load(client, NORMALIZED_FILES, write_disposition='append')

    expected_files = [
        LoadFilesystemJob.make_destination_filename(job.file_name(), client.schema.name, load_id1) for job in jobs1
    ] + [
        LoadFilesystemJob.make_destination_filename(job.file_name(), client.schema.name, load_id2) for job in jobs2
    ]
    expected_files = sorted([str(root_path.joinpath(fn)) for fn in expected_files])

    assert list(sorted(client.fs_client.ls(root_path))) == expected_files


def perform_load(
    client: FilesystemClient, cases: Sequence[str], write_disposition: str='append'
) -> Tuple[List[LoadJob], Path, str]:
    dataset_name = client.config.dataset_name
    load = setup_loader(dataset_name)
    load_id, schema = prepare_load_package(load.load_storage, cases, write_disposition)

    client.schema = schema
    client.initialize_storage()
    root_path = Path(client.fs_path).joinpath(client.config.dataset_name)

    files = load.load_storage.list_new_jobs(load_id)
    jobs = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        jobs.append(job)

    return jobs, root_path, load_id
