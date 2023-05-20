from typing import Sequence, Tuple, cast, List
import shutil
from pathlib import Path
import json

import pytest

from dlt.common.utils import uniq_id
from dlt.common.storages import LoadStorage
from dlt.common.schema import Schema
from dlt.common.destination.reference import DestinationReference, LoadJob
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.destinations import filesystem
from dlt.load import Load

from tests.utils import clean_test_storage, init_test_logging, TEST_DICT_CONFIG_PROVIDER, preserve_environ
from tests.load.filesystem.utils import get_client, setup_loader


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl"
]


def test_succesful_load(all_buckets_env: str) -> None:
    dataset_name = 'test_' + uniq_id()
    dataset_name = 'test_' + uniq_id()
    client, jobs, root_path, load_id = perform_load(all_buckets_env, dataset_name, NORMALIZED_FILES, write_disposition='replace')

    for job in jobs:
        assert job.state() == 'completed'
        dest_fn = LoadFilesystemJob.make_destination_filename(job.file_name(), client.schema.name, load_id)
        assert client.fs_client.exists(root_path.joinpath(dest_fn))


def test_replace_write_disposition(all_buckets_env: str) -> None:
    dataset_name = 'test_' + uniq_id()
    client, jobs1, root_path, load_id1 = perform_load(all_buckets_env, dataset_name, NORMALIZED_FILES, write_disposition='replace')

    job_1_paths = []

    for job in jobs1:
        dest_fn = LoadFilesystemJob.make_destination_filename(job.file_name(), client.schema.name, load_id1)
        job_1_paths.append(root_path.joinpath(dest_fn))

    client, jobs2, root_path, load_id2 = perform_load(all_buckets_env, dataset_name, [NORMALIZED_FILES[0]], write_disposition='replace')

    job_2_path = root_path.joinpath(LoadFilesystemJob.make_destination_filename(jobs2[0].file_name(), client.schema.name, load_id2))

    # First file from load1 remains, second file is replaced by load2
    # assert that only these two files are in the destination folder
    ls = set(client.fs_client.ls(root_path))
    assert ls == {str(job_1_paths[0]), str(job_2_path)}


def perform_load(
    bucket_url: str, dataset_name: str, cases: Sequence[str], write_disposition: str='append'
) -> Tuple[FilesystemClient, List[LoadJob], Path, str]:
    load = setup_loader(dataset_name)
    load_id, schema = prepare_load_package(load.load_storage, cases, write_disposition)

    client = get_client(schema, dataset_name)
    client.initialize_storage()
    root_path = Path(client.fs_path).joinpath(client.config.dataset_name)

    files = load.load_storage.list_new_jobs(load_id)
    jobs = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        jobs.append(job)

    return client, jobs, root_path, load_id



def prepare_load_package(load_storage: LoadStorage, cases: Sequence[str], write_disposition: str='append') -> Tuple[str, Schema]:
    load_id = uniq_id()
    load_storage.create_temp_load_package(load_id)
    for case in cases:
        path = f"./tests/load/cases/loading/{case}"
        shutil.copy(path, load_storage.storage.make_full_path(f"{load_id}/{LoadStorage.NEW_JOBS_FOLDER}"))
    schema_path = Path("./tests/load/cases/loading/schema.json")
    data = json.loads(schema_path.read_text(encoding='utf8'))
    for name, table in data['tables'].items():
        if name.startswith('_dlt'):
            continue
        table['write_disposition'] = write_disposition
    Path(
        load_storage.storage.make_full_path(load_id)
    ).joinpath(schema_path.name).write_text(json.dumps(data), encoding='utf8')

    load_storage.commit_temp_load_package(load_id)
    schema = load_storage.load_package_schema(load_id)
    return load_id, schema
