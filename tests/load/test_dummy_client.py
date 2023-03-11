import shutil
import os
from multiprocessing.pool import ThreadPool
from typing import List, Sequence, Tuple
import pytest
from unittest.mock import patch
from prometheus_client import CollectorRegistry

from dlt.common.exceptions import TerminalException, TerminalValueError
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage, LoadStorage
from dlt.common.storages.load_storage import JobWithUnsupportedWriterException
from dlt.common.utils import uniq_id
from dlt.common.destination.reference import DestinationReference, LoadJob

from dlt.load import Load
from dlt.destinations.job_client_impl import LoadEmptyJob

from dlt.destinations import dummy
from dlt.destinations.dummy import dummy as dummy_impl
from dlt.destinations.dummy.configuration import DummyClientConfiguration

from tests.utils import clean_test_storage, init_test_logging, TEST_DICT_CONFIG_PROVIDER


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl"
]


@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


def test_spool_job_started() -> None:
    # default config keeps the job always running
    load = setup_loader()
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 2
    jobs: List[LoadJob] = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        assert type(job) is dummy_impl.LoadDummyJob
        assert job.status() == "running"
        assert load.load_storage.storage.has_file(load.load_storage._get_job_file_path(load_id, LoadStorage.STARTED_JOBS_FOLDER, job.file_name()))
        jobs.append(job)
    # still running
    remaining_jobs = load.complete_jobs(load_id, jobs)
    assert len(remaining_jobs) == 2


def test_unsupported_writer_type() -> None:
    load = setup_loader()
    load_id, _ = prepare_load_package(
        load.load_storage,
        ["event_bot.181291798a78198.0.unsupported_format"]
    )
    with pytest.raises(TerminalValueError):
        load.load_storage.list_new_jobs(load_id)


def test_unsupported_write_disposition() -> None:
    load = setup_loader()
    load_id, schema = prepare_load_package(
        load.load_storage,
        [NORMALIZED_FILES[0]]
    )
    # mock unsupported disposition
    schema.get_table("event_user")["write_disposition"] = "merge"
    # write back schema
    load.load_storage._save_schema(schema, load_id)
    load.run(ThreadPool())
    # job with unsupported write disp. is failed
    exception = [f for f in load.load_storage.list_failed_jobs(load_id) if f.endswith(".exception")][0]
    assert "LoadClientUnsupportedWriteDisposition" in load.load_storage.storage.load(exception)


def test_spool_job_failed() -> None:
    # this config fails job on start
    load = setup_loader(client_config=DummyClientConfiguration(fail_prob=1.0))
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    files = load.load_storage.list_new_jobs(load_id)
    jobs: List[LoadJob] = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        assert type(job) is LoadEmptyJob
        assert job.status() == "failed"
        assert load.load_storage.storage.has_file(load.load_storage._get_job_file_path(load_id, LoadStorage.STARTED_JOBS_FOLDER, job.file_name()))
        jobs.append(job)
    # complete files
    remaining_jobs = load.complete_jobs(load_id, jobs)
    assert len(remaining_jobs) == 0
    for job in jobs:
        assert load.load_storage.storage.has_file(load.load_storage._get_job_file_path(load_id, LoadStorage.FAILED_JOBS_FOLDER, job.file_name()))
        assert load.load_storage.storage.has_file(load.load_storage._get_job_file_path(load_id, LoadStorage.FAILED_JOBS_FOLDER, job.file_name() + ".exception"))
    started_files = load.load_storage.list_started_jobs(load_id)
    assert len(started_files) == 0


def test_spool_job_retry_new() -> None:
    # this config retries job on start (transient fail)
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    files = load.load_storage.list_new_jobs(load_id)
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        assert job is None

    # call higher level function that returns jobs and counts
    load.pool = ThreadPool()
    jobs_count, jobs = load.spool_new_jobs(load_id, schema)
    assert jobs_count == 2
    assert len(jobs) == 0


def test_spool_job_retry_started() -> None:
    # this config keeps the job always running
    load = setup_loader()
    # dummy_impl.CLIENT_CONFIG = DummyClientConfiguration
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    files = load.load_storage.list_new_jobs(load_id)
    jobs: List[LoadJob] = []
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        assert type(job) is dummy_impl.LoadDummyJob
        assert job.status() == "running"
        assert  load.load_storage.storage.has_file(load.load_storage._get_job_file_path(load_id, LoadStorage.STARTED_JOBS_FOLDER, job.file_name()))
        # mock job config to make it retry
        job.retry_prob = 1.0
        jobs.append(job)
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 0
    # should retry, that moves jobs into new folder
    remaining_jobs = load.complete_jobs(load_id, jobs)
    assert len(remaining_jobs) == 0
    # clear retry flag
    dummy_impl.JOBS = {}
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 2
    # parse the new job names
    for fn in load.load_storage.list_new_jobs(load_id):
        # we failed when already running the job so retry count will increase
        assert LoadStorage.parse_job_file_name(fn).retry_count == 1
    for f in files:
        job = Load.w_spool_job(load, f, load_id, schema)
        assert job.status() == "running"


def test_try_retrieve_job() -> None:
    load = setup_loader()
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    # manually move jobs to started
    files = load.load_storage.list_new_jobs(load_id)
    for f in files:
        load.load_storage.start_job(load_id, FileStorage.get_file_name_from_file_path(f))
    # dummy client may retrieve jobs that it created itself, jobs in started folder are unknown
    # and returned as terminal
    with load.destination.client(schema, load.initial_client_config) as c:
        job_count, jobs = load.retrieve_jobs(c, load_id)
        assert job_count == 2
        for j in jobs:
            assert j.status() == "failed"
    # new load package
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    load.pool = ThreadPool()
    jobs_count, jobs = load.spool_new_jobs(load_id, schema)
    assert jobs_count == 2
    # now jobs are known
    with load.destination.client(schema, load.initial_client_config) as c:
        job_count, jobs = load.retrieve_jobs(c, load_id)
        assert job_count == 2
        for j in jobs:
            assert j.status() == "running"


def test_completed_loop() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    assert_complete_job(load, load.load_storage.storage)


def test_failed_loop() -> None:
    # ask to delete completed
    load = setup_loader(delete_completed_jobs=True, client_config=DummyClientConfiguration(fail_prob=1.0))
    # actually not deleted because one of the jobs failed
    assert_complete_job(load, load.load_storage.storage, should_delete_completed=False)


def test_completed_loop_with_delete_completed() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    load.load_storage = load.create_storage(is_storage_owner=False)
    load.load_storage.config.delete_completed_jobs = True
    assert_complete_job(load, load.load_storage.storage, should_delete_completed=True)


def test_retry_on_new_loop() -> None:
    # test job that retries sitting in new jobs
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    load_id, schema = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    load.run(ThreadPool())
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 2
    # one job will be completed
    # print(list(client.JOBS.keys()))
    # client.JOBS["event_user.839c6e6b514e427687586ccc65bf133f.jsonl"].retry_prob = 0
    # client.JOBS["event_user.839c6e6b514e427687586ccc65bf133f.jsonl"].completed_prob = 1.0
    load.run(ThreadPool())
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 2
    # jobs will be completed
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    load.run(ThreadPool())
    files = load.load_storage.list_new_jobs(load_id)
    assert len(files) == 0
    load.run(ThreadPool())
    assert not load.load_storage.storage.has_folder(load.load_storage.get_package_path(load_id))
    # parse the completed job names
    completed_path = load.load_storage.get_completed_package_path(load_id)
    for fn in load.load_storage.storage.list_folder_files(os.path.join(completed_path, LoadStorage.COMPLETED_JOBS_FOLDER)):
        # we failed on initializing a job, in that case the retry count will not be updated
        assert LoadStorage.parse_job_file_name(fn).retry_count == 0


def test_wrong_writer_type() -> None:
    load = setup_loader()
    load_id, _ = prepare_load_package(
        load.load_storage,
        ["event_bot.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values",
        "event_user.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values"]
    )
    with pytest.raises(JobWithUnsupportedWriterException) as exv:
        load.run(ThreadPool())
    assert exv.value.load_id == load_id


def test_exceptions() -> None:
    try:
        raise TerminalValueError("a")
    except TerminalException:
        assert True
    else:
        raise AssertionError()


def assert_complete_job(load: Load, storage: FileStorage, should_delete_completed: bool = False) -> None:
    load_id, _ = prepare_load_package(
        load.load_storage,
        NORMALIZED_FILES
    )
    # will complete all jobs
    with patch.object(dummy_impl.DummyClient, "complete_load") as complete_load:
        load.run(ThreadPool())
        # did process schema update
        assert storage.has_file(os.path.join(load.load_storage.get_package_path(load_id), LoadStorage.PROCESSED_SCHEMA_UPDATES_FILE_NAME))
        # will finalize the whole package
        load.run(ThreadPool())
        # moved to loaded
        assert not storage.has_folder(load.load_storage.get_package_path(load_id))
        completed_path = load.load_storage.get_completed_package_path(load_id)
        if should_delete_completed:
            # package was deleted
            assert not storage.has_folder(completed_path)
        else:
            # package not deleted
            assert storage.has_folder(completed_path)
        # complete load on client was called
        complete_load.assert_called_once_with(load_id)


def prepare_load_package(load_storage: LoadStorage, cases: Sequence[str]) -> Tuple[str, Schema]:
    load_id = uniq_id()
    load_storage.create_temp_load_package(load_id)
    for case in cases:
        path = f"./tests/load/cases/loading/{case}"
        shutil.copy(path, load_storage.storage.make_full_path(f"{load_id}/{LoadStorage.NEW_JOBS_FOLDER}"))
    for f in ["schema_updates.json", "schema.json"]:
        path = f"./tests/load/cases/loading/{f}"
        shutil.copy(path, load_storage.storage.make_full_path(load_id))
    load_storage.commit_temp_load_package(load_id)
    schema = load_storage.load_package_schema(load_id)
    return load_id, schema


def setup_loader(delete_completed_jobs: bool = False, client_config: DummyClientConfiguration = None) -> Load:
    # reset jobs for a test
    dummy_impl.JOBS = {}
    destination: DestinationReference = dummy
    client_config = client_config or DummyClientConfiguration(loader_file_format="jsonl")
    # patch destination to provide client_config
    # destination.client = lambda schema: dummy_impl.DummyClient(schema, client_config)

    # setup loader
    with TEST_DICT_CONFIG_PROVIDER().values({"delete_completed_jobs": delete_completed_jobs}):
        return Load(
            destination,
            CollectorRegistry(auto_describe=True),
            initial_client_config=client_config
    )
