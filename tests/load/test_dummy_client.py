import os
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time
from unittest import mock
import pytest
from unittest.mock import patch
from typing import List, Tuple

from dlt.common.exceptions import TerminalException, TerminalValueError
from dlt.common.storages import FileStorage, PackageStorage, ParsedLoadJobFileName
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.load_package import TPackageJobState
from dlt.common.storages.load_storage import JobFileFormatUnsupported
from dlt.common.destination.reference import RunnableLoadJob, TDestination
from dlt.common.schema.utils import (
    fill_hints_from_parent_and_clone_table,
    get_nested_tables,
    get_root_table,
)

from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations import dummy, filesystem
from dlt.destinations.impl.dummy import dummy as dummy_impl
from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration

from dlt.load import Load
from dlt.load.configuration import LoaderConfiguration
from dlt.load.exceptions import (
    LoadClientJobFailed,
    LoadClientJobRetry,
    TableChainFollowupJobCreationFailedException,
    FollowupJobCreationFailedException,
)
from dlt.load.utils import get_completed_table_chain, init_client, _extend_tables_with_table_chain

from tests.utils import (
    MockPipeline,
    clean_test_storage,
    init_test_logging,
    TEST_DICT_CONFIG_PROVIDER,
)
from tests.load.utils import prepare_load_package
from tests.utils import skip_if_not_active, TEST_STORAGE_ROOT

skip_if_not_active("dummy")

NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl",
]

SMALL_FILES = ["event_user.1234.0.jsonl", "event_loop_interrupted.1234.0.jsonl"]

REMOTE_FILESYSTEM = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, "_remote_filesystem"))


@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


def test_spool_job_started() -> None:
    # default config keeps the job always running
    load = setup_loader()
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    assert len(files) == 2
    jobs: List[RunnableLoadJob] = []
    for f in files:
        job = load.submit_job(f, load_id, schema)
        assert job.state() == "completed"
        assert type(job) is dummy_impl.LoadDummyJob
        # jobs runs, but is not moved yet (loader will do this)
        assert load.load_storage.normalized_packages.storage.has_file(
            load.load_storage.normalized_packages.get_job_file_path(
                load_id, PackageStorage.STARTED_JOBS_FOLDER, job.file_name()
            )
        )
        assert_job_metrics(job, "completed")
        jobs.append(job)
    remaining_jobs, finalized_jobs, _ = load.complete_jobs(load_id, jobs, schema)
    assert len(remaining_jobs) == 0
    assert len(finalized_jobs) == 2
    assert len(load._job_metrics) == 2
    for job in jobs:
        assert load._job_metrics[job.job_id()] == job.metrics()


def test_unsupported_writer_type() -> None:
    load = setup_loader()
    load_id, _ = prepare_load_package(
        load.load_storage, ["event_bot.181291798a78198.0.unsupported_format"]
    )
    with pytest.raises(TerminalValueError):
        load.load_storage.list_new_jobs(load_id)


def test_unsupported_write_disposition() -> None:
    # tests terminal error on retrieving job
    load = setup_loader()
    load_id, schema = prepare_load_package(load.load_storage, [NORMALIZED_FILES[0]])
    # mock unsupported disposition
    schema.get_table("event_user")["write_disposition"] = "skip"
    # write back schema
    load.load_storage.normalized_packages.save_schema(load_id, schema)
    with pytest.raises(LoadClientJobFailed) as e:
        with ThreadPoolExecutor() as pool:
            load.run(pool)

    assert "LoadClientUnsupportedWriteDisposition" in e.value.failed_message


def test_big_loadpackages() -> None:
    """
    This test guards against changes in the load that exponentially makes the loads slower
    """

    load = setup_loader()
    # make the loop faster by basically not sleeping
    load._run_loop_sleep_duration = 0.001
    load_id, schema = prepare_load_package(load.load_storage, SMALL_FILES, jobs_per_case=500)
    start_time = time()
    with ThreadPoolExecutor(max_workers=20) as pool:
        load.run(pool)
    duration = float(time() - start_time)

    # sanity check
    assert duration > 3
    # we want 1000 empty processed jobs to need less than 15 seconds total (locally it runs in 5)
    assert duration < 15

    # we should have 1000 jobs processed
    assert len(dummy_impl.JOBS) == 1000


def test_get_new_jobs_info() -> None:
    load = setup_loader()
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)

    # no write disposition specified - get all new jobs
    assert len(load.get_new_jobs_info(load_id)) == 2


def test_get_completed_table_chain_single_job_per_table() -> None:
    load = setup_loader()
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)

    # update tables so we have all possible hints
    for table_name, table in schema.tables.items():
        schema.tables[table_name] = fill_hints_from_parent_and_clone_table(schema.tables, table)

    top_job_table = get_root_table(schema.tables, "event_user")
    all_jobs = load.load_storage.normalized_packages.list_all_jobs_with_states(load_id)
    assert get_completed_table_chain(schema, all_jobs, top_job_table) is None
    # fake being completed
    assert (
        len(
            get_completed_table_chain(
                schema,
                all_jobs,
                top_job_table,
                "event_user.839c6e6b514e427687586ccc65bf133f.jsonl",
            )
        )
        == 1
    )
    # actually complete
    loop_top_job_table = get_root_table(schema.tables, "event_loop_interrupted")
    load.load_storage.normalized_packages.start_job(
        load_id, "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl"
    )
    all_jobs = load.load_storage.normalized_packages.list_all_jobs_with_states(load_id)
    assert get_completed_table_chain(schema, all_jobs, loop_top_job_table) is None
    load.load_storage.normalized_packages.complete_job(
        load_id, "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl"
    )
    all_jobs = load.load_storage.normalized_packages.list_all_jobs_with_states(load_id)
    assert get_completed_table_chain(schema, all_jobs, loop_top_job_table) == [
        schema.get_table("event_loop_interrupted")
    ]
    assert get_completed_table_chain(
        schema, all_jobs, loop_top_job_table, "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl"
    ) == [schema.get_table("event_loop_interrupted")]


def test_spool_job_failed() -> None:
    # this config fails job on start
    load = setup_loader(client_config=DummyClientConfiguration(fail_prob=1.0))
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    jobs: List[RunnableLoadJob] = []
    for f in files:
        job = load.submit_job(f, load_id, schema)
        assert type(job) is dummy_impl.LoadDummyJob
        assert job.state() == "failed"
        assert load.load_storage.normalized_packages.storage.has_file(
            load.load_storage.normalized_packages.get_job_file_path(
                load_id, PackageStorage.STARTED_JOBS_FOLDER, job.file_name()
            )
        )
        assert_job_metrics(job, "failed")
        jobs.append(job)
    assert len(jobs) == 2
    # complete files
    remaining_jobs, finalized_jobs, _ = load.complete_jobs(load_id, jobs, schema)
    assert len(remaining_jobs) == 0
    assert len(finalized_jobs) == 2
    for job in jobs:
        assert load.load_storage.normalized_packages.storage.has_file(
            load.load_storage.normalized_packages.get_job_file_path(
                load_id, PackageStorage.FAILED_JOBS_FOLDER, job.file_name()
            )
        )
        assert load.load_storage.normalized_packages.storage.has_file(
            load.load_storage.normalized_packages.get_job_file_path(
                load_id, PackageStorage.FAILED_JOBS_FOLDER, job.file_name() + ".exception"
            )
        )
        # load should collect two jobs
        assert load._job_metrics[job.job_id()] == job.metrics()
    started_files = load.load_storage.normalized_packages.list_started_jobs(load_id)
    assert len(started_files) == 0

    # test the whole
    loader_config = LoaderConfiguration(
        raise_on_failed_jobs=False,
        workers=1,
        pool_type="none",
    )
    load = setup_loader(
        client_config=DummyClientConfiguration(fail_prob=1.0),
        loader_config=loader_config,
    )
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    run_all(load)

    package_info = load.load_storage.get_load_package_info(load_id)
    assert package_info.state == "loaded"
    # all jobs failed
    assert len(package_info.jobs["failed_jobs"]) == 2
    # check metrics
    load_info = load.get_step_info(MockPipeline("pipe", True))  # type: ignore[abstract]
    metrics = load_info.metrics[load_id][0]["job_metrics"]
    assert len(metrics) == 2
    for job in jobs:
        assert job.job_id() in metrics
        assert metrics[job.job_id()].state == "failed"


def test_spool_job_failed_terminally_exception_init() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(fail_terminally_in_init=True))
    load_id, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    with patch.object(dummy_impl.DummyClient, "complete_load") as complete_load:
        with pytest.raises(LoadClientJobFailed) as py_ex:
            run_all(load)
        assert py_ex.value.load_id == load_id
        package_info = load.load_storage.get_load_package_info(load_id)
        assert package_info.state == "aborted"
        # both failed - we wait till the current loop is completed and then raise
        assert len(package_info.jobs["failed_jobs"]) == 2
        assert len(package_info.jobs["started_jobs"]) == 0
        # load id was never committed
        complete_load.assert_not_called()
        # metrics can be gathered
        assert len(load._job_metrics) == 2
        load_info = load.get_step_info(MockPipeline("pipe", True))  # type: ignore[abstract]
        metrics = load_info.metrics[load_id][0]["job_metrics"]
        assert len(metrics) == 2


def test_spool_job_failed_transiently_exception_init() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(fail_transiently_in_init=True))
    load_id, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    with patch.object(dummy_impl.DummyClient, "complete_load") as complete_load:
        with pytest.raises(LoadClientJobRetry) as py_ex:
            run_all(load)
        assert py_ex.value.load_id == load_id
        package_info = load.load_storage.get_load_package_info(load_id)
        assert package_info.state == "normalized"
        # both failed - we wait till the current loop is completed and then raise
        assert len(package_info.jobs["failed_jobs"]) == 0
        assert len(package_info.jobs["started_jobs"]) == 0
        assert len(package_info.jobs["new_jobs"]) == 2

        # load id was never committed
        complete_load.assert_not_called()
        # no metrics were gathered
        assert len(load._job_metrics) == 0
        load_info = load.get_step_info(MockPipeline("pipe", True))  # type: ignore[abstract]
        assert len(load_info.metrics) == 0


def test_spool_job_failed_exception_complete() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(fail_prob=1.0))
    load_id, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    with pytest.raises(LoadClientJobFailed) as py_ex:
        run_all(load)
    assert py_ex.value.load_id == load_id
    package_info = load.load_storage.get_load_package_info(load_id)
    assert package_info.state == "aborted"
    # both failed - we wait till the current loop is completed and then raise
    assert len(package_info.jobs["failed_jobs"]) == 2
    assert len(package_info.jobs["started_jobs"]) == 0
    # metrics can be gathered
    assert len(load._job_metrics) == 2
    load_info = load.get_step_info(MockPipeline("pipe", True))  # type: ignore[abstract]
    metrics = load_info.metrics[load_id][0]["job_metrics"]
    assert len(metrics) == 2


def test_spool_job_retry_new() -> None:
    # this config retries job on start (transient fail)
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    for f in files:
        job = load.submit_job(f, load_id, schema)
        assert job.state() == "retry"


def test_spool_job_retry_spool_new() -> None:
    # this config retries job on start (transient fail)
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    # call higher level function that returns jobs and counts
    with ThreadPoolExecutor() as pool:
        load.pool = pool
        jobs = load.start_new_jobs(load_id, schema, [])
        assert len(jobs) == 2


def test_spool_job_retry_started() -> None:
    # this config keeps the job always running
    load = setup_loader()
    # dummy_impl.CLIENT_CONFIG = DummyClientConfiguration
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    jobs: List[RunnableLoadJob] = []
    for f in files:
        job = load.submit_job(f, load_id, schema)
        assert type(job) is dummy_impl.LoadDummyJob
        assert job.state() == "completed"
        # mock job state to make it retry
        job.config.retry_prob = 1.0
        job._state = "retry"
        assert load.load_storage.normalized_packages.storage.has_file(
            load.load_storage.normalized_packages.get_job_file_path(
                load_id, PackageStorage.STARTED_JOBS_FOLDER, job.file_name()
            )
        )
        jobs.append(job)
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    assert len(files) == 0
    # should retry, that moves jobs into new folder, jobs are not counted as finalized
    remaining_jobs, finalized_jobs, _ = load.complete_jobs(load_id, jobs, schema)
    assert len(remaining_jobs) == 0
    assert len(finalized_jobs) == 0
    assert len(load._job_metrics) == 0
    # clear retry flag
    dummy_impl.JOBS = {}
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    assert len(files) == 2
    # parse the new job names
    for fn in load.load_storage.normalized_packages.list_new_jobs(load_id):
        # we failed when already running the job so retry count will increase
        assert ParsedLoadJobFileName.parse(fn).retry_count == 1

    # this time it will pass
    for f in files:
        job = load.submit_job(f, load_id, schema)
        assert job.state() == "completed"


def test_try_retrieve_job() -> None:
    load = setup_loader()
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    # manually move jobs to started
    files = load.load_storage.normalized_packages.list_new_jobs(load_id)
    for f in files:
        load.load_storage.normalized_packages.start_job(
            load_id, FileStorage.get_file_name_from_file_path(f)
        )
    # dummy client may retrieve jobs that it created itself, jobs in started folder are unknown
    # and returned as terminal
    jobs = load.resume_started_jobs(load_id, schema)
    assert len(jobs) == 2
    for j in jobs:
        assert j.state() == "failed"
    # new load package
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    load.pool = ThreadPoolExecutor()
    jobs = load.start_new_jobs(load_id, schema, [])  # type: ignore
    assert len(jobs) == 2
    # now jobs are known
    jobs = load.resume_started_jobs(load_id, schema)
    assert len(jobs) == 2
    for j in jobs:
        assert j.state() == "completed"
    assert len(dummy_impl.RETRIED_JOBS) == 2


def test_completed_loop() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    assert_complete_job(load)
    assert len(dummy_impl.JOBS) == 2
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0


def test_completed_loop_followup_jobs() -> None:
    # TODO: until we fix how we create capabilities we must set env
    load = setup_loader(
        client_config=DummyClientConfiguration(completed_prob=1.0, create_followup_jobs=True)
    )
    assert_complete_job(load)
    # for each JOB there's REFERENCE JOB
    assert len(dummy_impl.JOBS) == 2 * 2
    assert len(dummy_impl.JOBS) == len(dummy_impl.CREATED_FOLLOWUP_JOBS) * 2


def test_failing_followup_jobs() -> None:
    load = setup_loader(
        client_config=DummyClientConfiguration(
            completed_prob=1.0, create_followup_jobs=True, fail_followup_job_creation=True
        )
    )
    with pytest.raises(FollowupJobCreationFailedException) as exc:
        assert_complete_job(load)
        # follow up job errors on main thread
    assert "Failed to create followup job" in str(exc)

    # followup job fails, we have both jobs in started folder
    load_id = list(dummy_impl.JOBS.values())[1]._load_id
    started_files = load.load_storage.normalized_packages.list_started_jobs(load_id)
    assert len(started_files) == 2
    assert len(dummy_impl.JOBS) == 2
    assert len(dummy_impl.RETRIED_JOBS) == 0
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0
    # no metrics were collected
    assert len(load._job_metrics) == 0

    # now we can retry the same load, it will restart the two jobs and successfully create the followup jobs
    load.initial_client_config.fail_followup_job_creation = False  # type: ignore
    assert_complete_job(load, load_id=load_id)
    assert len(dummy_impl.JOBS) == 2 * 2
    assert len(dummy_impl.JOBS) == len(dummy_impl.CREATED_FOLLOWUP_JOBS) * 2
    assert len(dummy_impl.RETRIED_JOBS) == 2


def test_failing_table_chain_followup_jobs() -> None:
    load = setup_loader(
        client_config=DummyClientConfiguration(
            completed_prob=1.0,
            create_followup_table_chain_reference_jobs=True,
            fail_table_chain_followup_job_creation=True,
        )
    )
    with pytest.raises(TableChainFollowupJobCreationFailedException) as exc:
        assert_complete_job(load)
        # follow up job errors on main thread
    assert "Failed creating table chain followup jobs for table chain with root table" in str(exc)

    # table chain followup job fails, we have both jobs in started folder
    load_id = list(dummy_impl.JOBS.values())[1]._load_id
    started_files = load.load_storage.normalized_packages.list_started_jobs(load_id)
    assert len(started_files) == 2
    assert len(dummy_impl.JOBS) == 2
    assert len(dummy_impl.RETRIED_JOBS) == 0
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0
    # no metrics were collected
    assert len(load._job_metrics) == 0

    # now we can retry the same load, it will restart the two jobs and successfully create the table chain followup jobs
    load.initial_client_config.fail_table_chain_followup_job_creation = False  # type: ignore
    assert_complete_job(load, load_id=load_id)
    assert len(dummy_impl.JOBS) == 2 * 2
    assert len(dummy_impl.JOBS) == len(dummy_impl.CREATED_TABLE_CHAIN_FOLLOWUP_JOBS) * 2
    assert len(dummy_impl.RETRIED_JOBS) == 2


def test_failing_sql_table_chain_job() -> None:
    """
    Make sure we get a useful exception from a failing sql job
    """
    load = setup_loader(
        client_config=DummyClientConfiguration(
            completed_prob=1.0, create_followup_table_chain_sql_jobs=True
        ),
    )
    with pytest.raises(Exception) as exc:
        assert_complete_job(load)

    # sql jobs always fail because this is not an sql client, we just make sure the exception is there
    assert "Failed creating table chain followup jobs for table chain with root table" in str(exc)


def test_successful_table_chain_jobs() -> None:
    load = setup_loader(
        client_config=DummyClientConfiguration(
            completed_prob=1.0, create_followup_table_chain_reference_jobs=True
        ),
    )
    # we create 10 jobs per case (for two cases)
    # and expect two table chain jobs at the end
    assert_complete_job(load, jobs_per_case=10)
    assert len(dummy_impl.CREATED_TABLE_CHAIN_FOLLOWUP_JOBS) == 2
    assert len(dummy_impl.JOBS) == 22

    # check that we have 10 references per followup job
    for _, job in dummy_impl.CREATED_TABLE_CHAIN_FOLLOWUP_JOBS.items():
        assert len(job._remote_paths) == 10  # type: ignore


def test_failed_loop() -> None:
    # ask to delete completed
    load = setup_loader(
        delete_completed_jobs=True, client_config=DummyClientConfiguration(fail_prob=1.0)
    )
    # actually not deleted because one of the jobs failed
    with pytest.raises(LoadClientJobFailed) as e:
        assert_complete_job(load, should_delete_completed=False)

    assert "a random fail occurred" in e.value.failed_message
    # two failed jobs
    assert len(dummy_impl.JOBS) == 2
    assert list(dummy_impl.JOBS.values())[0].state() == "failed"
    assert list(dummy_impl.JOBS.values())[1].state() == "failed"
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0


def test_failed_loop_followup_jobs() -> None:
    # ask to delete completed
    load = setup_loader(
        delete_completed_jobs=True,
        client_config=DummyClientConfiguration(fail_prob=1.0, create_followup_jobs=True),
    )
    # actually not deleted because one of the jobs failed
    with pytest.raises(LoadClientJobFailed) as e:
        assert_complete_job(load, should_delete_completed=False)

    assert "a random fail occurred" in e.value.failed_message
    # followup jobs were not started
    assert len(dummy_impl.JOBS) == 2
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0


def test_completed_loop_with_delete_completed() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    load.load_storage = load.create_storage(is_storage_owner=False)
    load.load_storage.config.delete_completed_jobs = True
    assert_complete_job(load, should_delete_completed=True)


@pytest.mark.parametrize("to_truncate", [True, False])
def test_truncate_table_before_load_on_staging(to_truncate) -> None:
    load = setup_loader(
        client_config=DummyClientConfiguration(
            truncate_tables_on_staging_destination_before_load=to_truncate
        )
    )
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    destination_client = load.get_destination_client(schema)
    assert (
        destination_client.should_truncate_table_before_load_on_staging_destination(  # type: ignore
            schema.tables["_dlt_version"]["name"]
        )
        == to_truncate
    )


def test_retry_on_new_loop() -> None:
    # test job that retries sitting in new jobs
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    load_id, schema = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    with ThreadPoolExecutor() as pool:
        # 1st retry
        with pytest.raises(LoadClientJobRetry):
            load.run(pool)
        files = load.load_storage.normalized_packages.list_new_jobs(load_id)
        assert len(files) == 2
        # 2nd retry
        with pytest.raises(LoadClientJobRetry):
            load.run(pool)
        files = load.load_storage.normalized_packages.list_new_jobs(load_id)
        assert len(files) == 2

        # package will be completed
        load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
        load.run(pool)
        assert not load.load_storage.normalized_packages.storage.has_folder(
            load.load_storage.get_normalized_package_path(load_id)
        )
        sleep(1)
        # parse the completed job names
        completed_path = load.load_storage.loaded_packages.get_package_path(load_id)
        for fn in load.load_storage.loaded_packages.storage.list_folder_files(
            os.path.join(completed_path, PackageStorage.COMPLETED_JOBS_FOLDER)
        ):
            # we update a retry count in each case (5 times for each loop run)
            assert ParsedLoadJobFileName.parse(fn).retry_count == 10


def test_retry_exceptions() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(retry_prob=1.0))
    prepare_load_package(load.load_storage, NORMALIZED_FILES)

    with ThreadPoolExecutor() as pool:
        # 1st retry
        with pytest.raises(LoadClientJobRetry) as py_ex:
            while True:
                load.run(pool)
        # configured to retry 5 times before exception
        assert py_ex.value.max_retry_count == py_ex.value.retry_count == 5
        # we can do it again
        with pytest.raises(LoadClientJobRetry) as py_ex:
            while True:
                load.run(pool)
        # this continues retry
        assert py_ex.value.max_retry_count * 2 == py_ex.value.retry_count == 10


def test_load_single_thread() -> None:
    os.environ["LOAD__WORKERS"] = "1"
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    assert load.config.pool_type == "none"
    load_id, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    # we do not need pool to complete
    metrics = load.run(None)
    while metrics.pending_items > 0:
        metrics = load.run(None)
    assert not load.load_storage.storage.has_folder(
        load.load_storage.get_normalized_package_path(load_id)
    )


def test_wrong_writer_type() -> None:
    load = setup_loader()
    load_id, _ = prepare_load_package(
        load.load_storage,
        [
            "event_bot.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values",
            "event_user.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values",
        ],
    )
    with ThreadPoolExecutor() as pool:
        with pytest.raises(JobFileFormatUnsupported) as exv:
            load.run(pool)
    assert exv.value.load_id == load_id


def test_extend_table_chain() -> None:
    load = setup_loader()
    _, schema = prepare_load_package(
        load.load_storage, ["event_user.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values"]
    )
    # only event user table (no other jobs)
    tables = _extend_tables_with_table_chain(schema, ["event_user"], ["event_user"])
    assert tables == {"event_user"}
    # add child jobs
    tables = _extend_tables_with_table_chain(
        schema, ["event_user"], ["event_user", "event_user__parse_data__entities"]
    )
    assert tables == {"event_user", "event_user__parse_data__entities"}
    user_chain = {name for name in schema.data_table_names() if name.startswith("event_user__")} | {
        "event_user"
    }
    # change event user to merge/replace to get full table chain
    for w_d in ["merge", "replace"]:
        schema.tables["event_user"]["write_disposition"] = w_d  # type:ignore[typeddict-item]
        tables = _extend_tables_with_table_chain(schema, ["event_user"], ["event_user"])
        assert tables == user_chain
    # no jobs for bot
    assert _extend_tables_with_table_chain(schema, ["event_bot"], ["event_user"]) == set()
    # skip unseen tables
    del schema.tables["event_user__parse_data__entities"]["x-normalizer"]
    entities_chain = {
        name
        for name in schema.data_table_names()
        if name.startswith("event_user__parse_data__entities")
    }
    tables = _extend_tables_with_table_chain(schema, ["event_user"], ["event_user"])
    assert tables == user_chain - {"event_user__parse_data__entities"}
    # exclude the whole chain
    tables = _extend_tables_with_table_chain(
        schema, ["event_user"], ["event_user"], lambda table_name: table_name not in entities_chain
    )
    assert tables == user_chain - entities_chain
    # ask for tables that are not top
    tables = _extend_tables_with_table_chain(schema, ["event_user__parse_data__entities"], [])
    # user chain but without entities (not seen data)
    assert tables == user_chain - {"event_user__parse_data__entities"}
    # go to append and ask only for entities chain
    schema.tables["event_user"]["write_disposition"] = "append"
    tables = _extend_tables_with_table_chain(
        schema, ["event_user__parse_data__entities"], entities_chain
    )
    # without entities (not seen data)
    assert tables == entities_chain - {"event_user__parse_data__entities"}

    # add multiple chains
    bot_jobs = {"event_bot", "event_bot__data__buttons"}
    tables = _extend_tables_with_table_chain(
        schema, ["event_user__parse_data__entities", "event_bot"], entities_chain | bot_jobs
    )
    assert tables == (entities_chain | bot_jobs) - {"event_user__parse_data__entities"}


def test_get_completed_table_chain_cases() -> None:
    load = setup_loader()
    _, schema = prepare_load_package(
        load.load_storage, ["event_user.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values"]
    )

    # update tables so we have all possible hints
    for table_name, table in schema.tables.items():
        schema.tables[table_name] = fill_hints_from_parent_and_clone_table(schema.tables, table)

    # child completed, parent not
    event_user = schema.get_table("event_user")
    event_user_entities = schema.get_table("event_user__parse_data__entities")
    event_user_job: Tuple[TPackageJobState, ParsedLoadJobFileName] = (
        "started_jobs",
        ParsedLoadJobFileName("event_user", "event_user_id", 0, "jsonl"),
    )
    event_user_entities_job: Tuple[TPackageJobState, ParsedLoadJobFileName] = (
        "completed_jobs",
        ParsedLoadJobFileName(
            "event_user__parse_data__entities", "event_user__parse_data__entities_id", 0, "jsonl"
        ),
    )
    chain = get_completed_table_chain(schema, [event_user_job, event_user_entities_job], event_user)
    assert chain is None

    # parent just got completed
    chain = get_completed_table_chain(
        schema,
        [event_user_job, event_user_entities_job],
        event_user,
        event_user_job[1].job_id(),
    )
    # full chain
    assert chain == [event_user, event_user_entities]

    # parent failed, child completed
    chain = get_completed_table_chain(
        schema, [("failed_jobs", event_user_job[1]), event_user_entities_job], event_user
    )
    assert chain == [event_user, event_user_entities]

    # both failed
    chain = get_completed_table_chain(
        schema,
        [("failed_jobs", event_user_job[1]), ("failed_jobs", event_user_entities_job[1])],
        event_user,
    )
    assert chain == [event_user, event_user_entities]

    # merge and replace do not require whole chain to be in jobs
    user_chain = get_nested_tables(schema.tables, "event_user")
    for w_d in ["merge", "replace"]:
        event_user["write_disposition"] = w_d  # type:ignore[typeddict-item]

        chain = get_completed_table_chain(
            schema, [event_user_job], event_user, event_user_job[1].job_id()
        )
        assert chain == user_chain

        # but if child is present and incomplete...
        chain = get_completed_table_chain(
            schema,
            [event_user_job, ("new_jobs", event_user_entities_job[1])],
            event_user,
            event_user_job[1].job_id(),
        )
        # noting is returned
        assert chain is None

    # skip unseen
    deep_child = schema.tables[
        "event_user__parse_data__response_selector__default__response__response_templates"
    ]
    del deep_child["x-normalizer"]
    chain = get_completed_table_chain(
        schema, [event_user_job], event_user, event_user_job[1].job_id()
    )
    user_chain.remove(deep_child)
    assert chain == user_chain


def test_init_client_truncate_tables() -> None:
    load = setup_loader()
    _, schema = prepare_load_package(
        load.load_storage, ["event_user.b1d32c6660b242aaabbf3fc27245b7e6.0.insert_values"]
    )

    nothing_ = lambda _: False
    all_ = lambda _: True

    event_user = ParsedLoadJobFileName("event_user", "event_user_id", 0, "jsonl")
    event_bot = ParsedLoadJobFileName("event_bot", "event_bot_id", 0, "jsonl")

    with patch.object(dummy_impl.DummyClient, "initialize_storage") as initialize_storage:
        with patch.object(dummy_impl.DummyClient, "update_stored_schema") as update_stored_schema:
            with load.get_destination_client(schema) as client:
                init_client(client, schema, [], {}, nothing_, nothing_)
            # we do not allow for any staging dataset tables
            assert update_stored_schema.call_count == 1
            assert update_stored_schema.call_args[1]["only_tables"] == {
                "_dlt_loads",
                "_dlt_version",
            }
            assert initialize_storage.call_count == 2
            # initialize storage is called twice, we deselected all tables to truncate
            assert initialize_storage.call_args_list[0].args == ()
            assert initialize_storage.call_args_list[1].kwargs["truncate_tables"] == set()

            initialize_storage.reset_mock()
            update_stored_schema.reset_mock()

            # now we want all tables to be truncated but not on staging
            with load.get_destination_client(schema) as client:
                init_client(client, schema, [event_user], {}, all_, nothing_)
            assert update_stored_schema.call_count == 1
            assert "event_user" in update_stored_schema.call_args[1]["only_tables"]
            assert initialize_storage.call_count == 2
            assert initialize_storage.call_args_list[0].args == ()
            assert initialize_storage.call_args_list[1].kwargs["truncate_tables"] == {"event_user"}

            # now we push all to stage
            initialize_storage.reset_mock()
            update_stored_schema.reset_mock()

            with load.get_destination_client(schema) as client:
                init_client(client, schema, [event_user, event_bot], {}, nothing_, all_)
            assert update_stored_schema.call_count == 2
            # first call main dataset
            assert {"event_user", "event_bot"} <= set(
                update_stored_schema.call_args_list[0].kwargs["only_tables"]
            )
            # second one staging dataset
            assert {"event_user", "event_bot"} <= set(
                update_stored_schema.call_args_list[1].kwargs["only_tables"]
            )
            assert initialize_storage.call_count == 4
            assert initialize_storage.call_args_list[0].args == ()
            assert initialize_storage.call_args_list[1].kwargs["truncate_tables"] == set()
            assert initialize_storage.call_args_list[2].args == ()
            # all tables that will be used on staging must be truncated
            assert initialize_storage.call_args_list[3].kwargs["truncate_tables"] == {
                "event_user",
                "event_bot",
            }

            replace_ = (
                lambda table_name: client.prepare_load_table(table_name)["write_disposition"]
                == "replace"
            )
            merge_ = (
                lambda table_name: client.prepare_load_table(table_name)["write_disposition"]
                == "merge"
            )

            # set event_bot chain to merge
            bot_chain = get_nested_tables(schema.tables, "event_bot")
            for w_d in ["merge", "replace"]:
                initialize_storage.reset_mock()
                update_stored_schema.reset_mock()
                for bot in bot_chain:
                    bot["write_disposition"] = w_d  # type:ignore[typeddict-item]
                # merge goes to staging, replace goes to truncate
                with load.get_destination_client(schema) as client:
                    init_client(client, schema, [event_user, event_bot], {}, replace_, merge_)

                if w_d == "merge":
                    # we use staging dataset
                    assert update_stored_schema.call_count == 2
                    # 4 tables to update in main dataset
                    assert len(update_stored_schema.call_args_list[0].kwargs["only_tables"]) == 4
                    assert (
                        "event_user" in update_stored_schema.call_args_list[0].kwargs["only_tables"]
                    )
                    # full bot table chain + dlt version but no user
                    assert len(
                        update_stored_schema.call_args_list[1].kwargs["only_tables"]
                    ) == 1 + len(bot_chain)
                    assert (
                        "event_user"
                        not in update_stored_schema.call_args_list[1].kwargs["only_tables"]
                    )

                    assert initialize_storage.call_count == 4
                    assert initialize_storage.call_args_list[1].kwargs["truncate_tables"] == set()
                    assert initialize_storage.call_args_list[3].kwargs[
                        "truncate_tables"
                    ] == update_stored_schema.call_args_list[1].kwargs["only_tables"] - {
                        "_dlt_version"
                    }

                if w_d == "replace":
                    assert update_stored_schema.call_count == 1
                    assert initialize_storage.call_count == 2
                    # we truncate the whole bot chain but not user (which is append)
                    assert len(
                        initialize_storage.call_args_list[1].kwargs["truncate_tables"]
                    ) == len(bot_chain)
                    # migrate only tables for which we have jobs
                    assert len(update_stored_schema.call_args_list[0].kwargs["only_tables"]) == 4
                    # print(initialize_storage.call_args_list)
                    # print(update_stored_schema.call_args_list)


def test_dummy_staging_filesystem() -> None:
    load = setup_loader(
        client_config=DummyClientConfiguration(completed_prob=1.0), filesystem_staging=True
    )
    assert_complete_job(load)
    # two reference jobs
    assert len(dummy_impl.JOBS) == 2
    assert len(dummy_impl.CREATED_FOLLOWUP_JOBS) == 0


def test_load_multiple_packages() -> None:
    load = setup_loader(client_config=DummyClientConfiguration(completed_prob=1.0))
    load.config.pool_type = "none"
    load_id_1, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    sleep(0.1)
    load_id_2, _ = prepare_load_package(load.load_storage, NORMALIZED_FILES)
    run_metrics = load.run(None)
    assert run_metrics.pending_items == 1
    # assert load._current_load_id is None
    metrics_id_1 = load._job_metrics
    assert len(metrics_id_1) == 2
    assert load._step_info_metrics(load_id_1)[0]["job_metrics"] == metrics_id_1
    run_metrics = load.run(None)
    assert run_metrics.pending_items == 0
    metrics_id_2 = load._job_metrics
    assert len(metrics_id_2) == 2
    assert load._step_info_metrics(load_id_2)[0]["job_metrics"] == metrics_id_2
    load_info = load.get_step_info(MockPipeline("pipe", True))  # type: ignore[abstract]
    assert load_id_1 in load_info.metrics
    assert load_id_2 in load_info.metrics
    assert load_info.metrics[load_id_1][0]["job_metrics"] == metrics_id_1
    assert load_info.metrics[load_id_2][0]["job_metrics"] == metrics_id_2
    # execute empty run
    load.run(None)
    assert len(load_info.metrics) == 2


def test_terminal_exceptions() -> None:
    try:
        raise TerminalValueError("a")
    except TerminalException:
        assert True
    else:
        raise AssertionError()


def assert_job_metrics(job: RunnableLoadJob, expected_state: str) -> None:
    metrics = job.metrics()
    assert metrics.state == expected_state
    assert metrics.started_at <= metrics.finished_at
    assert metrics.job_id == job.job_id()
    assert metrics.table_name == job._parsed_file_name.table_name
    assert metrics.file_path == job._file_path


def assert_complete_job(
    load: Load, should_delete_completed: bool = False, load_id: str = None, jobs_per_case: int = 1
) -> None:
    if not load_id:
        load_id, _ = prepare_load_package(
            load.load_storage, NORMALIZED_FILES, jobs_per_case=jobs_per_case
        )
    # will complete all jobs
    timestamp = "2024-04-05T09:16:59.942779Z"
    mocked_timestamp = {"state": {"created_at": timestamp}}
    with mock.patch(
        "dlt.current.load_package",
        return_value=mocked_timestamp,
    ), patch.object(
        dummy_impl.DummyClient,
        "complete_load",
    ) as complete_load:
        with ThreadPoolExecutor() as pool:
            load.run(pool)

            # moved to loaded
            assert not load.load_storage.storage.has_folder(
                load.load_storage.get_normalized_package_path(load_id)
            )
            completed_path = load.load_storage.loaded_packages.get_job_state_folder_path(
                load_id, "completed_jobs"
            )

            # should have migrated the schema
            assert load.load_storage.storage.has_file(
                os.path.join(
                    load.load_storage.get_loaded_package_path(load_id),
                    PackageStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME,
                )
            )

            if should_delete_completed:
                # package was deleted
                assert not load.load_storage.loaded_packages.storage.has_folder(completed_path)
            else:
                # package not deleted
                assert load.load_storage.loaded_packages.storage.has_folder(completed_path)
            # complete load on client was called
            complete_load.assert_called_once_with(load_id)
            # assert if all jobs in final state have metrics
            metrics = load.get_step_info(MockPipeline("pipe", True)).metrics[load_id][0]  # type: ignore[abstract]
            package_info = load.load_storage.loaded_packages.get_load_package_jobs(load_id)
            for state, jobs in package_info.items():
                for job in jobs:
                    job_metrics = metrics["job_metrics"].get(job.job_id())
                    if state in ("failed_jobs", "completed_jobs"):
                        assert job_metrics is not None
                        assert (
                            metrics["job_metrics"][job.job_id()].state == "failed"
                            if state == "failed_jobs"
                            else "completed"
                        )
                        remote_url = job_metrics.remote_url
                        if load.initial_client_config.create_followup_jobs:  # type: ignore
                            assert remote_url.endswith(job.file_name())
                        elif load.is_staging_destination_job(job.file_name()):
                            # staging destination should contain reference to remote filesystem
                            assert (
                                FilesystemConfiguration.make_file_url(REMOTE_FILESYSTEM)
                                in remote_url
                            )
                        else:
                            assert remote_url is None
                    else:
                        assert job_metrics is None


def run_all(load: Load) -> None:
    pool = ThreadPoolExecutor()
    while True:
        metrics = load.run(pool)
        if metrics.pending_items == 0:
            return
        sleep(0.1)


def setup_loader(
    delete_completed_jobs: bool = False,
    client_config: DummyClientConfiguration = None,
    loader_config: LoaderConfiguration = None,
    filesystem_staging: bool = False,
) -> Load:
    # reset jobs for a test
    dummy_impl.JOBS = {}
    dummy_impl.CREATED_FOLLOWUP_JOBS = {}
    dummy_impl.RETRIED_JOBS = {}
    dummy_impl.CREATED_TABLE_CHAIN_FOLLOWUP_JOBS = {}

    client_config = client_config or DummyClientConfiguration(
        loader_file_format="jsonl", completed_prob=1
    )
    destination: TDestination = dummy(**client_config)  # type: ignore[assignment]
    # setup
    staging_system_config = None
    staging = None
    if filesystem_staging:
        # do not accept jsonl to not conflict with filesystem destination
        # client_config = client_config or DummyClientConfiguration(
        #     loader_file_format="reference", completed_prob=1
        # )
        staging_system_config = FilesystemDestinationClientConfiguration()._bind_dataset_name(
            dataset_name="dummy"
        )
        staging_system_config.as_staging_destination = True
        os.makedirs(REMOTE_FILESYSTEM)
        staging = filesystem(bucket_url=REMOTE_FILESYSTEM)
    # patch destination to provide client_config
    # destination.client = lambda schema: dummy_impl.DummyClient(schema, client_config)
    # setup loader
    with TEST_DICT_CONFIG_PROVIDER().values({"delete_completed_jobs": delete_completed_jobs}):
        return Load(
            destination,
            initial_client_config=client_config,
            config=loader_config,
            staging_destination=staging,  # type: ignore[arg-type]
            initial_staging_client_config=staging_system_config,
        )
